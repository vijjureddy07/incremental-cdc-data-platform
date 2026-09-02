"""PySpark-based CDC normalization, deduplication, conflict resolution, and sequence ordering engine."""

import json
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from src.normalization.fingerprint import (
    canonicalize_business_key,
    compute_entity_sequence_key,
    compute_event_fingerprint,
)
from src.normalization.models import (
    NormalizedCDCEvent,
    QuarantinedEvent,
    QuarantineReasonCode,
)
from src.utils.helpers import parse_iso_timestamp

ENRICHED_CDC_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("business_key", MapType(StringType(), StringType()), True),
        StructField("business_key_canonical", StringType(), True),
        StructField("entity_sequence_key", StringType(), True),
        StructField("sequence_number", LongType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("source_commit_timestamp", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("payload_json", StringType(), True),
        StructField("before_payload_json", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingestion_batch_id", StringType(), True),
        StructField("ingestion_order", LongType(), True),
        StructField("event_fingerprint", StringType(), True),
        StructField("is_late_arrival", BooleanType(), True),
    ]
)


class SparkCDCNormalizationProcessor:
    """Processes validated raw CDC records into canonical normalized streams using PySpark transformations."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def process(
        self,
        valid_records: list[dict[str, Any]],
    ) -> tuple[list[NormalizedCDCEvent], list[QuarantinedEvent], int, int, int]:
        """Normalize, deduplicate, resolve conflicts, and order CDC events.

        Returns:
            tuple of (accepted_events, quarantined_events, exact_duplicates_dropped, duplicate_event_conflicts, sequence_conflicts)
        """
        if not valid_records:
            return [], [], 0, 0, 0

        # 1. Compute Ingestion-Context Late Arrival Boundaries
        # Deterministically order all observed ingestion batches
        sorted_batch_ids = sorted(
            {str(r.get("ingestion_batch_id") or r.get("batch_id") or "") for r in valid_records}
        )

        prior_max_ts_by_batch: dict[str, datetime | None] = {}
        running_max_ts: datetime | None = None

        for b_id in sorted_batch_ids:
            prior_max_ts_by_batch[b_id] = running_max_ts
            batch_records = [
                r
                for r in valid_records
                if str(r.get("ingestion_batch_id") or r.get("batch_id") or "") == b_id
            ]
            for br in batch_records:
                try:
                    ts_dt = parse_iso_timestamp(str(br.get("event_timestamp")))
                    if running_max_ts is None or ts_dt > running_max_ts:
                        running_max_ts = ts_dt
                except Exception:
                    pass

        # 2. Enrich records with canonical keys, deterministic fingerprints, and late arrival flags
        enriched_rows: list[dict[str, Any]] = []
        for r in valid_records:
            b_key_dict = {str(k): str(v) for k, v in r.get("business_key", {}).items()}
            b_key_canonical = canonicalize_business_key(b_key_dict)
            entity_key = compute_entity_sequence_key(r["table_name"], b_key_canonical)
            fingerprint = compute_event_fingerprint(r)

            # Ingestion-derived late arrival determination
            rec_batch = str(r.get("ingestion_batch_id") or r.get("batch_id") or "")
            prior_max = prior_max_ts_by_batch.get(rec_batch)
            try:
                rec_ts = parse_iso_timestamp(str(r.get("event_timestamp")))
                is_late = bool(prior_max is not None and rec_ts < prior_max)
            except Exception:
                is_late = False

            row_copy = {
                "event_id": str(r.get("event_id", "")),
                "table_name": str(r.get("table_name", "")),
                "operation": str(r.get("operation", "")),
                "business_key": b_key_dict,
                "business_key_canonical": b_key_canonical,
                "entity_sequence_key": entity_key,
                "sequence_number": int(r["sequence_number"]),
                "event_timestamp": str(r.get("event_timestamp", "")),
                "source_commit_timestamp": str(r.get("source_commit_timestamp", "")),
                "batch_id": str(r.get("batch_id", "")),
                "payload_json": (
                    json.dumps(r["payload"], sort_keys=True) if r.get("payload") is not None else None
                ),
                "before_payload_json": (
                    json.dumps(r["before_payload"], sort_keys=True)
                    if r.get("before_payload") is not None
                    else None
                ),
                "source_system": str(r.get("source_system", "")),
                "source_file": str(r.get("source_file", "")),
                "ingestion_batch_id": str(r.get("ingestion_batch_id", "")),
                "ingestion_order": int(r.get("ingestion_order", 0)),
                "event_fingerprint": fingerprint,
                "is_late_arrival": is_late,
            }
            enriched_rows.append(row_copy)

        # 3. Ingest into Spark DataFrame using explicit StructType schema
        df: DataFrame = self.spark.createDataFrame(enriched_rows, schema=ENRICHED_CDC_SCHEMA)

        # 4. Duplicate Event ID Resolution (Exact Replay vs Conflicting Event ID)
        event_id_window = Window.partitionBy("event_id")
        # Deterministic provenance ordering for exact duplicate winner selection
        event_id_order_window = Window.partitionBy("event_id").orderBy(
            F.col("batch_id").asc(),
            F.col("ingestion_batch_id").asc(),
            F.col("source_file").asc(),
            F.col("ingestion_order").asc(),
        )

        df_dup = df.withColumn(
            "distinct_fingerprints",
            F.size(F.collect_set("event_fingerprint").over(event_id_window)),
        ).withColumn(
            "total_event_id_count",
            F.count(F.lit(1)).over(event_id_window),
        ).withColumn(
            "event_id_rn",
            F.row_number().over(event_id_order_window),
        )

        # Conflicting duplicate event_id (same event_id, different fingerprint) -> Quarantine all
        conflicting_events_df = df_dup.filter(F.col("distinct_fingerprints") > 1)
        conflicting_rows = conflicting_events_df.collect()

        quarantined_conflicts: list[QuarantinedEvent] = []
        for row in conflicting_rows:
            raw_rec = {
                "event_id": row["event_id"],
                "table_name": row["table_name"],
                "operation": row["operation"],
                "business_key": row["business_key"],
                "sequence_number": row["sequence_number"],
                "event_timestamp": row["event_timestamp"],
                "source_commit_timestamp": row["source_commit_timestamp"],
                "batch_id": row["batch_id"],
                "payload": json.loads(row["payload_json"]) if row["payload_json"] else None,
                "before_payload": (
                    json.loads(row["before_payload_json"]) if row["before_payload_json"] else None
                ),
                "source_system": row["source_system"],
            }
            quarantined_conflicts.append(
                QuarantinedEvent(
                    quarantine_code=QuarantineReasonCode.DUPLICATE_EVENT_CONFLICT,
                    quarantine_reason=(
                        f"Event ID '{row['event_id']}' has conflicting semantic payloads "
                        f"(multiple distinct fingerprints found)."
                    ),
                    raw_record=raw_rec,
                    event_id=row["event_id"],
                    table_name=row["table_name"],
                    source_file=row["source_file"],
                    batch_id=row["batch_id"],
                )
            )

        # Exact duplicate deliveries (same event_id, same fingerprint) -> drop redundant copies (rn > 1)
        exact_duplicates_df = df_dup.filter(
            (F.col("distinct_fingerprints") == 1) & (F.col("total_event_id_count") > 1) & (F.col("event_id_rn") > 1)
        )
        exact_duplicates_dropped = exact_duplicates_df.count()

        # Retained candidate rows (distinct_fingerprints == 1 and rn == 1)
        retained_candidates_df = df_dup.filter(
            (F.col("distinct_fingerprints") == 1) & (F.col("event_id_rn") == 1)
        )

        # 5. Equal-Sequence Conflict Detection across the same entity
        entity_seq_window = Window.partitionBy("entity_sequence_key", "sequence_number")

        df_seq = retained_candidates_df.withColumn(
            "seq_collision_count",
            F.count(F.lit(1)).over(entity_seq_window),
        )

        # Sequence conflicts: multiple distinct logical events sharing entity + sequence_number
        seq_conflict_df = df_seq.filter(F.col("seq_collision_count") > 1)
        seq_conflict_rows = seq_conflict_df.collect()

        quarantined_seq_conflicts: list[QuarantinedEvent] = []
        for row in seq_conflict_rows:
            raw_rec = {
                "event_id": row["event_id"],
                "table_name": row["table_name"],
                "operation": row["operation"],
                "business_key": row["business_key"],
                "sequence_number": row["sequence_number"],
                "event_timestamp": row["event_timestamp"],
                "source_commit_timestamp": row["source_commit_timestamp"],
                "batch_id": row["batch_id"],
                "payload": json.loads(row["payload_json"]) if row["payload_json"] else None,
                "before_payload": (
                    json.loads(row["before_payload_json"]) if row["before_payload_json"] else None
                ),
                "source_system": row["source_system"],
            }
            quarantined_seq_conflicts.append(
                QuarantinedEvent(
                    quarantine_code=QuarantineReasonCode.SEQUENCE_CONFLICT,
                    quarantine_reason=(
                        f"Entity '{row['entity_sequence_key']}' has multiple distinct events "
                        f"at sequence number {row['sequence_number']}."
                    ),
                    raw_record=raw_rec,
                    event_id=row["event_id"],
                    table_name=row["table_name"],
                    source_file=row["source_file"],
                    batch_id=row["batch_id"],
                )
            )

        # 6. Filter Accepted Clean Events
        accepted_df = df_seq.filter(F.col("seq_collision_count") == 1)

        # 7. Authoritative Deterministic Ordering: table_name, business_key_canonical, sequence_number, event_id
        ordered_accepted_df = accepted_df.orderBy(
            F.col("table_name").asc(),
            F.col("business_key_canonical").asc(),
            F.col("sequence_number").asc(),
            F.col("event_id").asc(),
        )

        accepted_rows = ordered_accepted_df.collect()
        accepted_events: list[NormalizedCDCEvent] = []

        for row in accepted_rows:
            accepted_events.append(
                NormalizedCDCEvent(
                    event_id=str(row["event_id"]),
                    table_name=str(row["table_name"]),
                    operation=str(row["operation"]),
                    business_key=dict(row["business_key"]),
                    business_key_canonical=str(row["business_key_canonical"]),
                    entity_sequence_key=str(row["entity_sequence_key"]),
                    sequence_number=int(row["sequence_number"]),
                    event_timestamp=str(row["event_timestamp"]),
                    source_commit_timestamp=str(row["source_commit_timestamp"]),
                    batch_id=str(row["batch_id"]),
                    source_system=str(row["source_system"]),
                    payload=json.loads(row["payload_json"]) if row["payload_json"] else None,
                    before_payload=(
                        json.loads(row["before_payload_json"]) if row["before_payload_json"] else None
                    ),
                    event_fingerprint=str(row["event_fingerprint"]),
                    ingestion_batch_id=str(row["ingestion_batch_id"]),
                    source_file=str(row["source_file"]),
                    is_late_arrival=bool(row["is_late_arrival"]),
                )
            )

        all_quarantined = quarantined_conflicts + quarantined_seq_conflicts

        return (
            accepted_events,
            all_quarantined,
            exact_duplicates_dropped,
            len(quarantined_conflicts),
            len(quarantined_seq_conflicts),
        )
