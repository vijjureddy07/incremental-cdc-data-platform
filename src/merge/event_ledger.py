"""Durable Delta Lake applied event ledger for replay, sequence checkpoints, and crash recovery."""

from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.merge.models import (
    EVENT_LEDGER_SCHEMA,
    EventLedgerRecord,
    LedgerStatus,
)
from src.normalization.models import NormalizedCDCEvent


class EventApplyLedger:
    """Manages the durable Delta Lake event application ledger."""

    def __init__(
        self,
        spark: SparkSession,
        ledger_base_dir: str | Path = "data/delta/control/event_apply_ledger",
    ) -> None:
        self.spark = spark
        self.ledger_dir = Path(ledger_base_dir)

    def ledger_exists(self) -> bool:
        """Check whether the ledger Delta table exists."""
        if not self.ledger_dir.exists():
            return False
        return DeltaTable.isDeltaTable(self.spark, str(self.ledger_dir))

    def initialize_ledger(self, overwrite: bool = False) -> None:
        """Initialize the ledger Delta table if not present."""
        if self.ledger_exists() and not overwrite:
            return

        self.ledger_dir.mkdir(parents=True, exist_ok=True)
        empty_df = self.spark.createDataFrame([], schema=EVENT_LEDGER_SCHEMA)
        (
            empty_df.write.format("delta")
            .mode("overwrite" if overwrite else "errorIfExists")
            .save(str(self.ledger_dir))
        )

    def read_ledger(self) -> DataFrame:
        """Read the ledger as a Spark DataFrame."""
        if not self.ledger_exists():
            self.initialize_ledger()
        return self.spark.read.format("delta").load(str(self.ledger_dir))

    def get_all_ledger_records(self) -> list[EventLedgerRecord]:
        """Fetch all ledger records as Python dataclass instances."""
        if not self.ledger_exists():
            return []

        df = self.read_ledger()
        rows = df.collect()
        return [
            EventLedgerRecord(
                event_id=str(r["event_id"]),
                event_fingerprint=str(r["event_fingerprint"]),
                entity_sequence_key=str(r["entity_sequence_key"]),
                table_name=str(r["table_name"]),
                business_key_canonical=str(r["business_key_canonical"]),
                sequence_number=int(r["sequence_number"]),
                operation=str(r["operation"]),
                source_commit_timestamp=str(r["source_commit_timestamp"]),
                processing_id=str(r["processing_id"]),
                source_file=str(r["source_file"]),
                status=str(r["status"]),
            )
            for r in rows
        ]

    def get_pending_records(self) -> list[EventLedgerRecord]:
        """Fetch all ledger records currently in PENDING state."""
        all_recs = self.get_all_ledger_records()
        return [r for r in all_recs if r.status == LedgerStatus.PENDING.value]

    def get_applied_max_sequences(self) -> dict[str, int]:
        """Compute the maximum applied sequence number per entity_sequence_key."""
        if not self.ledger_exists():
            return {}

        df = self.read_ledger().filter(F.col("status") == LedgerStatus.APPLIED.value)
        agg_df = df.groupBy("entity_sequence_key").agg(F.max("sequence_number").alias("max_seq"))
        return {str(row["entity_sequence_key"]): int(row["max_seq"]) for row in agg_df.collect()}

    def get_ledger_record_by_event_id(self, event_id: str) -> EventLedgerRecord | None:
        """Retrieve a ledger record by event_id if present."""
        if not self.ledger_exists():
            return None

        df = self.read_ledger().filter(F.col("event_id") == event_id)
        rows = df.collect()
        if not rows:
            return None

        r = rows[0]
        return EventLedgerRecord(
            event_id=str(r["event_id"]),
            event_fingerprint=str(r["event_fingerprint"]),
            entity_sequence_key=str(r["entity_sequence_key"]),
            table_name=str(r["table_name"]),
            business_key_canonical=str(r["business_key_canonical"]),
            sequence_number=int(r["sequence_number"]),
            operation=str(r["operation"]),
            source_commit_timestamp=str(r["source_commit_timestamp"]),
            processing_id=str(r["processing_id"]),
            source_file=str(r["source_file"]),
            status=str(r["status"]),
        )

    def record_pending_events(
        self,
        events: list[NormalizedCDCEvent],
        processing_id: str,
    ) -> None:
        """Record a batch of incoming events into the ledger with PENDING status.

        Invariant: PENDING intent is immutable. If an event_id already exists in the ledger,
        its existing row is left untouched (whenNotMatchedInsertAll only).
        """
        if not events:
            return

        if not processing_id or not isinstance(processing_id, str):
            raise ValueError(
                "A non-empty processing_id string is required to record pending events."
            )

        if not self.ledger_exists():
            self.initialize_ledger()

        rows: list[dict[str, Any]] = [
            {
                "event_id": ev.event_id,
                "event_fingerprint": ev.event_fingerprint,
                "entity_sequence_key": ev.entity_sequence_key,
                "table_name": ev.table_name,
                "business_key_canonical": ev.business_key_canonical,
                "sequence_number": ev.sequence_number,
                "operation": ev.operation,
                "source_commit_timestamp": ev.source_commit_timestamp,
                "processing_id": processing_id,
                "source_file": ev.source_file,
                "status": LedgerStatus.PENDING.value,
            }
            for ev in events
        ]

        source_df = self.spark.createDataFrame(rows, schema=EVENT_LEDGER_SCHEMA)
        delta_table = DeltaTable.forPath(self.spark, str(self.ledger_dir))

        (
            delta_table.alias("target")
            .merge(
                source_df.alias("source"),
                "target.event_id = source.event_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    def mark_events_applied(self, event_ids: list[str]) -> None:
        """Transition a list of event_ids in the ledger from PENDING to APPLIED."""
        if not event_ids:
            return

        if not self.ledger_exists():
            self.initialize_ledger()

        delta_table = DeltaTable.forPath(self.spark, str(self.ledger_dir))
        delta_table.update(
            condition=F.col("event_id").isin(event_ids)
            & (F.col("status") == F.lit(LedgerStatus.PENDING.value)),
            set={"status": F.lit(LedgerStatus.APPLIED.value)},
        )
