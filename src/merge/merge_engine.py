"""Delta Lake MERGE engine supporting INSERT, UPDATE, HARD DELETE, and SOFT DELETE policies."""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.merge.event_adapter import convert_events_to_spark_df
from src.merge.models import (
    DeletePolicy,
    MergeAmbiguityError,
)
from src.merge.target_store import DeltaTargetStore
from src.normalization.models import NormalizedCDCEvent
from src.source.schemas import TABLE_PRIMARY_KEYS, TABLE_SCHEMAS_MAP


class DeltaMergeEngine:
    """Executes deterministic Delta Lake MERGE operations on current-state tables."""

    def __init__(
        self,
        spark: SparkSession,
        target_store: DeltaTargetStore,
    ) -> None:
        self.spark = spark
        self.target_store = target_store

    def merge_wave(
        self,
        table_name: str,
        events: list[NormalizedCDCEvent],
        delete_policy: DeletePolicy = DeletePolicy.HARD,
        processing_id: str = "proc_delta",
    ) -> tuple[int, int, int]:
        """Execute a single wave of mutations on a target Delta table.

        Args:
            table_name: The target table name.
            events: List of NormalizedCDCEvent instances in this wave.
            delete_policy: HARD or SOFT delete policy.
            processing_id: Processing batch ID for target metadata provenance.

        Returns:
            Tuple of (inserts_applied, updates_applied, deletes_applied)
        """
        if not events:
            return 0, 0, 0

        pk = TABLE_PRIMARY_KEYS[table_name]
        table_path = self.target_store.get_table_path(table_name)
        if not self.target_store.table_exists(table_name):
            raise FileNotFoundError(f"Target Delta table '{table_name}' does not exist at {table_path}")

        # Ambiguity protection: verify no two events in this wave target the same primary key
        pks_seen: set[str] = set()
        for ev in events:
            val = str(ev.business_key.get(pk))
            if val in pks_seen:
                raise MergeAmbiguityError(
                    f"Ambiguous MERGE: Multiple events in the same wave target primary key {pk}={val} on {table_name}."
                )
            pks_seen.add(val)

        delta_table = DeltaTable.forPath(self.spark, str(table_path))
        base_schema = TABLE_SCHEMAS_MAP[table_name]
        business_cols = [f.name for f in base_schema.fields]

        upsert_events = [e for e in events if e.operation in ("INSERT", "UPDATE")]
        delete_events = [e for e in events if e.operation == "DELETE"]

        # 1. Apply Upserts (INSERT / UPDATE)
        if upsert_events:
            upsert_df = convert_events_to_spark_df(table_name, upsert_events, self.spark, processing_id=processing_id)

            # Build update set expressions
            update_set = {col: f"source.{col}" for col in business_cols}
            update_set.update(
                {
                    "_last_sequence_number": "source._last_sequence_number",
                    "_last_event_id": "source._last_event_id",
                    "_last_operation": "source._last_operation",
                    "_last_event_fingerprint": "source._last_event_fingerprint",
                    "_last_source_commit_timestamp": "source._last_source_commit_timestamp",
                    "_last_processing_id": "source._last_processing_id",
                    "_is_deleted": F.lit(False),
                    "_deleted_at": F.lit(None),
                }
            )

            # Build insert set expressions
            insert_set = {col: f"source.{col}" for col in business_cols}
            insert_set.update(
                {
                    "_last_sequence_number": "source._last_sequence_number",
                    "_last_event_id": "source._last_event_id",
                    "_last_operation": "source._last_operation",
                    "_last_event_fingerprint": "source._last_event_fingerprint",
                    "_last_source_commit_timestamp": "source._last_source_commit_timestamp",
                    "_last_processing_id": "source._last_processing_id",
                    "_is_deleted": F.lit(False),
                    "_deleted_at": F.lit(None),
                }
            )

            (
                delta_table.alias("target")
                .merge(
                    upsert_df.alias("source"),
                    f"target.{pk} = source.{pk}",
                )
                .whenMatchedUpdate(set=update_set)
                .whenNotMatchedInsert(values=insert_set)
                .execute()
            )

        # 2. Apply Deletes (DELETE)
        if delete_events:
            delete_df = convert_events_to_spark_df(table_name, delete_events, self.spark, processing_id=processing_id)

            if delete_policy == DeletePolicy.HARD:
                (
                    delta_table.alias("target")
                    .merge(
                        delete_df.alias("source"),
                        f"target.{pk} = source.{pk}",
                    )
                    .whenMatchedDelete()
                    .execute()
                )
            elif delete_policy == DeletePolicy.SOFT:
                # In soft delete mode, update metadata and tombstone without modifying business data
                soft_delete_set = {
                    "_is_deleted": F.lit(True),
                    "_deleted_at": "source._last_source_commit_timestamp",
                    "_last_sequence_number": "source._last_sequence_number",
                    "_last_event_id": "source._last_event_id",
                    "_last_operation": "source._last_operation",
                    "_last_event_fingerprint": "source._last_event_fingerprint",
                    "_last_source_commit_timestamp": "source._last_source_commit_timestamp",
                    "_last_processing_id": "source._last_processing_id",
                }

                (
                    delta_table.alias("target")
                    .merge(
                        delete_df.alias("source"),
                        f"target.{pk} = source.{pk}",
                    )
                    .whenMatchedUpdate(set=soft_delete_set)
                    .execute()
                )

        inserts_applied = len([e for e in events if e.operation == "INSERT"])
        updates_applied = len([e for e in events if e.operation == "UPDATE"])
        deletes_applied = len(delete_events)

        return inserts_applied, updates_applied, deletes_applied
