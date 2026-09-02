"""Unit tests for Delta Lake current-state target table store, schemas, and bootstrap."""

import tempfile

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DecimalType, TimestampType

from src.merge.merge_engine import DeltaMergeEngine
from src.merge.models import TARGET_METADATA_FIELDS, DeletePolicy, TargetAlreadyInitializedError
from src.merge.target_store import DeltaTargetStore
from src.normalization.models import NormalizedCDCEvent
from src.source.generator import SnapshotConfig, SourceGenerator


def test_target_store_initialization_counts(spark_session: SparkSession):
    """Verify Delta current-state tables initialize with exact Module 1 snapshot row counts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DeltaTargetStore(spark=spark_session, target_base_dir=tmpdir)
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        snapshot = source_gen.generate_snapshot_dicts()

        counts = store.initialize_targets(snapshot)

        assert counts["accounts"] == 40
        assert counts["subscriptions"] == 60
        assert counts["invoices"] == 120
        assert counts["payments"] == 90

        # Read back each table
        for tbl, expected_cnt in counts.items():
            assert store.table_exists(tbl)
            df = store.read_current_table(tbl)
            assert df.count() == expected_cnt


def test_target_store_initialization_idempotence(spark_session: SparkSession):
    """Verify repeated initialization without overwrite raises TargetAlreadyInitializedError; with overwrite succeeds."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DeltaTargetStore(spark=spark_session, target_base_dir=tmpdir)
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        snapshot = source_gen.generate_snapshot_dicts()

        # First run succeeds
        store.initialize_targets(snapshot)

        # Second run without overwrite raises error
        with pytest.raises(TargetAlreadyInitializedError):
            store.initialize_targets(snapshot, overwrite=False)

        # Run with overwrite=True succeeds and maintains count
        counts = store.initialize_targets(snapshot, overwrite=True)
        assert counts["accounts"] == 40


def test_target_store_typed_schema_and_metadata(spark_session: SparkSession):
    """Verify target tables preserve meaningful column data types and CDC operational metadata."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DeltaTargetStore(spark=spark_session, target_base_dir=tmpdir)
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        snapshot = source_gen.generate_snapshot_dicts()

        store.initialize_targets(snapshot)

        sub_df = store.read_current_table("subscriptions")
        schema_fields = {f.name: f.dataType for f in sub_df.schema.fields}

        # Check domain types
        assert isinstance(schema_fields["monthly_amount"], DecimalType)
        assert isinstance(schema_fields["start_date"], DateType)
        assert isinstance(schema_fields["created_at"], TimestampType)

        # Check metadata fields
        for meta_f in TARGET_METADATA_FIELDS:
            assert meta_f.name in schema_fields

        first_row = sub_df.first()
        assert first_row["_last_sequence_number"] == 0
        assert first_row["_last_event_id"] == "snapshot_init"
        assert first_row["_last_operation"] == "SNAPSHOT"
        assert first_row["_is_deleted"] is False
        assert first_row["_deleted_at"] is None


def test_target_store_time_travel_history(spark_session: SparkSession):
    """Verify Delta commit history and time-travel querying via versionAsOf."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DeltaTargetStore(spark=spark_session, target_base_dir=tmpdir)
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        snapshot = source_gen.generate_snapshot_dicts()

        store.initialize_targets(snapshot)

        # Initial version is 0
        v0_version = store.get_table_version("accounts")
        assert v0_version == 0
        v0_df = store.read_target_version("accounts", version=0)
        assert v0_df.count() == 40

        history = store.get_delta_history("accounts")
        assert len(history) >= 1
        assert history[0]["version"] == 0


def test_target_store_time_travel_after_mutation(spark_session: SparkSession):
    """Verify time travel querying after real mutation: versionAsOf=0 still preserves snapshot state."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DeltaTargetStore(spark=spark_session, target_base_dir=tmpdir)
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        snapshot = source_gen.generate_snapshot_dicts()
        store.initialize_targets(snapshot)

        # 1. Capture initial version 0 state for ACC-0001
        v0_version = store.get_table_version("accounts")
        assert v0_version == 0
        v0_acc1 = store.read_current_table("accounts").filter("account_id = 'ACC-0001'").first()
        initial_name = v0_acc1["account_name"]

        # 2. Apply a real UPDATE through DeltaMergeEngine
        merge_engine = DeltaMergeEngine(spark=spark_session, target_store=store)
        update_ev = NormalizedCDCEvent(
            event_id="evt_acc_time_travel_01",
            table_name="accounts",
            operation="UPDATE",
            business_key={"account_id": "ACC-0001"},
            sequence_number=10,
            event_timestamp="2026-05-11T02:00:00Z",
            source_commit_timestamp="2026-05-11T02:00:01Z",
            batch_id="batch_001",
            payload={
                "account_id": "ACC-0001",
                "account_name": "Acme Global Updated",
                "industry": v0_acc1["industry"],
                "country": v0_acc1["country"],
                "status": "ACTIVE",
                "created_at": v0_acc1["created_at"].isoformat()
                if hasattr(v0_acc1["created_at"], "isoformat")
                else str(v0_acc1["created_at"]),
                "updated_at": "2026-05-11T02:00:00Z",
            },
            before_payload=None,
            source_system="b2b_saas_postgres",
            entity_sequence_key='accounts:{"account_id":"ACC-0001"}',
            business_key_canonical='{"account_id":"ACC-0001"}',
            event_fingerprint="fp_acc_tt_01",
            is_late_arrival=False,
            source_file="batch_id=batch_001/accounts.jsonl",
            ingestion_batch_id="batch_001",
        )

        ins, upd, dels = merge_engine.merge_wave(
            table_name="accounts",
            events=[update_ev],
            delete_policy=DeletePolicy.HARD,
            processing_id="proc_time_travel_test",
        )
        assert upd == 1

        # 3. Verify current version is incremented and reflects updated state
        v1_version = store.get_table_version("accounts")
        assert v1_version > 0
        current_acc1 = (
            store.read_current_table("accounts").filter("account_id = 'ACC-0001'").first()
        )
        assert current_acc1["account_name"] == "Acme Global Updated"
        assert current_acc1["_last_sequence_number"] == 10
        assert current_acc1["_last_operation"] == "UPDATE"
        assert current_acc1["_last_processing_id"] == "proc_time_travel_test"

        # 4. Query historical version 0 and assert original snapshot state is preserved
        historical_v0_acc1 = (
            store.read_target_version("accounts", version=0)
            .filter("account_id = 'ACC-0001'")
            .first()
        )
        assert historical_v0_acc1["account_name"] == initial_name
        assert historical_v0_acc1["_last_sequence_number"] == 0
        assert historical_v0_acc1["_last_operation"] == "SNAPSHOT"
        assert historical_v0_acc1["_last_processing_id"] == "snapshot_bootstrap"
