"""Unit tests for Delta Lake current-state target table store, schemas, and bootstrap."""

import tempfile

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DecimalType, TimestampType

from src.merge.models import TARGET_METADATA_FIELDS, TargetAlreadyInitializedError
from src.merge.target_store import DeltaTargetStore
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
