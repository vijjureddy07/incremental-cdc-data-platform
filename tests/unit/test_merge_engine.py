"""Unit tests for DeltaMergeEngine mutation policies, updates, and deletes."""

import tempfile

import pytest
from pyspark.sql import SparkSession

from src.merge.merge_engine import DeltaMergeEngine
from src.merge.models import DeletePolicy, MergeAmbiguityError
from src.merge.target_store import DeltaTargetStore
from src.normalization.models import NormalizedCDCEvent
from src.source.generator import SnapshotConfig, SourceGenerator


def _make_account_event(
    event_id: str,
    operation: str,
    account_id: str,
    status: str = "ACTIVE",
    country: str = "US",
    sequence_number: int = 1,
) -> NormalizedCDCEvent:
    payload = (
        {
            "account_id": account_id,
            "account_name": f"Account {account_id}",
            "industry": "Software",
            "country": country,
            "status": status,
            "created_at": "2026-05-11T01:00:00Z",
            "updated_at": "2026-05-11T01:00:00Z",
        }
        if operation != "DELETE"
        else None
    )
    before_payload = (
        {
            "account_id": account_id,
            "account_name": f"Account {account_id}",
            "industry": "Software",
            "country": country,
            "status": "SUSPENDED",
            "created_at": "2026-05-11T01:00:00Z",
            "updated_at": "2026-05-11T01:00:00Z",
        }
        if operation in ("UPDATE", "DELETE")
        else None
    )

    return NormalizedCDCEvent(
        event_id=event_id,
        table_name="accounts",
        operation=operation,
        business_key={"account_id": account_id},
        sequence_number=sequence_number,
        event_timestamp="2026-05-11T01:00:00Z",
        source_commit_timestamp="2026-05-11T01:00:01Z",
        batch_id="batch_001",
        payload=payload,
        before_payload=before_payload,
        source_system="b2b_saas_postgres",
        entity_sequence_key=f'accounts:{{"account_id":"{account_id}"}}',
        business_key_canonical=f'{{"account_id":"{account_id}"}}',
        event_fingerprint=f"fp_{event_id}",
        is_late_arrival=False,
        source_file="batch_id=batch_001/accounts.jsonl",
        ingestion_batch_id="batch_001",
    )


def test_merge_engine_insert_and_update(spark_session: SparkSession):
    """Verify Delta MERGE inserts new rows and updates existing rows."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DeltaTargetStore(spark=spark_session, target_base_dir=tmpdir)
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        store.initialize_targets(source_gen.generate_snapshot_dicts())

        engine = DeltaMergeEngine(spark=spark_session, target_store=store)

        # 1. Insert ACC-0041 (not in snapshot)
        ins_ev = _make_account_event("evt_ins_01", "INSERT", "ACC-0041", sequence_number=1)
        engine.merge_wave("accounts", [ins_ev])

        df = store.read_current_table("accounts")
        assert df.count() == 41
        acc_41 = df.filter(df.account_id == "ACC-0041").first()
        assert acc_41["_last_sequence_number"] == 1
        assert acc_41["_last_operation"] == "INSERT"

        # 2. Update ACC-0001 (already in snapshot)
        upd_ev = _make_account_event("evt_upd_01", "UPDATE", "ACC-0001", status="SUSPENDED", sequence_number=2)
        engine.merge_wave("accounts", [upd_ev])

        df_after = store.read_current_table("accounts")
        assert df_after.count() == 41
        acc_01 = df_after.filter(df_after.account_id == "ACC-0001").first()
        assert acc_01["status"] == "SUSPENDED"
        assert acc_01["_last_sequence_number"] == 2
        assert acc_01["_last_operation"] == "UPDATE"


def test_merge_engine_hard_delete(spark_session: SparkSession):
    """Verify HARD delete physically deletes the row from target Delta table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DeltaTargetStore(spark=spark_session, target_base_dir=tmpdir)
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        store.initialize_targets(source_gen.generate_snapshot_dicts())

        engine = DeltaMergeEngine(spark=spark_session, target_store=store)

        del_ev = _make_account_event("evt_del_01", "DELETE", "ACC-0001", sequence_number=10)
        engine.merge_wave("accounts", [del_ev], delete_policy=DeletePolicy.HARD)

        df = store.read_current_table("accounts")
        assert df.count() == 39
        assert df.filter(df.account_id == "ACC-0001").count() == 0


def test_merge_engine_soft_delete(spark_session: SparkSession):
    """Verify SOFT delete sets _is_deleted=True and tombstone without physically removing the row."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DeltaTargetStore(spark=spark_session, target_base_dir=tmpdir)
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        store.initialize_targets(source_gen.generate_snapshot_dicts())

        engine = DeltaMergeEngine(spark=spark_session, target_store=store)

        del_ev = _make_account_event("evt_del_01", "DELETE", "ACC-0001", sequence_number=10)
        engine.merge_wave("accounts", [del_ev], delete_policy=DeletePolicy.SOFT)

        # Active current table filters out deleted rows -> 39
        active_df = store.read_current_table("accounts", include_deleted=False)
        assert active_df.count() == 39

        # Full table including deleted retains 40 rows
        all_df = store.read_current_table("accounts", include_deleted=True)
        assert all_df.count() == 40
        del_row = all_df.filter(all_df.account_id == "ACC-0001").first()
        assert del_row["_is_deleted"] is True
        assert del_row["_deleted_at"] == "2026-05-11T01:00:01Z"
        assert del_row["_last_operation"] == "DELETE"


def test_merge_engine_soft_delete_cleared_by_subsequent_update(spark_session: SparkSession):
    """Verify a subsequent UPDATE restores _is_deleted=False and clears _deleted_at on a soft-deleted row."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DeltaTargetStore(spark=spark_session, target_base_dir=tmpdir)
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        store.initialize_targets(source_gen.generate_snapshot_dicts())

        engine = DeltaMergeEngine(spark=spark_session, target_store=store)

        # 1. Soft delete
        del_ev = _make_account_event("evt_del_01", "DELETE", "ACC-0001", sequence_number=10)
        engine.merge_wave("accounts", [del_ev], delete_policy=DeletePolicy.SOFT)

        # 2. Subsequent Update
        upd_ev = _make_account_event("evt_upd_01", "UPDATE", "ACC-0001", status="ACTIVE", sequence_number=20)
        engine.merge_wave("accounts", [upd_ev], delete_policy=DeletePolicy.SOFT)

        active_df = store.read_current_table("accounts", include_deleted=False)
        assert active_df.count() == 40
        acc_01 = active_df.filter(active_df.account_id == "ACC-0001").first()
        assert acc_01["_is_deleted"] is False
        assert acc_01["_deleted_at"] is None
        assert acc_01["_last_sequence_number"] == 20


def test_merge_engine_ambiguity_protection_multiple_events_same_pk(spark_session: SparkSession):
    """Verify merge_wave raises MergeAmbiguityError if multiple events in the same wave target the same PK."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DeltaTargetStore(spark=spark_session, target_base_dir=tmpdir)
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        store.initialize_targets(source_gen.generate_snapshot_dicts())

        engine = DeltaMergeEngine(spark=spark_session, target_store=store)

        ev1 = _make_account_event("evt_01", "UPDATE", "ACC-0001", status="ACTIVE", sequence_number=10)
        ev2 = _make_account_event("evt_02", "UPDATE", "ACC-0001", status="SUSPENDED", sequence_number=10)

        with pytest.raises(MergeAmbiguityError):
            engine.merge_wave("accounts", [ev1, ev2])
