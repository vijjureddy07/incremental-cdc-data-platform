"""End-to-end integration tests for Delta Change Data Feed, downstream archive, and replay idempotency."""

from pathlib import Path

from pyspark.sql import SparkSession

from src.cdf.archive import CDFArchiveStore
from src.cdf.pipeline import CDFDownstreamPipeline
from src.cdf.reader import CDFReader
from src.cdf.state_store import CDFStateStore
from src.merge.models import DeletePolicy
from src.merge.pipeline import DeltaMergePipeline
from src.normalization.models import NormalizedCDCEvent
from src.source.generator import SnapshotConfig, SourceGenerator


def _make_accounts_event(
    event_id: str,
    operation: str,
    account_id: str,
    sequence_number: int,
    status: str = "ACTIVE",
    industry: str = "Technology",
) -> NormalizedCDCEvent:
    """Helper to create a valid NormalizedCDCEvent for accounts."""
    payload = (
        {
            "account_id": account_id,
            "account_name": f"Account {account_id}",
            "industry": industry,
            "country": "US",
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
            "industry": industry,
            "country": "US",
            "status": "SUSPENDED" if status == "ACTIVE" else "ACTIVE",
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
        batch_id="batch_cdf_001",
        payload=payload,
        before_payload=before_payload,
        source_system="b2b_saas_postgres",
        entity_sequence_key=f'accounts:{{"account_id":"{account_id}"}}',
        business_key_canonical=f'{{"account_id":"{account_id}"}}',
        event_fingerprint=f"fp_{event_id}_{sequence_number}",
        is_late_arrival=False,
        source_file="batch_cdf_001/accounts.jsonl",
        ingestion_batch_id="batch_cdf_001",
    )


def test_delta_cdf_end_to_end_lifecycle_with_module4(spark_session: SparkSession, tmp_path: Path):
    """Verify complete end-to-end Delta CDF lifecycle using real Module 4 mutations and downstream archive.

    1. Bootstrap Module 4 current-state targets from Module 1 snapshot.
    2. Register & enable CDF via Module 6; capture CDF start version.
    3. Apply real Module 4 operations: INSERT, UPDATE, HARD DELETE.
    4. Assert CDF outputs: insert, update_preimage, update_postimage, delete.
    5. Archive changes and advance checkpoint.
    6. Replay same range and verify archive idempotency (0 duplicates).
    """
    delta_dir = tmp_path / "delta"
    current_dir = delta_dir / "current"
    ledger_dir = delta_dir / "control" / "event_apply_ledger"
    archive_dir = delta_dir / "downstream" / "cdf_archive"
    control_db = tmp_path / "control" / "cdf_consumer.db"

    # 1. Initialize Module 4 Delta Targets
    source_gen = SourceGenerator(SnapshotConfig(seed=42))
    initial_snapshot = source_gen.generate_snapshot_dicts()

    merge_pipeline = DeltaMergePipeline(
        spark=spark_session,
        target_base_dir=current_dir,
        ledger_base_dir=ledger_dir,
        delete_policy=DeletePolicy.HARD,
    )
    merge_pipeline.target_store.initialize_targets(initial_snapshot)

    # 2. Register & Enable CDF via Module 6
    state_store = CDFStateStore(db_path=control_db)
    reader = CDFReader(spark_session)
    archive_store = CDFArchiveStore(spark_session, archive_base_dir=archive_dir)

    cdf_pipeline = CDFDownstreamPipeline(
        spark=spark_session,
        target_store=merge_pipeline.target_store,
        state_store=state_store,
        reader=reader,
        archive_store=archive_store,
    )

    reg = cdf_pipeline.register_table("accounts")
    # Initial snapshot was version 0, enabling CDF produced version 1
    assert reg.cdf_start_version == 1
    assert reg.last_processed_version == 0

    # Consume initial enabling commit (empty data rows)
    res_init = cdf_pipeline.consume_table("accounts")
    assert res_init.checkpoint_after == 1
    assert res_init.input_change_rows == 0

    # 3. Apply real Module 4 operations
    # - INSERT new account ACC-0041
    # - UPDATE existing account ACC-0001 (initial snapshot status is SUSPENDED -> update to ACTIVE)
    # - HARD DELETE existing account ACC-0002
    ev_ins = _make_accounts_event("evt_ins_01", "INSERT", "ACC-0041", sequence_number=1, status="ACTIVE")
    ev_upd = _make_accounts_event("evt_upd_01", "UPDATE", "ACC-0001", sequence_number=10, status="ACTIVE")
    ev_del = _make_accounts_event("evt_del_01", "DELETE", "ACC-0002", sequence_number=20)

    merge_res = merge_pipeline.run([ev_ins, ev_upd, ev_del], processing_id="proc_cdf_e2e")
    assert merge_res.status == "SUCCESS"
    assert merge_res.events_applied == 3

    current_version = merge_pipeline.target_store.get_table_version("accounts")
    assert current_version > 1

    # 4. Read CDF and assert all change types
    cdf_df = cdf_pipeline.replay_range("accounts", start_version=2, end_version=current_version)
    cdf_rows = cdf_df.collect()

    change_types = {r["_change_type"] for r in cdf_rows}
    assert "insert" in change_types
    assert "update_preimage" in change_types
    assert "update_postimage" in change_types
    assert "delete" in change_types

    # Assert INSERT record
    ins_row = [r for r in cdf_rows if r["account_id"] == "ACC-0041"][0]
    assert ins_row["_change_type"] == "insert"
    assert ins_row["status"] == "ACTIVE"
    assert ins_row["_last_processing_id"] == "proc_cdf_e2e"

    # Assert UPDATE preimage and postimage pair
    upd_pre = [r for r in cdf_rows if r["account_id"] == "ACC-0001" and r["_change_type"] == "update_preimage"][0]
    upd_post = [r for r in cdf_rows if r["account_id"] == "ACC-0001" and r["_change_type"] == "update_postimage"][0]
    assert upd_pre["_commit_version"] == upd_post["_commit_version"]
    assert upd_pre["status"] == "SUSPENDED"
    assert upd_post["status"] == "ACTIVE"

    # Assert HARD DELETE record
    del_row = [r for r in cdf_rows if r["account_id"] == "ACC-0002"][0]
    assert del_row["_change_type"] == "delete"
    assert del_row["account_name"] == "Healthcare Solutions 2"

    # 5. Archive changes and advance checkpoint
    consume_res = cdf_pipeline.consume_table("accounts")
    assert consume_res.no_op is False
    assert consume_res.start_version == 2
    assert consume_res.end_version == current_version
    assert consume_res.input_change_rows == len(cdf_rows)
    assert consume_res.archive_rows_inserted == len(cdf_rows)
    assert consume_res.checkpoint_after == current_version

    # Verify archive table content
    archive_df = archive_store.read_archive("accounts")
    assert archive_df.count() == len(cdf_rows)
    assert "_change_id" in archive_df.columns
    assert "_source_table" in archive_df.columns

    # 6. Replay range directly into archive to verify idempotency (0 duplicates)
    inserted_on_replay = archive_store.write_changes("accounts", cdf_df, primary_key="account_id")
    assert inserted_on_replay == 0
    assert archive_store.read_archive("accounts").count() == len(cdf_rows)


def test_delta_cdf_soft_delete_represented_as_update_pre_and_postimage(
    spark_session: SparkSession, tmp_path: Path
):
    """Verify that a soft delete (_is_deleted=True) is recorded by CDF as update_preimage/postimage, NOT delete."""
    delta_dir = tmp_path / "delta"
    current_dir = delta_dir / "current"
    ledger_dir = delta_dir / "control" / "event_apply_ledger"

    source_gen = SourceGenerator(SnapshotConfig(seed=42))
    initial_snapshot = source_gen.generate_snapshot_dicts()

    # Configure soft delete policy
    merge_pipeline = DeltaMergePipeline(
        spark=spark_session,
        target_base_dir=current_dir,
        ledger_base_dir=ledger_dir,
        delete_policy=DeletePolicy.SOFT,
    )
    merge_pipeline.target_store.initialize_targets(initial_snapshot)

    reader = CDFReader(spark_session)
    acc_path = merge_pipeline.target_store.get_table_path("accounts")
    start_v = reader.enable_cdf(acc_path)

    # Soft delete ACC-0003
    del_ev = _make_accounts_event("evt_soft_del", "DELETE", "ACC-0003", sequence_number=30)
    merge_pipeline.run([del_ev], processing_id="proc_soft_del")

    end_v = merge_pipeline.target_store.get_table_version("accounts")
    cdf_df = reader.read_changes(acc_path, start_version=start_v + 1, end_version=end_v)
    rows = cdf_df.filter(cdf_df.account_id == "ACC-0003").collect()

    change_types = [r["_change_type"] for r in rows]
    # In soft delete, the row is updated with _is_deleted=True, not physically removed
    assert "delete" not in change_types
    assert "update_preimage" in change_types
    assert "update_postimage" in change_types

    post_image = [r for r in rows if r["_change_type"] == "update_postimage"][0]
    assert post_image["_is_deleted"] is True
