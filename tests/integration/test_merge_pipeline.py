"""Integration tests for Delta Lake MERGE, recovery, and mutation oracle reconciliation."""

import json
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.cdc.generator import CDCScenarioGenerator
from src.merge.models import (
    AppliedEventConflictError,
    AppliedSequenceConflictError,
    DeletePolicy,
    LedgerStatus,
    MergeError,
    PendingRecoveryError,
)
from src.merge.pipeline import DeltaMergePipeline
from src.merge.reconciliation import reconcile_delta_against_mutation_oracle
from src.normalization.models import NormalizedCDCEvent
from src.normalization.pipeline import CDCNormalizationPipeline
from src.source.generator import SnapshotConfig, SourceGenerator
from src.source.mutation_engine import SourceMutationEngine


def _write_cdc_landing_files(events: list[dict], landing_dir: Path, batch_id: str) -> list[Path]:
    batch_dir = landing_dir / f"batch_id={batch_id}"
    batch_dir.mkdir(parents=True, exist_ok=True)
    by_tbl: dict[str, list[dict]] = {}
    for ev in events:
        tbl = ev.get("table_name") or "unknown"
        by_tbl.setdefault(tbl, []).append(ev)

    created: list[Path] = []
    for tbl, recs in by_tbl.items():
        fp = batch_dir / f"{tbl}.jsonl"
        with open(fp, "w", encoding="utf-8") as f:
            for r in recs:
                f.write(json.dumps(r) + "\n")
        created.append(fp)
    return created


def _make_sample_event(
    event_id: str,
    table_name: str = "accounts",
    operation: str = "UPDATE",
    account_id: str = "ACC-0001",
    sequence_number: int = 10,
    status: str = "ACTIVE",
    fingerprint: str | None = None,
) -> NormalizedCDCEvent:
    payload = (
        {
            "account_id": account_id,
            "account_name": f"Account {account_id}",
            "industry": "Technology",
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
            "industry": "Technology",
            "country": "US",
            "status": "SUSPENDED",
            "created_at": "2026-05-11T01:00:00Z",
            "updated_at": "2026-05-11T01:00:00Z",
        }
        if operation in ("UPDATE", "DELETE")
        else None
    )

    return NormalizedCDCEvent(
        event_id=event_id,
        table_name=table_name,
        operation=operation,
        business_key={"account_id": account_id},
        sequence_number=sequence_number,
        event_timestamp="2026-05-11T01:00:00Z",
        source_commit_timestamp="2026-05-11T01:00:01Z",
        batch_id="batch_001",
        payload=payload,
        before_payload=before_payload,
        source_system="b2b_saas_postgres",
        entity_sequence_key=f'{table_name}:{{"account_id":"{account_id}"}}',
        business_key_canonical=f'{{"account_id":"{account_id}"}}',
        event_fingerprint=fingerprint or f"fp_{event_id}_{sequence_number}",
        is_late_arrival=False,
        source_file=f"batch_id=batch_001/{table_name}.jsonl",
        ingestion_batch_id="batch_001",
    )


def test_merge_pipeline_clean_batch_1_application(spark_session: SparkSession):
    """Verify Batch 1 clean inserts and updates mutate Delta targets and register APPLIED in ledger."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        snapshot = source_gen.generate_snapshot_dicts()

        # Initialize Target Store & Ledger
        pipeline = DeltaMergePipeline(
            spark=spark_session,
            target_base_dir=base / "delta" / "current",
            ledger_base_dir=base / "delta" / "control" / "event_apply_ledger",
        )
        pipeline.target_store.initialize_targets(snapshot)

        # Generate Batch 1 & Normalize
        cdc_gen = CDCScenarioGenerator(source_gen)
        b1_events = [e.to_dict() for e in cdc_gen.generate_batch_1_inserts_and_updates("batch_001")]
        b1_files = _write_cdc_landing_files(b1_events, base / "cdc_landing", "batch_001")

        norm_p = CDCNormalizationPipeline(
            spark=spark_session,
            normalized_base_dir=base / "norm",
            quarantine_base_dir=base / "quar",
        )
        accepted, _, norm_metrics = norm_p.run_pipeline(b1_files)
        assert len(accepted) == 8

        # Run Delta Merge Pipeline
        res = pipeline.run(accepted)
        assert res.status == "SUCCESS"
        assert res.events_applied == 8
        assert res.fresh_events == 8
        assert res.insert_events_applied == 4
        assert res.update_events_applied == 4
        assert res.pending_events_remaining == 0

        # Ledger check
        ledger_records = pipeline.event_ledger.get_all_ledger_records()
        assert len(ledger_records) == 8
        assert all(r.status == LedgerStatus.APPLIED.value for r in ledger_records)


def test_merge_pipeline_advanced_batch_2_out_of_order_101_then_102(spark_session: SparkSession):
    """Verify out-of-order events 102 and 101 for ACC-0002 are applied deterministically: 101 then 102."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        snapshot = source_gen.generate_snapshot_dicts()

        pipeline = DeltaMergePipeline(
            spark=spark_session,
            target_base_dir=base / "delta" / "current",
            ledger_base_dir=base / "delta" / "control" / "event_apply_ledger",
        )
        pipeline.target_store.initialize_targets(snapshot)

        cdc_gen = CDCScenarioGenerator(source_gen)
        b1_events = [e.to_dict() for e in cdc_gen.generate_batch_1_inserts_and_updates("batch_001")]
        b2_events = [e.to_dict() for e in cdc_gen.generate_batch_2_advanced_cdc_scenarios("batch_002")]

        files_b1 = _write_cdc_landing_files(b1_events, base / "cdc_landing", "batch_001")
        files_b2 = _write_cdc_landing_files(b2_events, base / "cdc_landing", "batch_002")

        norm_p = CDCNormalizationPipeline(spark=spark_session, normalized_base_dir=base / "norm", quarantine_base_dir=base / "quar")
        accepted, _, _ = norm_p.run_pipeline(files_b1 + files_b2)

        res = pipeline.run(accepted)
        assert res.status == "SUCCESS"

        # Check ACC-0002 state in accounts table
        acc_df = pipeline.target_store.read_current_table("accounts")
        acc_0002 = acc_df.filter(acc_df.account_id == "ACC-0002").first()
        assert acc_0002["_last_sequence_number"] == 102
        assert acc_0002["status"] == "TRIAL"


def test_merge_pipeline_replay_exact_no_op_and_version_stability(spark_session: SparkSession):
    """Verify exact replay of a previously applied processing set is a no-op with unchanged Delta table versions."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        snapshot = source_gen.generate_snapshot_dicts()

        pipeline = DeltaMergePipeline(
            spark=spark_session,
            target_base_dir=base / "delta" / "current",
            ledger_base_dir=base / "delta" / "control" / "event_apply_ledger",
        )
        pipeline.target_store.initialize_targets(snapshot)

        cdc_gen = CDCScenarioGenerator(source_gen)
        b1_events = [e.to_dict() for e in cdc_gen.generate_batch_1_inserts_and_updates("batch_001")]
        files = _write_cdc_landing_files(b1_events, base / "cdc_landing", "batch_001")

        norm_p = CDCNormalizationPipeline(spark=spark_session, normalized_base_dir=base / "norm", quarantine_base_dir=base / "quar")
        accepted, _, _ = norm_p.run_pipeline(files)

        # Run 1: Apply
        res1 = pipeline.run(accepted)
        assert res1.events_applied == 8

        # Record versions after Run 1
        v_accounts_1 = pipeline.target_store.get_table_version("accounts")
        ledger_count_1 = len(pipeline.event_ledger.get_all_ledger_records())

        # Run 2: Replay same events
        res2 = pipeline.run(accepted)
        assert res2.status == "SUCCESS_WITH_SKIPS"
        assert res2.events_applied == 0
        assert res2.replay_events_skipped == 8

        # Delta version and ledger counts remain unchanged
        v_accounts_2 = pipeline.target_store.get_table_version("accounts")
        ledger_count_2 = len(pipeline.event_ledger.get_all_ledger_records())
        assert v_accounts_1 == v_accounts_2
        assert ledger_count_1 == ledger_count_2


def test_merge_pipeline_stale_event_lower_sequence_skipped(spark_session: SparkSession):
    """Verify an event with sequence_number < max_applied_sequence is classified STALE and skipped."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        pipeline = DeltaMergePipeline(
            spark=spark_session,
            target_base_dir=base / "delta" / "current",
            ledger_base_dir=base / "delta" / "control" / "event_apply_ledger",
        )
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        pipeline.target_store.initialize_targets(source_gen.generate_snapshot_dicts())

        # Apply event at sequence 50
        ev_50 = _make_sample_event("evt_seq_50", sequence_number=50, status="ACTIVE")
        res1 = pipeline.run([ev_50])
        assert res1.events_applied == 1

        # Submit older event at sequence 30 for same entity
        ev_30 = _make_sample_event("evt_seq_30", sequence_number=30, status="SUSPENDED")
        res2 = pipeline.run([ev_30])
        assert res2.status == "SUCCESS_WITH_SKIPS"
        assert res2.events_applied == 0
        assert res2.stale_events_skipped == 1

        # Target table remains at sequence 50
        df = pipeline.target_store.read_current_table("accounts")
        row = df.filter(df.account_id == "ACC-0001").first()
        assert row["_last_sequence_number"] == 50
        assert row["status"] == "ACTIVE"


def test_merge_pipeline_cross_processing_equal_sequence_conflict(spark_session: SparkSession):
    """Verify equal-sequence with different event_id across processing runs raises AppliedSequenceConflictError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        pipeline = DeltaMergePipeline(
            spark=spark_session,
            target_base_dir=base / "delta" / "current",
            ledger_base_dir=base / "delta" / "control" / "event_apply_ledger",
        )
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        pipeline.target_store.initialize_targets(source_gen.generate_snapshot_dicts())

        # Apply event A at sequence 50
        ev_a = _make_sample_event("evt_a", sequence_number=50, fingerprint="fp_a")
        pipeline.run([ev_a])

        # Submit distinct event B at sequence 50 for same entity
        ev_b = _make_sample_event("evt_b", sequence_number=50, fingerprint="fp_b")
        with pytest.raises(AppliedSequenceConflictError):
            pipeline.run([ev_b])


def test_merge_pipeline_cross_processing_event_id_conflict(spark_session: SparkSession):
    """Verify same event_id with different fingerprint across processing runs raises AppliedEventConflictError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        pipeline = DeltaMergePipeline(
            spark=spark_session,
            target_base_dir=base / "delta" / "current",
            ledger_base_dir=base / "delta" / "control" / "event_apply_ledger",
        )
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        pipeline.target_store.initialize_targets(source_gen.generate_snapshot_dicts())

        ev1 = _make_sample_event("evt_same_id", sequence_number=10, fingerprint="fp_v1")
        pipeline.run([ev1])

        ev2 = _make_sample_event("evt_same_id", sequence_number=10, fingerprint="fp_v2_conflicting")
        with pytest.raises(AppliedEventConflictError):
            pipeline.run([ev2])


def test_merge_pipeline_hard_delete_stale_resurrection_protection(spark_session: SparkSession):
    """Verify that after a HARD delete at sequence 30, an older replay at sequence 10 does not resurrect the entity."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        pipeline = DeltaMergePipeline(
            spark=spark_session,
            target_base_dir=base / "delta" / "current",
            ledger_base_dir=base / "delta" / "control" / "event_apply_ledger",
            delete_policy=DeletePolicy.HARD,
        )
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        pipeline.target_store.initialize_targets(source_gen.generate_snapshot_dicts())

        # 1. ACC-0001 exists in snapshot
        df = pipeline.target_store.read_current_table("accounts")
        assert df.filter(df.account_id == "ACC-0001").count() == 1

        # 2. Hard DELETE at sequence 30
        del_ev = _make_sample_event("evt_del_30", operation="DELETE", account_id="ACC-0001", sequence_number=30)
        res_del = pipeline.run([del_ev])
        assert res_del.delete_events_applied == 1

        # 3. ACC-0001 is physically deleted
        df_del = pipeline.target_store.read_current_table("accounts")
        assert df_del.filter(df_del.account_id == "ACC-0001").count() == 0

        # 4. Replay an older INSERT/UPDATE at sequence 10
        stale_ev = _make_sample_event("evt_ins_10", operation="INSERT", account_id="ACC-0001", sequence_number=10)
        res_stale = pipeline.run([stale_ev])
        assert res_stale.status == "SUCCESS_WITH_SKIPS"
        assert res_stale.stale_events_skipped == 1

        # 5. Row is NOT resurrected
        df_final = pipeline.target_store.read_current_table("accounts")
        assert df_final.filter(df_final.account_id == "ACC-0001").count() == 0


def test_merge_pipeline_failure_after_pending_and_recovery(spark_session: SparkSession):
    """Verify crash after writing PENDING before target MERGE recovers safely on retry."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        pipeline = DeltaMergePipeline(
            spark=spark_session,
            target_base_dir=base / "delta" / "current",
            ledger_base_dir=base / "delta" / "control" / "event_apply_ledger",
        )
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        pipeline.target_store.initialize_targets(source_gen.generate_snapshot_dicts())

        ev = _make_sample_event("evt_crash_pending", account_id="ACC-0041", sequence_number=1)

        # Run 1: Injected crash after PENDING
        with pytest.raises(MergeError):
            pipeline.run([ev], fail_after_pending_group=1)

        # Target table must NOT have ACC-0041
        df = pipeline.target_store.read_current_table("accounts")
        assert df.filter(df.account_id == "ACC-0041").count() == 0

        # Ledger has 1 PENDING row
        pending = pipeline.event_ledger.get_pending_records()
        assert len(pending) == 1
        assert pending[0].event_id == "evt_crash_pending"

        # Run 2: Retry same input
        res = pipeline.run([ev])
        assert res.status == "SUCCESS"
        assert res.events_applied == 1
        assert res.recovered_pending_events == 1
        assert res.pending_events_remaining == 0

        # Target table now has ACC-0041 exactly once
        df_after = pipeline.target_store.read_current_table("accounts")
        assert df_after.filter(df_after.account_id == "ACC-0041").count() == 1


def test_merge_pipeline_failure_after_target_and_recovery(spark_session: SparkSession):
    """Verify crash after target mutation before marking APPLIED recovers idempotently on retry."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        pipeline = DeltaMergePipeline(
            spark=spark_session,
            target_base_dir=base / "delta" / "current",
            ledger_base_dir=base / "delta" / "control" / "event_apply_ledger",
        )
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        pipeline.target_store.initialize_targets(source_gen.generate_snapshot_dicts())

        ev = _make_sample_event("evt_crash_target", account_id="ACC-0041", sequence_number=1)

        # Run 1: Injected crash after target mutation
        with pytest.raises(MergeError):
            pipeline.run([ev], fail_after_target_group=1)

        # Target table already has ACC-0041
        df = pipeline.target_store.read_current_table("accounts")
        assert df.filter(df.account_id == "ACC-0041").count() == 1

        # But ledger is still PENDING
        pending = pipeline.event_ledger.get_pending_records()
        assert len(pending) == 1
        assert pending[0].event_id == "evt_crash_target"

        # Run 2: Retry same input
        res = pipeline.run([ev])
        assert res.status == "SUCCESS"
        assert res.recovered_pending_events == 1
        assert res.pending_events_remaining == 0

        # Target table still has ACC-0041 exactly once (no duplicate rows)
        df_after = pipeline.target_store.read_current_table("accounts")
        assert df_after.filter(df_after.account_id == "ACC-0041").count() == 1
        assert len(pipeline.event_ledger.get_pending_records()) == 0


def test_merge_pipeline_hard_delete_failure_and_recovery(spark_session: SparkSession):
    """Verify crash after physical target DELETE before marking APPLIED recovers idempotently."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        pipeline = DeltaMergePipeline(
            spark=spark_session,
            target_base_dir=base / "delta" / "current",
            ledger_base_dir=base / "delta" / "control" / "event_apply_ledger",
            delete_policy=DeletePolicy.HARD,
        )
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        pipeline.target_store.initialize_targets(source_gen.generate_snapshot_dicts())

        del_ev = _make_sample_event("evt_del_fail", operation="DELETE", account_id="ACC-0001", sequence_number=10)

        # Run 1: Injected crash after target delete
        with pytest.raises(MergeError):
            pipeline.run([del_ev], fail_after_target_group=1)

        # Target row is deleted, ledger still PENDING
        df = pipeline.target_store.read_current_table("accounts")
        assert df.filter(df.account_id == "ACC-0001").count() == 0
        assert len(pipeline.event_ledger.get_pending_records()) == 1

        # Run 2: Retry
        res = pipeline.run([del_ev])
        assert res.status == "SUCCESS"
        assert res.delete_events_applied == 1
        assert len(pipeline.event_ledger.get_pending_records()) == 0


def test_merge_pipeline_unresolved_pending_blocks_unrelated_processing(spark_session: SparkSession):
    """Verify that an unrelated new processing batch cannot run if unresolved PENDING events exist in the ledger."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        pipeline = DeltaMergePipeline(
            spark=spark_session,
            target_base_dir=base / "delta" / "current",
            ledger_base_dir=base / "delta" / "control" / "event_apply_ledger",
        )
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        pipeline.target_store.initialize_targets(source_gen.generate_snapshot_dicts())

        ev1 = _make_sample_event("evt_interrupted", account_id="ACC-0041", sequence_number=1)

        # Crash ev1 in PENDING
        with pytest.raises(MergeError):
            pipeline.run([ev1], fail_after_pending_group=1)

        # Attempt to run unrelated ev2
        ev2 = _make_sample_event("evt_unrelated", account_id="ACC-0002", sequence_number=5)
        with pytest.raises(PendingRecoveryError):
            pipeline.run([ev2])


def test_merge_pipeline_late_fresh_event_applied(spark_session: SparkSession):
    """Verify a late historical event with sequence > max_applied is accepted and applied."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        pipeline = DeltaMergePipeline(
            spark=spark_session,
            target_base_dir=base / "delta" / "current",
            ledger_base_dir=base / "delta" / "control" / "event_apply_ledger",
        )
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        pipeline.target_store.initialize_targets(source_gen.generate_snapshot_dicts())

        # ACC-0001 applied at sequence 10
        ev1 = _make_sample_event("evt_1", sequence_number=10)
        pipeline.run([ev1])

        # Late event arriving with sequence 20 (fresh sequence)
        ev_late_fresh = _make_sample_event("evt_late_fresh", sequence_number=20, status="SUSPENDED")
        ev_late_fresh.is_late_arrival = True
        res = pipeline.run([ev_late_fresh])

        assert res.events_applied == 1
        df = pipeline.target_store.read_current_table("accounts")
        acc = df.filter(df.account_id == "ACC-0001").first()
        assert acc["_last_sequence_number"] == 20
        assert acc["status"] == "SUSPENDED"


def test_merge_pipeline_late_stale_event_skipped(spark_session: SparkSession):
    """Verify a late event with sequence < max_applied is skipped as STALE."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        pipeline = DeltaMergePipeline(
            spark=spark_session,
            target_base_dir=base / "delta" / "current",
            ledger_base_dir=base / "delta" / "control" / "event_apply_ledger",
        )
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        pipeline.target_store.initialize_targets(source_gen.generate_snapshot_dicts())

        # ACC-0001 applied at sequence 50
        ev1 = _make_sample_event("evt_1", sequence_number=50, status="ACTIVE")
        pipeline.run([ev1])

        # Late event arriving with sequence 20 (stale sequence)
        ev_late_stale = _make_sample_event("evt_late_stale", sequence_number=20, status="SUSPENDED")
        ev_late_stale.is_late_arrival = True
        res = pipeline.run([ev_late_stale])

        assert res.stale_events_skipped == 1
        assert res.events_applied == 0
        df = pipeline.target_store.read_current_table("accounts")
        acc = df.filter(df.account_id == "ACC-0001").first()
        assert acc["_last_sequence_number"] == 50
        assert acc["status"] == "ACTIVE"


def test_merge_pipeline_full_module1_oracle_reconciliation(spark_session: SparkSession):
    """Verify full end-to-end reconciliation: Delta current-state matches Module 1 mutation oracle exactly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        initial_snapshot = source_gen.generate_snapshot_dicts()

        # 1. Initialize Delta Target Store & Ledger
        pipeline = DeltaMergePipeline(
            spark=spark_session,
            target_base_dir=base / "delta" / "current",
            ledger_base_dir=base / "delta" / "control" / "event_apply_ledger",
            delete_policy=DeletePolicy.HARD,
        )
        pipeline.target_store.initialize_targets(initial_snapshot)

        # 2. Initialize Module 1 Mutation Oracle
        mutation_oracle = SourceMutationEngine(initial_state=initial_snapshot)

        # 3. Generate All Module 1 CDC batches & Normalize via Module 3
        cdc_gen = CDCScenarioGenerator(source_gen)
        all_batches = cdc_gen.generate_all_batches()

        all_files: list[Path] = []
        for b_id, events in all_batches.items():
            dict_events = [e.to_dict() if hasattr(e, "to_dict") else e for e in events]
            all_files.extend(_write_cdc_landing_files(dict_events, base / "cdc_landing", b_id))

        norm_p = CDCNormalizationPipeline(
            spark=spark_session,
            normalized_base_dir=base / "norm",
            quarantine_base_dir=base / "quar",
        )
        accepted, _, _ = norm_p.run_pipeline(all_files)

        # 4. Apply accepted events to Delta Lake tables via Module 4 pipeline
        res = pipeline.run(accepted)
        assert res.status == "SUCCESS"
        assert res.events_applied == 12  # Batch 1 (8) + Batch 2 valid accepted (4)

        # 5. Apply accepted events to the Module 1 mutation oracle
        for ev in accepted:
            mutation_oracle.apply_event(ev.to_dict())

        # 6. Reconcile Delta state against Oracle
        report = reconcile_delta_against_mutation_oracle(pipeline.target_store, mutation_oracle)
        assert report["is_reconciled"] is True, f"Reconciliation failed: {report['mismatches']}"

        # 7. Exact count assertions derived from oracle lifecycle
        counts = report["counts"]
        assert counts["accounts"]["delta_count"] == 41
        assert counts["subscriptions"]["delta_count"] == 61
        assert counts["invoices"]["delta_count"] == 121
        assert counts["payments"]["delta_count"] == 90  # PAY-0091 inserted (B1) - PAY-0002 deleted (B2) from 90 initial
