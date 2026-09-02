"""Integration tests for the complete transactional watermark incremental ingestion pipeline."""

import tempfile
from pathlib import Path

import pytest

from src.cdc.generator import CDCScenarioGenerator
from src.source.generator import SnapshotConfig, SourceGenerator
from src.source.mutation_engine import SourceMutationEngine
from src.watermark.control_store import SQLiteWatermarkControlStore
from src.watermark.models import (
    CompositeWatermark,
    WatermarkError,
    WatermarkRunAudit,
    WatermarkRunStatus,
)
from src.watermark.pipeline import WatermarkPipeline
from src.watermark.source_adapter import InMemorySourceAdapter


def test_full_watermark_lifecycle_and_incremental_extraction():
    """Verify complete end-to-end watermark ingestion lifecycle:

    1. Initial Full Extraction (accounts: 40, subscriptions: 60, invoices: 120, payments: 90)
    2. Zero-change NO_DATA rerun
    3. Applying Module 1 Batch 1 mutations to source engine
    4. Incremental extraction extracting only changed records (2 per table) with exact keys
    5. Physical-delete blind spot demonstration (deleted record cannot be detected)
    6. Restart persistence across control store reload
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        base_path = Path(tmpdir)
        db_path = base_path / "control.db"
        landing_dir = base_path / "landing"

        # Step 1: Initialize deterministic seed-42 source snapshot
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        initial_tables = source_gen.generate_snapshot_dicts()
        mutation_engine = SourceMutationEngine(initial_state=initial_tables)

        control_store = SQLiteWatermarkControlStore(db_path)
        source_adapter = InMemorySourceAdapter(source_tables=mutation_engine.get_snapshot_state())
        pipeline = WatermarkPipeline(
            control_store=control_store,
            source_adapter=source_adapter,
            landing_base_dir=landing_dir,
        )

        # ----------------------------------------------------------------------
        # Phase 1: Initial Full Load
        # ----------------------------------------------------------------------
        results = pipeline.run_all_tables()

        assert results["accounts"].status == WatermarkRunStatus.SUCCESS
        assert results["accounts"].rows_extracted == 40

        assert results["subscriptions"].status == WatermarkRunStatus.SUCCESS
        assert results["subscriptions"].rows_extracted == 60

        assert results["invoices"].status == WatermarkRunStatus.SUCCESS
        assert results["invoices"].rows_extracted == 120

        assert results["payments"].status == WatermarkRunStatus.SUCCESS
        assert results["payments"].rows_extracted == 90

        # Verify landing files exist
        for _table, res in results.items():
            assert res.landing_path is not None
            assert Path(res.landing_path).exists()

        # ----------------------------------------------------------------------
        # Phase 2: Immediate Rerun -> NO_DATA
        # ----------------------------------------------------------------------
        rerun_results = pipeline.run_all_tables()
        for _table, res in rerun_results.items():
            assert res.status == WatermarkRunStatus.NO_DATA
            assert res.rows_extracted == 0
            assert res.landing_path is None

        # ----------------------------------------------------------------------
        # Phase 3: Apply Module 1 Batch 1 Mutations to Source Mutation Engine
        # ----------------------------------------------------------------------
        cdc_gen = CDCScenarioGenerator(source_generator=source_gen)
        batch_1_events = cdc_gen.generate_batch_1_inserts_and_updates("batch_001")
        apply_res = mutation_engine.apply_batch(batch_1_events, sort_by_sequence=True)
        assert apply_res.applied_count == 8  # 4 inserts + 4 updates

        # Update source adapter with the new current state
        source_adapter._tables = mutation_engine.get_snapshot_state()

        # ----------------------------------------------------------------------
        # Phase 4: Incremental Extraction
        # ----------------------------------------------------------------------
        inc_results = pipeline.run_all_tables()

        # Accounts: 1 insert (ACC-0041) + 1 update (ACC-0001) = 2
        acc_res = inc_results["accounts"]
        assert acc_res.status == WatermarkRunStatus.SUCCESS
        assert acc_res.rows_extracted == 2
        acc_keys = {r["account_id"] for r in acc_res.records}
        assert acc_keys == {"ACC-0041", "ACC-0001"}

        # Subscriptions: 1 insert (SUB-0061) + 1 update (SUB-0001) = 2
        sub_res = inc_results["subscriptions"]
        assert sub_res.status == WatermarkRunStatus.SUCCESS
        assert sub_res.rows_extracted == 2
        sub_keys = {r["subscription_id"] for r in sub_res.records}
        assert sub_keys == {"SUB-0061", "SUB-0001"}

        # Invoices: 1 insert (INV-0121) + 1 update (INV-0001) = 2
        inv_res = inc_results["invoices"]
        assert inv_res.status == WatermarkRunStatus.SUCCESS
        assert inv_res.rows_extracted == 2
        inv_keys = {r["invoice_id"] for r in inv_res.records}
        assert inv_keys == {"INV-0121", "INV-0001"}

        # Payments: 1 insert (PAY-0091) + 1 update (PAY-0001) = 2
        pay_res = inc_results["payments"]
        assert pay_res.status == WatermarkRunStatus.SUCCESS
        assert pay_res.rows_extracted == 2
        pay_keys = {r["payment_id"] for r in pay_res.records}
        assert pay_keys == {"PAY-0091", "PAY-0001"}

        # ----------------------------------------------------------------------
        # Phase 5: Physical Delete Blind-Spot Demonstration
        # ----------------------------------------------------------------------
        # Hard-delete PAY-0002 from the mutation engine
        batch_2_events = cdc_gen.generate_batch_2_advanced_cdc_scenarios("batch_002")
        del_event = next(e for e in batch_2_events if e.event_id == "evt_del_pay_0002")
        mutation_engine.apply_event(del_event)
        assert mutation_engine.get_record("payments", "PAY-0002") is None

        # Expose mutated state to watermark adapter
        source_adapter._tables = mutation_engine.get_snapshot_state()

        # Run watermark extraction for payments
        del_run_res = pipeline.run_table_extraction("payments")

        # The deleted record cannot be emitted because it no longer exists in source tables
        assert del_run_res.status == WatermarkRunStatus.NO_DATA
        assert del_run_res.rows_extracted == 0

        # ----------------------------------------------------------------------
        # Phase 6: Control Store Restart Persistence
        # ----------------------------------------------------------------------
        control_store.close()
        new_control_store = SQLiteWatermarkControlStore(db_path)
        state_acc = new_control_store.get_or_create_watermark_state("accounts")
        assert state_acc.version == 3  # Initial (1->2) + Incremental (2->3)
        assert state_acc.last_watermark.key == "ACC-0001"
        new_control_store.close()


def test_bounded_high_watermark_isolation():
    """Verify that records modified after HIGH watermark is captured are excluded

    from current run and extracted in the subsequent run.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        base_path = Path(tmpdir)
        db_path = base_path / "control.db"
        landing_dir = base_path / "landing"

        tables = {
            "accounts": [
                {
                    "account_id": "ACC-0001",
                    "account_name": "Co 1",
                    "updated_at": "2026-01-01T10:00:00Z",
                },
                {
                    "account_id": "ACC-0002",
                    "account_name": "Co 2",
                    "updated_at": "2026-01-01T11:00:00Z",
                },
            ]
        }

        control_store = SQLiteWatermarkControlStore(db_path)
        source_adapter = InMemorySourceAdapter(source_tables=tables)
        pipeline = WatermarkPipeline(
            control_store=control_store,
            source_adapter=source_adapter,
            landing_base_dir=landing_dir,
        )

        # Hook to inject a new row with higher updated_at immediately after HIGH is captured
        def mutate_source_after_high_capture():
            tables["accounts"].append(
                {
                    "account_id": "ACC-0003",
                    "account_name": "Co 3",
                    "updated_at": "2026-01-01T15:00:00Z",
                }
            )

        # Run 1: Extracts only ACC-0001 and ACC-0002 (up to frozen HIGH 11:00:00Z)
        res1 = pipeline.run_table_extraction(
            table_name="accounts",
            post_capture_hook=mutate_source_after_high_capture,
        )

        assert res1.status == WatermarkRunStatus.SUCCESS
        assert res1.rows_extracted == 2
        assert {r["account_id"] for r in res1.records} == {"ACC-0001", "ACC-0002"}

        # Run 2: Next run extracts ACC-0003
        res2 = pipeline.run_table_extraction(table_name="accounts")
        assert res2.status == WatermarkRunStatus.SUCCESS
        assert res2.rows_extracted == 1
        assert res2.records[0]["account_id"] == "ACC-0003"


def test_failed_window_high_reused_despite_source_mutation():
    """CRITICAL RECOVERY TEST:

    Verify that if an extraction run captures HIGH and lands data but fails before commit,
    a retry reuses the EXACT SAME prior HIGH and batch_id, even if the source table
    has mutated with newer records before the retry occurs.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        base_path = Path(tmpdir)
        db_path = base_path / "control.db"
        landing_dir = base_path / "landing"

        tables = {
            "accounts": [
                {
                    "account_id": "ACC-0001",
                    "account_name": "Co 1",
                    "updated_at": "2026-01-01T10:00:00Z",
                },
            ]
        }

        control_store = SQLiteWatermarkControlStore(db_path)
        source_adapter = InMemorySourceAdapter(source_tables=tables)
        pipeline = WatermarkPipeline(
            control_store=control_store,
            source_adapter=source_adapter,
            landing_base_dir=landing_dir,
        )

        # Attempt 1: Captures HIGH (10:00:00Z, ACC-0001), lands file, but fails before checkpoint
        with pytest.raises(RuntimeError) as exc_info:
            pipeline.run_table_extraction(table_name="accounts", fail_before_commit=True)
        assert "Simulated failure after landing" in str(exc_info.value)

        # Verify Attempt 1 recorded FAILED audit with HIGH and batch_id
        history1 = control_store.get_table_audit_history("accounts")
        assert len(history1) == 1
        failed_audit = history1[0]
        assert failed_audit.status == WatermarkRunStatus.FAILED
        assert failed_audit.high_watermark.timestamp == "2026-01-01T10:00:00Z"
        assert failed_audit.high_watermark.key == "ACC-0001"
        assert failed_audit.batch_id is not None
        orig_batch_id = failed_audit.batch_id

        # Mutate source BEFORE retry by adding ACC-0002 at 11:00:00Z
        tables["accounts"].append(
            {
                "account_id": "ACC-0002",
                "account_name": "Co 2 (Newer)",
                "updated_at": "2026-01-01T11:00:00Z",
            }
        )

        # Attempt 2 (Retry): Must reuse prior HIGH (10:00:00Z, ACC-0001) and same batch_id
        res_retry = pipeline.run_table_extraction(table_name="accounts")
        assert res_retry.status == WatermarkRunStatus.SUCCESS
        assert res_retry.batch_id == orig_batch_id
        assert res_retry.high_watermark.timestamp == "2026-01-01T10:00:00Z"
        assert res_retry.high_watermark.key == "ACC-0001"
        assert res_retry.rows_extracted == 1
        assert res_retry.records[0]["account_id"] == "ACC-0001"

        # Checkpoint is now at 10:00:00Z
        state_after_retry = control_store.get_or_create_watermark_state("accounts")
        assert state_after_retry.version == 2
        assert state_after_retry.last_watermark.timestamp == "2026-01-01T10:00:00Z"

        # Attempt 3 (Next normal run): Extracts ACC-0002 and advances to 11:00:00Z
        res_next = pipeline.run_table_extraction(table_name="accounts")
        assert res_next.status == WatermarkRunStatus.SUCCESS
        assert res_next.rows_extracted == 1
        assert res_next.records[0]["account_id"] == "ACC-0002"
        assert res_next.high_watermark.timestamp == "2026-01-01T11:00:00Z"

        state_final = control_store.get_or_create_watermark_state("accounts")
        assert state_final.version == 3
        assert state_final.last_watermark.key == "ACC-0002"


def test_failure_before_landing_keeps_watermark_unchanged_and_recovers_window():
    """Verify that failure before landing succeeds leaves checkpoint unchanged, and retry

    recovers the exact same window and advances checkpoint.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        base_path = Path(tmpdir)
        db_path = base_path / "control.db"
        landing_dir = base_path / "landing"

        tables = {
            "accounts": [
                {
                    "account_id": "ACC-0001",
                    "account_name": "Co 1",
                    "updated_at": "2026-01-01T10:00:00Z",
                },
            ]
        }

        control_store = SQLiteWatermarkControlStore(db_path)
        source_adapter = InMemorySourceAdapter(source_tables=tables)
        pipeline = WatermarkPipeline(
            control_store=control_store,
            source_adapter=source_adapter,
            landing_base_dir=landing_dir,
        )

        # Attempt 1: Injected failure before landing
        with pytest.raises(RuntimeError) as exc_info:
            pipeline.run_table_extraction(table_name="accounts", fail_before_landing=True)
        assert "Simulated failure before landing" in str(exc_info.value)

        # Checkpoint remains unchanged
        state = control_store.get_or_create_watermark_state("accounts")
        assert state.version == 1
        assert state.last_watermark.is_initial

        # Mutate source before retry
        tables["accounts"].append(
            {
                "account_id": "ACC-0002",
                "account_name": "Co 2",
                "updated_at": "2026-01-01T12:00:00Z",
            }
        )

        # Attempt 2 (Retry): Reuses captured window for ACC-0001
        res_retry = pipeline.run_table_extraction(table_name="accounts")
        assert res_retry.status == WatermarkRunStatus.SUCCESS
        assert res_retry.rows_extracted == 1
        assert res_retry.records[0]["account_id"] == "ACC-0001"

        state_after = control_store.get_or_create_watermark_state("accounts")
        assert state_after.version == 2
        assert state_after.last_watermark.key == "ACC-0001"


def test_landing_row_count_verification_failure(monkeypatch):
    """Verify that if landing file verification detects a row count mismatch,

    WatermarkError is raised, the run is marked FAILED, and the checkpoint does NOT commit.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        base_path = Path(tmpdir)
        db_path = base_path / "control.db"
        landing_dir = base_path / "landing"

        tables = {
            "accounts": [
                {
                    "account_id": "ACC-0001",
                    "account_name": "Co 1",
                    "updated_at": "2026-01-01T10:00:00Z",
                },
                {
                    "account_id": "ACC-0002",
                    "account_name": "Co 2",
                    "updated_at": "2026-01-01T11:00:00Z",
                },
            ]
        }

        control_store = SQLiteWatermarkControlStore(db_path)
        source_adapter = InMemorySourceAdapter(source_tables=tables)
        pipeline = WatermarkPipeline(
            control_store=control_store,
            source_adapter=source_adapter,
            landing_base_dir=landing_dir,
        )

        # Mock read_watermark_batch_jsonl to simulate a corrupt/partial file returning only 1 row
        from src.watermark import pipeline as pipeline_mod

        monkeypatch.setattr(
            pipeline_mod,
            "read_watermark_batch_jsonl",
            lambda path: [{"account_id": "ACC-0001"}],
        )

        with pytest.raises(WatermarkError) as exc_info:
            pipeline.run_table_extraction(table_name="accounts")
        assert "Landing row count mismatch" in str(exc_info.value)

        # Watermark state MUST NOT advance
        state = control_store.get_or_create_watermark_state("accounts")
        assert state.version == 1
        assert state.last_watermark.is_initial

        # Audit must be FAILED
        history = control_store.get_table_audit_history("accounts")
        assert len(history) == 1
        assert history[0].status == WatermarkRunStatus.FAILED


def test_process_death_running_attempt_recovery():
    """Verify recovery when a prior process dies leaving an audit in RUNNING status."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_path = Path(tmpdir)
        db_path = base_path / "control.db"
        landing_dir = base_path / "landing"

        tables = {
            "accounts": [
                {
                    "account_id": "ACC-0001",
                    "account_name": "Co 1",
                    "updated_at": "2026-01-01T10:00:00Z",
                },
            ]
        }

        control_store = SQLiteWatermarkControlStore(db_path)
        source_adapter = InMemorySourceAdapter(source_tables=tables)
        pipeline = WatermarkPipeline(
            control_store=control_store,
            source_adapter=source_adapter,
            landing_base_dir=landing_dir,
        )

        # Manually create an abandoned RUNNING audit
        crashed_audit = WatermarkRunAudit(
            run_id="run_crashed_worker",
            table_name="accounts",
            expected_version=1,
            batch_id="batch_accounts_crashed",
            low_watermark=CompositeWatermark(None, None),
            high_watermark=CompositeWatermark("2026-01-01T10:00:00Z", "ACC-0001"),
            status=WatermarkRunStatus.RUNNING,
            started_at="2026-01-01T10:05:00Z",
        )
        control_store.start_run_audit(crashed_audit)

        # Recovery run takes ownership
        res = pipeline.run_table_extraction(table_name="accounts")
        assert res.status == WatermarkRunStatus.SUCCESS
        assert res.batch_id == "batch_accounts_crashed"

        # Checkpoint is committed
        state = control_store.get_or_create_watermark_state("accounts")
        assert state.version == 2
        assert state.last_watermark.key == "ACC-0001"

        # The crashed run is marked FAILED/superseded
        crashed_record = control_store.get_run_audit("run_crashed_worker")
        assert crashed_record is not None
        assert crashed_record.status == WatermarkRunStatus.FAILED
        assert "Superseded" in (crashed_record.error_message or "")
