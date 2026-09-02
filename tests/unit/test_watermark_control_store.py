"""Unit tests for the durable SQLite watermark control store with explicit transaction management."""

import tempfile
from pathlib import Path

import pytest

from src.watermark.control_store import SQLiteWatermarkControlStore
from src.watermark.models import (
    CompositeWatermark,
    WatermarkCommitError,
    WatermarkConcurrencyError,
    WatermarkRunAudit,
    WatermarkRunStatus,
)


def test_control_store_initial_table_state():
    """Verify initial uncommitted table watermark creation."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "control.db"
        with SQLiteWatermarkControlStore(db_path) as store:
            state = store.get_or_create_watermark_state(
                table_name="accounts",
                watermark_column="updated_at",
                tie_breaker_column="account_id",
            )
            assert state.table_name == "accounts"
            assert state.watermark_column == "updated_at"
            assert state.tie_breaker_column == "account_id"
            assert state.last_watermark.is_initial
            assert state.version == 1
            assert state.last_success_run_id is None


def test_control_store_checkpoint_commit_advancement():
    """Verify successful watermark checkpoint commit advances version and timestamp."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "control.db"
        with SQLiteWatermarkControlStore(db_path) as store:
            init_state = store.get_or_create_watermark_state("accounts")
            assert init_state.version == 1

            new_wm = CompositeWatermark("2026-01-16T09:00:00Z", "ACC-0040")
            updated = store.commit_watermark_checkpoint(
                table_name="accounts",
                expected_version=1,
                new_watermark=new_wm,
                run_id="run_001",
            )

            assert updated.version == 2
            assert updated.last_watermark == new_wm
            assert updated.last_success_run_id == "run_001"


def test_control_store_optimistic_concurrency_conflict():
    """Verify that committing with a stale expected_version raises WatermarkConcurrencyError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "control.db"
        with SQLiteWatermarkControlStore(db_path) as store:
            # Process A reads version 1
            state_a = store.get_or_create_watermark_state("accounts")
            assert state_a.version == 1

            # Process B reads version 1
            state_b = store.get_or_create_watermark_state("accounts")
            assert state_b.version == 1

            # Process A commits -> advances state to version 2
            wm_a = CompositeWatermark("2026-01-16T09:00:00Z", "ACC-0040")
            store.commit_watermark_checkpoint(
                table_name="accounts",
                expected_version=state_a.version,
                new_watermark=wm_a,
                run_id="run_a",
            )

            # Process B tries to commit with stale version 1 -> Must raise WatermarkConcurrencyError
            wm_b = CompositeWatermark("2026-01-16T10:00:00Z", "ACC-0050")
            with pytest.raises(WatermarkConcurrencyError) as exc_info:
                store.commit_watermark_checkpoint(
                    table_name="accounts",
                    expected_version=state_b.version,  # Stale version 1
                    new_watermark=wm_b,
                    run_id="run_b",
                )

            assert "Optimistic concurrency conflict" in str(exc_info.value)

            # Confirm database remains at Process A's commit
            final_state = store.get_or_create_watermark_state("accounts")
            assert final_state.version == 2
            assert final_state.last_watermark == wm_a


def test_control_store_two_connection_concurrency_conflict():
    """Verify optimistic concurrency conflict between two separate SQLite connection instances."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "control.db"

        # Initialize table state
        with SQLiteWatermarkControlStore(db_path) as init_store:
            init_store.get_or_create_watermark_state("accounts")

        # Connection 1 and Connection 2 open concurrently
        store1 = SQLiteWatermarkControlStore(db_path)
        store2 = SQLiteWatermarkControlStore(db_path)

        state1 = store1.get_or_create_watermark_state("accounts")
        state2 = store2.get_or_create_watermark_state("accounts")
        assert state1.version == 1
        assert state2.version == 1

        # Connection 1 commits version 1 -> 2
        wm1 = CompositeWatermark("2026-01-16T09:00:00Z", "ACC-0040")
        store1.commit_watermark_checkpoint("accounts", state1.version, wm1, "run_c1")

        # Connection 2 attempts to commit with stale version 1 -> raises WatermarkConcurrencyError
        wm2 = CompositeWatermark("2026-01-16T10:00:00Z", "ACC-0050")
        with pytest.raises(WatermarkConcurrencyError):
            store2.commit_watermark_checkpoint("accounts", state2.version, wm2, "run_c2")

        # Connection 1 commit is preserved
        store1.close()
        store2.close()

        with SQLiteWatermarkControlStore(db_path) as check_store:
            final_state = check_store.get_or_create_watermark_state("accounts")
            assert final_state.version == 2
            assert final_state.last_watermark == wm1


def test_control_store_atomic_success_commit():
    """Verify commit_successful_run atomically updates watermark state and marks audit SUCCESS."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "control.db"
        with SQLiteWatermarkControlStore(db_path) as store:
            store.get_or_create_watermark_state("accounts")
            audit = WatermarkRunAudit(
                run_id="run_atomic_01",
                table_name="accounts",
                expected_version=1,
                batch_id="batch_acc_01",
                low_watermark=CompositeWatermark(None, None),
                high_watermark=CompositeWatermark("2026-01-16T09:00:00Z", "ACC-0040"),
                status=WatermarkRunStatus.RUNNING,
                started_at="2026-01-16T09:01:00Z",
            )
            store.start_run_audit(audit)

            # Atomic commit
            state = store.commit_successful_run(
                table_name="accounts",
                expected_version=1,
                new_watermark=CompositeWatermark("2026-01-16T09:00:00Z", "ACC-0040"),
                run_id="run_atomic_01",
                rows_extracted=40,
                landing_path="/data/watermark_landing/test.jsonl",
            )

            assert state.version == 2
            assert state.last_watermark.key == "ACC-0040"

            saved_audit = store.get_run_audit("run_atomic_01")
            assert saved_audit is not None
            assert saved_audit.status == WatermarkRunStatus.SUCCESS
            assert saved_audit.rows_extracted == 40
            assert saved_audit.completed_at is not None


def test_control_store_atomic_success_rollback():
    """Verify that if run audit update fails during atomic commit, the watermark update is rolled back."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "control.db"
        with SQLiteWatermarkControlStore(db_path) as store:
            store.get_or_create_watermark_state("accounts")

            # Try to commit with a run_id that does NOT exist in watermark_run_audit
            with pytest.raises(WatermarkCommitError):
                store.commit_successful_run(
                    table_name="accounts",
                    expected_version=1,
                    new_watermark=CompositeWatermark("2026-01-16T09:00:00Z", "ACC-0040"),
                    run_id="non_existent_run_id",
                    rows_extracted=10,
                    landing_path="/fake/path.jsonl",
                )

            # Watermark state MUST remain unchanged (version 1, initial watermark)
            state = store.get_or_create_watermark_state("accounts")
            assert state.version == 1
            assert state.last_watermark.is_initial


def test_control_store_get_recoverable_window():
    """Verify recoverable window lookup finds eligible failed/running attempts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "control.db"
        with SQLiteWatermarkControlStore(db_path) as store:
            store.get_or_create_watermark_state("accounts")

            # Create a failed attempt with valid window boundaries
            audit = WatermarkRunAudit(
                run_id="run_failed_01",
                table_name="accounts",
                expected_version=1,
                batch_id="batch_acc_fail",
                low_watermark=CompositeWatermark(None, None),
                high_watermark=CompositeWatermark("2026-01-16T09:00:00Z", "ACC-0040"),
                status=WatermarkRunStatus.FAILED,
                started_at="2026-01-16T09:01:00Z",
                error_message="Connection timeout",
            )
            store.start_run_audit(audit)

            # Lookup recoverable window
            rec = store.get_recoverable_window("accounts")
            assert rec is not None
            assert rec.run_id == "run_failed_01"
            assert rec.batch_id == "batch_acc_fail"
            assert rec.high_watermark.key == "ACC-0040"

            # If watermark advances (e.g. by another run to version 2), failed attempt is no longer recoverable
            store.commit_watermark_checkpoint(
                "accounts", 1, CompositeWatermark("2026-01-16T09:00:00Z", "ACC-0040"), "run_succ"
            )
            assert store.get_recoverable_window("accounts") is None


def test_control_store_table_independence():
    """Verify that committing a watermark for accounts does not affect subscriptions."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "control.db"
        with SQLiteWatermarkControlStore(db_path) as store:
            store.get_or_create_watermark_state("accounts")
            store.get_or_create_watermark_state("subscriptions")

            # Advance accounts
            wm_acc = CompositeWatermark("2026-01-16T09:00:00Z", "ACC-0040")
            store.commit_watermark_checkpoint("accounts", 1, wm_acc, "run_acc")

            acc_state = store.get_or_create_watermark_state("accounts")
            sub_state = store.get_or_create_watermark_state("subscriptions")

            assert acc_state.version == 2
            assert acc_state.last_watermark == wm_acc
            assert sub_state.version == 1
            assert sub_state.last_watermark.is_initial


def test_control_store_restart_persistence():
    """Verify closing database and opening a new instance retains committed state."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "control.db"

        # Session 1: write checkpoint
        store1 = SQLiteWatermarkControlStore(db_path)
        store1.get_or_create_watermark_state("invoices")
        wm = CompositeWatermark("2026-04-01T12:00:00Z", "INV-0120")
        store1.commit_watermark_checkpoint("invoices", 1, wm, "run_init")
        store1.close()

        # Session 2: reopen against same SQLite file
        store2 = SQLiteWatermarkControlStore(db_path)
        reopened_state = store2.get_or_create_watermark_state("invoices")
        assert reopened_state.version == 2
        assert reopened_state.last_watermark == wm
        assert reopened_state.last_success_run_id == "run_init"
        store2.close()


def test_control_store_run_auditing_lifecycle():
    """Verify full audit logging lifecycle: start, complete, and history lookup."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "control.db"
        with SQLiteWatermarkControlStore(db_path) as store:
            audit = WatermarkRunAudit(
                run_id="run_pay_100",
                table_name="payments",
                expected_version=1,
                batch_id="batch_pay_abc",
                low_watermark=CompositeWatermark(None, None),
                high_watermark=CompositeWatermark("2026-04-01T15:00:00Z", "PAY-0090"),
                status=WatermarkRunStatus.RUNNING,
                started_at="2026-04-01T15:01:00Z",
            )
            store.start_run_audit(audit)

            # Inspect running state
            rec = store.get_run_audit("run_pay_100")
            assert rec is not None
            assert rec.status == WatermarkRunStatus.RUNNING
            assert rec.rows_extracted == 0

            # Complete run
            store.complete_run_audit(
                run_id="run_pay_100",
                status=WatermarkRunStatus.SUCCESS,
                rows_extracted=90,
                landing_path="/data/watermark_landing/table=payments/batch_pay_abc/data.jsonl",
            )

            done_rec = store.get_run_audit("run_pay_100")
            assert done_rec is not None
            assert done_rec.status == WatermarkRunStatus.SUCCESS
            assert done_rec.rows_extracted == 90
            assert done_rec.completed_at is not None

            # Retrieve history
            history = store.get_table_audit_history("payments")
            assert len(history) == 1
            assert history[0].run_id == "run_pay_100"
