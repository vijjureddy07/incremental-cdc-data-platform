"""Durable SQLite control store for watermark state and execution run auditing."""

import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.utils.helpers import format_iso_timestamp
from src.watermark.models import (
    CompositeWatermark,
    WatermarkCommitError,
    WatermarkConcurrencyError,
    WatermarkRunAudit,
    WatermarkRunStatus,
    WatermarkState,
)


class SQLiteWatermarkControlStore:
    """Manages persistent watermark state and run audit history in local SQLite using explicit SQL transactions."""

    def __init__(self, db_path: str | Path = "data/control_store.db") -> None:
        self.db_path = Path(db_path)
        if self.db_path.parent and not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,
            isolation_level=None,  # Manual transaction management with explicit BEGIN/COMMIT/ROLLBACK
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._tx_depth = 0
        self._init_schema()

    @contextmanager
    def _transaction(self) -> Generator[None, None, None]:
        """Context manager managing explicit SQL transaction boundaries via BEGIN IMMEDIATE."""
        is_outer = self._tx_depth == 0
        if is_outer:
            self._conn.execute("BEGIN IMMEDIATE")
        self._tx_depth += 1
        try:
            yield
            if is_outer:
                self._conn.execute("COMMIT")
        except Exception:
            if is_outer:
                self._conn.execute("ROLLBACK")
            raise
        finally:
            self._tx_depth -= 1

    def _init_schema(self) -> None:
        """Initialize database control tables and indexes if not already present."""
        with self._transaction():
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS watermark_state (
                    table_name TEXT PRIMARY KEY,
                    watermark_column TEXT NOT NULL,
                    tie_breaker_column TEXT NOT NULL,
                    last_watermark_timestamp TEXT,
                    last_watermark_key TEXT,
                    version INTEGER NOT NULL DEFAULT 1,
                    last_success_run_id TEXT,
                    updated_at TEXT NOT NULL
                );
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS watermark_run_audit (
                    run_id TEXT PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    expected_version INTEGER,
                    batch_id TEXT,
                    low_watermark_timestamp TEXT,
                    low_watermark_key TEXT,
                    high_watermark_timestamp TEXT,
                    high_watermark_key TEXT,
                    status TEXT NOT NULL,
                    rows_extracted INTEGER NOT NULL DEFAULT 0,
                    landing_path TEXT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    error_message TEXT
                );
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_audit_table_started
                ON watermark_run_audit (table_name, started_at);
                """
            )

    def get_or_create_watermark_state(
        self,
        table_name: str,
        watermark_column: str = "updated_at",
        tie_breaker_column: str = "id",
    ) -> WatermarkState:
        """Retrieve current persisted watermark state, initializing an uncommitted entry if needed in a race-safe manner."""
        now_str = format_iso_timestamp(datetime.now(UTC))

        with self._transaction():
            # Race-safe insertion: insert if not present
            self._conn.execute(
                """
                INSERT OR IGNORE INTO watermark_state (
                    table_name, watermark_column, tie_breaker_column,
                    last_watermark_timestamp, last_watermark_key,
                    version, last_success_run_id, updated_at
                ) VALUES (?, ?, ?, NULL, NULL, 1, NULL, ?)
                """,
                (table_name, watermark_column, tie_breaker_column, now_str),
            )

            cursor = self._conn.execute(
                "SELECT * FROM watermark_state WHERE table_name = ?",
                (table_name,),
            )
            row = cursor.fetchone()
            if row is None:
                raise WatermarkCommitError(
                    f"Failed to retrieve or initialize watermark state for table '{table_name}'."
                )
            return self._row_to_watermark_state(row)

    def start_run_audit(self, run_audit: WatermarkRunAudit) -> None:
        """Record the initial creation of a watermark extraction run."""
        try:
            with self._transaction():
                self._conn.execute(
                    """
                    INSERT INTO watermark_run_audit (
                        run_id, table_name, expected_version, batch_id,
                        low_watermark_timestamp, low_watermark_key,
                        high_watermark_timestamp, high_watermark_key,
                        status, rows_extracted, landing_path,
                        started_at, completed_at, error_message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_audit.run_id,
                        run_audit.table_name,
                        run_audit.expected_version,
                        run_audit.batch_id,
                        run_audit.low_watermark.timestamp,
                        run_audit.low_watermark.key,
                        run_audit.high_watermark.timestamp,
                        run_audit.high_watermark.key,
                        run_audit.status.value,
                        run_audit.rows_extracted,
                        run_audit.landing_path,
                        run_audit.started_at,
                        run_audit.completed_at,
                        run_audit.error_message,
                    ),
                )
        except Exception as e:
            raise WatermarkCommitError(f"Failed to record start of run audit: {e}") from e

    def update_run_audit_window(
        self,
        run_id: str,
        expected_version: int,
        low_watermark: CompositeWatermark,
        high_watermark: CompositeWatermark,
        batch_id: str,
    ) -> None:
        """Update audit record with resolved extraction window boundaries and deterministic batch_id."""
        try:
            with self._transaction():
                self._conn.execute(
                    """
                    UPDATE watermark_run_audit
                    SET expected_version = ?,
                        low_watermark_timestamp = ?,
                        low_watermark_key = ?,
                        high_watermark_timestamp = ?,
                        high_watermark_key = ?,
                        batch_id = ?
                    WHERE run_id = ?
                    """,
                    (
                        expected_version,
                        low_watermark.timestamp,
                        low_watermark.key,
                        high_watermark.timestamp,
                        high_watermark.key,
                        batch_id,
                        run_id,
                    ),
                )
        except Exception as e:
            raise WatermarkCommitError(f"Failed to update run audit window: {e}") from e

    def get_recoverable_window(self, table_name: str) -> WatermarkRunAudit | None:
        """Find the most recent uncommitted or failed extraction window that is eligible for deterministic recovery.

        Eligibility Criteria:
        1. Attempt status is FAILED or RUNNING (uncompleted/crashed process)
        2. Attempt has a valid captured HIGH watermark
        3. Attempt expected_version equals the current table version
        4. Attempt LOW watermark equals the current committed table watermark
        5. Attempt HIGH watermark > LOW watermark
        """
        current_state = self.get_or_create_watermark_state(table_name)

        cursor = self._conn.execute(
            """
            SELECT * FROM watermark_run_audit
            WHERE table_name = ?
              AND status IN ('FAILED', 'RUNNING')
              AND expected_version IS NOT NULL
              AND high_watermark_timestamp IS NOT NULL
            ORDER BY started_at DESC, rowid DESC
            LIMIT 1
            """,
            (table_name,),
        )
        row = cursor.fetchone()
        if row is None:
            return None

        candidate = self._row_to_run_audit(row)

        # Verify eligibility against current persisted state
        if (
            candidate.expected_version == current_state.version
            and candidate.low_watermark == current_state.last_watermark
            and candidate.high_watermark > candidate.low_watermark
            and candidate.batch_id is not None
        ):
            return candidate

        return None

    def mark_superseded(self, run_id: str, new_run_id: str) -> None:
        """Mark an abandoned RUNNING audit record as FAILED (superseded by recovery run)."""
        now_str = format_iso_timestamp(datetime.now(UTC))
        try:
            with self._transaction():
                self._conn.execute(
                    """
                    UPDATE watermark_run_audit
                    SET status = ?,
                        completed_at = ?,
                        error_message = ?
                    WHERE run_id = ? AND status = ?
                    """,
                    (
                        WatermarkRunStatus.FAILED.value,
                        now_str,
                        f"Superseded by recovery run {new_run_id}",
                        run_id,
                        WatermarkRunStatus.RUNNING.value,
                    ),
                )
        except Exception as e:
            raise WatermarkCommitError(f"Failed to mark run {run_id} as superseded: {e}") from e

    def commit_successful_run(
        self,
        table_name: str,
        expected_version: int,
        new_watermark: CompositeWatermark,
        run_id: str,
        rows_extracted: int,
        landing_path: str,
        completed_at: str | None = None,
    ) -> WatermarkState:
        """Atomically commit watermark checkpoint advancement AND mark run audit SUCCESS inside ONE SQL transaction.

        If either compare-and-swap or audit update fails, both operations are rolled back completely.
        """
        now_str = format_iso_timestamp(datetime.now(UTC))
        done_time = completed_at or now_str

        with self._transaction():
            # Step 1: Compare-and-swap watermark update
            cursor = self._conn.execute(
                """
                UPDATE watermark_state
                SET last_watermark_timestamp = ?,
                    last_watermark_key = ?,
                    version = version + 1,
                    last_success_run_id = ?,
                    updated_at = ?
                WHERE table_name = ? AND version = ?
                """,
                (
                    new_watermark.timestamp,
                    new_watermark.key,
                    run_id,
                    now_str,
                    table_name,
                    expected_version,
                ),
            )

            if cursor.rowcount == 0:
                check_cursor = self._conn.execute(
                    "SELECT version FROM watermark_state WHERE table_name = ?",
                    (table_name,),
                )
                actual_row = check_cursor.fetchone()
                actual_version = actual_row["version"] if actual_row else "NOT_FOUND"

                raise WatermarkConcurrencyError(
                    f"Optimistic concurrency conflict for table '{table_name}': "
                    f"expected version {expected_version}, but found version {actual_version}."
                )

            # Step 2: Mark run audit as SUCCESS
            audit_cursor = self._conn.execute(
                """
                UPDATE watermark_run_audit
                SET status = ?,
                    rows_extracted = ?,
                    landing_path = ?,
                    completed_at = ?,
                    error_message = NULL
                WHERE run_id = ?
                """,
                (
                    WatermarkRunStatus.SUCCESS.value,
                    rows_extracted,
                    landing_path,
                    done_time,
                    run_id,
                ),
            )

            if audit_cursor.rowcount == 0:
                raise WatermarkCommitError(
                    f"Failed to update run audit for run {run_id} during atomic commit."
                )

            # Step 3: Fetch updated state
            state_cursor = self._conn.execute(
                "SELECT * FROM watermark_state WHERE table_name = ?",
                (table_name,),
            )
            updated_row = state_cursor.fetchone()
            if updated_row is None:
                raise WatermarkCommitError(
                    f"Failed to fetch updated state for table '{table_name}' after commit."
                )
            return self._row_to_watermark_state(updated_row)

    def commit_watermark_checkpoint(
        self,
        table_name: str,
        expected_version: int,
        new_watermark: CompositeWatermark,
        run_id: str,
    ) -> WatermarkState:
        """Commit an advanced watermark checkpoint using compare-and-swap optimistic concurrency."""
        now_str = format_iso_timestamp(datetime.now(UTC))

        with self._transaction():
            cursor = self._conn.execute(
                """
                UPDATE watermark_state
                SET last_watermark_timestamp = ?,
                    last_watermark_key = ?,
                    version = version + 1,
                    last_success_run_id = ?,
                    updated_at = ?
                WHERE table_name = ? AND version = ?
                """,
                (
                    new_watermark.timestamp,
                    new_watermark.key,
                    run_id,
                    now_str,
                    table_name,
                    expected_version,
                ),
            )

            if cursor.rowcount == 0:
                check_cursor = self._conn.execute(
                    "SELECT version FROM watermark_state WHERE table_name = ?",
                    (table_name,),
                )
                actual_row = check_cursor.fetchone()
                actual_version = actual_row["version"] if actual_row else "NOT_FOUND"

                raise WatermarkConcurrencyError(
                    f"Optimistic concurrency conflict for table '{table_name}': "
                    f"expected version {expected_version}, but found version {actual_version}."
                )

            state_cursor = self._conn.execute(
                "SELECT * FROM watermark_state WHERE table_name = ?",
                (table_name,),
            )
            updated_row = state_cursor.fetchone()
            if updated_row is None:
                raise WatermarkCommitError(
                    f"Failed to fetch updated state for table '{table_name}' after commit."
                )
            return self._row_to_watermark_state(updated_row)

    def complete_run_audit(
        self,
        run_id: str,
        status: WatermarkRunStatus,
        rows_extracted: int,
        landing_path: str | None = None,
        error_message: str | None = None,
        completed_at: str | None = None,
    ) -> None:
        """Update audit log entry with final status and metrics."""
        done_time = completed_at or format_iso_timestamp(datetime.now(UTC))

        try:
            with self._transaction():
                self._conn.execute(
                    """
                    UPDATE watermark_run_audit
                    SET status = ?,
                        rows_extracted = ?,
                        landing_path = COALESCE(?, landing_path),
                        completed_at = ?,
                        error_message = ?
                    WHERE run_id = ?
                    """,
                    (
                        status.value,
                        rows_extracted,
                        landing_path,
                        done_time,
                        error_message,
                        run_id,
                    ),
                )
        except Exception as e:
            raise WatermarkCommitError(f"Failed to update run audit for run {run_id}: {e}") from e

    def get_run_audit(self, run_id: str) -> WatermarkRunAudit | None:
        """Retrieve a specific run audit record by run_id."""
        cursor = self._conn.execute(
            "SELECT * FROM watermark_run_audit WHERE run_id = ?",
            (run_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return self._row_to_run_audit(row)

    def get_table_audit_history(self, table_name: str) -> list[WatermarkRunAudit]:
        """Retrieve full audit history for a table ordered by started_at DESC."""
        cursor = self._conn.execute(
            """
            SELECT * FROM watermark_run_audit
            WHERE table_name = ?
            ORDER BY started_at DESC, rowid DESC
            """,
            (table_name,),
        )
        return [self._row_to_run_audit(r) for r in cursor.fetchall()]

    def close(self) -> None:
        """Close SQLite database connection."""
        if self._conn:
            self._conn.close()

    def __enter__(self) -> "SQLiteWatermarkControlStore":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    @staticmethod
    def _row_to_watermark_state(row: sqlite3.Row) -> WatermarkState:
        return WatermarkState(
            table_name=row["table_name"],
            watermark_column=row["watermark_column"],
            tie_breaker_column=row["tie_breaker_column"],
            last_watermark=CompositeWatermark(
                timestamp=row["last_watermark_timestamp"],
                key=row["last_watermark_key"],
            ),
            version=row["version"],
            last_success_run_id=row["last_success_run_id"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_run_audit(row: sqlite3.Row) -> WatermarkRunAudit:
        return WatermarkRunAudit(
            run_id=row["run_id"],
            table_name=row["table_name"],
            expected_version=row["expected_version"],
            batch_id=row["batch_id"],
            low_watermark=CompositeWatermark(
                timestamp=row["low_watermark_timestamp"],
                key=row["low_watermark_key"],
            ),
            high_watermark=CompositeWatermark(
                timestamp=row["high_watermark_timestamp"],
                key=row["high_watermark_key"],
            ),
            status=WatermarkRunStatus(row["status"]),
            rows_extracted=row["rows_extracted"],
            landing_path=row["landing_path"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            error_message=row["error_message"],
        )
