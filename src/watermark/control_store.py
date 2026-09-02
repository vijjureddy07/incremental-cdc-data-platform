"""Durable SQLite control store for watermark state and execution run auditing."""

import sqlite3
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
    """Manages persistent watermark state and run audit history in local SQLite."""

    def __init__(self, db_path: str | Path = "data/control_store.db") -> None:
        self.db_path = Path(db_path)
        if self.db_path.parent and not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,
            isolation_level=None,  # Autocommit mode; explicit transactions used
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize database control tables and indexes if not already present."""
        with self._conn:
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
                    batch_id TEXT NOT NULL,
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
        """Retrieve current persisted watermark state, initializing an uncommitted entry if needed."""
        now_str = format_iso_timestamp(datetime.now(UTC))

        with self._conn:
            cursor = self._conn.execute(
                "SELECT * FROM watermark_state WHERE table_name = ?",
                (table_name,),
            )
            row = cursor.fetchone()

            if row is not None:
                return self._row_to_watermark_state(row)

            # Insert initial state (version=1, None cursors)
            self._conn.execute(
                """
                INSERT INTO watermark_state (
                    table_name, watermark_column, tie_breaker_column,
                    last_watermark_timestamp, last_watermark_key,
                    version, last_success_run_id, updated_at
                ) VALUES (?, ?, ?, NULL, NULL, 1, NULL, ?)
                """,
                (table_name, watermark_column, tie_breaker_column, now_str),
            )

            return WatermarkState(
                table_name=table_name,
                watermark_column=watermark_column,
                tie_breaker_column=tie_breaker_column,
                last_watermark=CompositeWatermark(None, None),
                version=1,
                last_success_run_id=None,
                updated_at=now_str,
            )

    def commit_watermark_checkpoint(
        self,
        table_name: str,
        expected_version: int,
        new_watermark: CompositeWatermark,
        run_id: str,
    ) -> WatermarkState:
        """Commit an advanced watermark checkpoint using compare-and-swap optimistic concurrency."""
        now_str = format_iso_timestamp(datetime.now(UTC))

        with self._conn:
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
                # Fetch actual version to provide a detailed concurrency error
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

            # Return updated state
            return self.get_or_create_watermark_state(table_name)

    def start_run_audit(self, run_audit: WatermarkRunAudit) -> None:
        """Record the start of a watermark extraction run in RUNNING status."""
        try:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO watermark_run_audit (
                        run_id, table_name, batch_id,
                        low_watermark_timestamp, low_watermark_key,
                        high_watermark_timestamp, high_watermark_key,
                        status, rows_extracted, landing_path,
                        started_at, completed_at, error_message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_audit.run_id,
                        run_audit.table_name,
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
            with self._conn:
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
        with self._conn:
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
        with self._conn:
            cursor = self._conn.execute(
                """
                SELECT * FROM watermark_run_audit
                WHERE table_name = ?
                ORDER BY started_at DESC
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
