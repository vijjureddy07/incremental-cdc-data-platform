"""Durable SQLite control store for downstream Change Data Feed consumer checkpoints."""

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from src.cdf.models import (
    CDFInvalidRangeError,
    CDFSourceAlreadyRegisteredError,
    CDFSourceNotFoundError,
    CDFSourceRegistration,
)


def _utc_now_iso() -> str:
    """Return current UTC time in ISO-8601 string format."""
    return datetime.now(UTC).isoformat()


class CDFStateStore:
    """Manages downstream Delta CDF registration and checkpoint state in SQLite."""

    def __init__(self, db_path: str | Path = "data/control/cdf_consumer.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def _get_connection(self) -> sqlite3.Connection:
        """Create a new SQLite connection configured with row factory."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self) -> None:
        """Create the cdf_consumer_state table if it does not already exist."""
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cdf_consumer_state (
                    source_table TEXT PRIMARY KEY,
                    source_path TEXT NOT NULL,
                    cdf_start_version INTEGER NOT NULL,
                    last_processed_version INTEGER NOT NULL,
                    registered_at TEXT NOT NULL,
                    last_updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def register_source(
        self,
        source_table: str,
        source_path: str | Path,
        cdf_start_version: int,
        if_exists: str = "ignore",
    ) -> CDFSourceRegistration:
        """Register a new CDF source table with its initial enabling version.

        Args:
            source_table: Canonical table name (e.g. 'accounts').
            source_path: Path to the current-state Delta table.
            cdf_start_version: Delta table version at which CDF was enabled.
            if_exists: 'ignore' to return existing registration, 'error' to raise.

        Returns:
            CDFSourceRegistration instance.
        """
        if cdf_start_version < 0:
            raise CDFInvalidRangeError(
                f"Invalid cdf_start_version {cdf_start_version}; must be non-negative."
            )

        str_path = str(Path(source_path).resolve())
        existing = self.get_source(source_table)
        if existing is not None:
            if if_exists == "ignore":
                return existing
            raise CDFSourceAlreadyRegisteredError(
                f"Source table '{source_table}' is already registered in CDF state store."
            )

        now = _utc_now_iso()
        initial_last_processed = cdf_start_version - 1

        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO cdf_consumer_state (
                    source_table, source_path, cdf_start_version,
                    last_processed_version, registered_at, last_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    source_table,
                    str_path,
                    cdf_start_version,
                    initial_last_processed,
                    now,
                    now,
                ),
            )
            conn.commit()

        return CDFSourceRegistration(
            source_table=source_table,
            source_path=str_path,
            cdf_start_version=cdf_start_version,
            last_processed_version=initial_last_processed,
            registered_at=now,
            last_updated_at=now,
        )

    def get_source(self, source_table: str) -> CDFSourceRegistration | None:
        """Retrieve registration state for a source table, or None if not registered."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT source_table, source_path, cdf_start_version,
                       last_processed_version, registered_at, last_updated_at
                FROM cdf_consumer_state
                WHERE source_table = ?
                """,
                (source_table,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return CDFSourceRegistration(
                source_table=row["source_table"],
                source_path=row["source_path"],
                cdf_start_version=row["cdf_start_version"],
                last_processed_version=row["last_processed_version"],
                registered_at=row["registered_at"],
                last_updated_at=row["last_updated_at"],
            )

    def list_sources(self) -> list[CDFSourceRegistration]:
        """List all registered CDF source tables."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT source_table, source_path, cdf_start_version,
                       last_processed_version, registered_at, last_updated_at
                FROM cdf_consumer_state
                ORDER BY source_table ASC
                """
            )
            return [
                CDFSourceRegistration(
                    source_table=row["source_table"],
                    source_path=row["source_path"],
                    cdf_start_version=row["cdf_start_version"],
                    last_processed_version=row["last_processed_version"],
                    registered_at=row["registered_at"],
                    last_updated_at=row["last_updated_at"],
                )
                for row in cursor.fetchall()
            ]

    def advance_checkpoint(
        self,
        source_table: str,
        new_processed_version: int,
    ) -> CDFSourceRegistration:
        """Advance the last processed version checkpoint for a source table.

        Args:
            source_table: Table name.
            new_processed_version: New highest processed commit version.

        Returns:
            Updated CDFSourceRegistration.
        """
        existing = self.get_source(source_table)
        if existing is None:
            raise CDFSourceNotFoundError(
                f"Cannot advance checkpoint: source table '{source_table}' is not registered."
            )

        if new_processed_version < existing.last_processed_version:
            raise CDFInvalidRangeError(
                f"Cannot move checkpoint backwards from version {existing.last_processed_version} "
                f"to {new_processed_version}."
            )

        now = _utc_now_iso()
        with self._get_connection() as conn:
            conn.execute(
                """
                UPDATE cdf_consumer_state
                SET last_processed_version = ?, last_updated_at = ?
                WHERE source_table = ?
                """,
                (new_processed_version, now, source_table),
            )
            conn.commit()

        return CDFSourceRegistration(
            source_table=existing.source_table,
            source_path=existing.source_path,
            cdf_start_version=existing.cdf_start_version,
            last_processed_version=new_processed_version,
            registered_at=existing.registered_at,
            last_updated_at=now,
        )
