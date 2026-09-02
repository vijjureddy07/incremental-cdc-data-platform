"""Source adapter for querying in-memory source snapshots via bounded composite watermarks."""

from copy import deepcopy
from typing import Any

from src.source.schemas import TABLE_PRIMARY_KEYS
from src.watermark.models import (
    CompositeWatermark,
    WatermarkExtractionError,
)


class InMemorySourceAdapter:
    """Extracts bounded incremental datasets from in-memory source tables."""

    def __init__(
        self,
        source_tables: dict[str, list[dict[str, Any]]],
        primary_keys: dict[str, str] | None = None,
    ) -> None:
        self._tables = source_tables
        self._primary_keys = primary_keys or TABLE_PRIMARY_KEYS

    def get_primary_key_column(self, table_name: str) -> str:
        """Get the primary key column name for the given table."""
        if table_name in self._primary_keys:
            return self._primary_keys[table_name]
        return "id"

    def get_current_source_records(self, table_name: str) -> list[dict[str, Any]]:
        """Retrieve all current raw records for a table."""
        if table_name not in self._tables:
            raise WatermarkExtractionError(f"Table '{table_name}' does not exist in source.")
        return self._tables[table_name]

    def capture_source_high_watermark(
        self,
        table_name: str,
        watermark_column: str = "updated_at",
    ) -> CompositeWatermark:
        """Capture the current source maximum composite watermark (updated_at, PK)."""
        records = self.get_current_source_records(table_name)
        if not records:
            return CompositeWatermark(None, None)

        pk_col = self.get_primary_key_column(table_name)
        max_cursor = CompositeWatermark(None, None)

        for r in records:
            ts = r.get(watermark_column)
            key = str(r.get(pk_col)) if r.get(pk_col) is not None else None
            cursor = CompositeWatermark(timestamp=ts, key=key)
            if cursor > max_cursor:
                max_cursor = cursor

        return max_cursor

    def extract_bounded_window(
        self,
        table_name: str,
        low_watermark: CompositeWatermark,
        high_watermark: CompositeWatermark,
        watermark_column: str = "updated_at",
    ) -> list[dict[str, Any]]:
        """Extract all source rows satisfying: LOW < (updated_at, PK) <= HIGH.

        Ordering:
        Deterministically sorted by updated_at ASC, primary_key ASC.
        """
        records = self.get_current_source_records(table_name)
        pk_col = self.get_primary_key_column(table_name)
        extracted: list[dict[str, Any]] = []

        for r in records:
            ts = r.get(watermark_column)
            key = str(r.get(pk_col)) if r.get(pk_col) is not None else None
            cursor = CompositeWatermark(timestamp=ts, key=key)

            # Predicate: LOW < cursor <= HIGH
            if low_watermark < cursor <= high_watermark:
                extracted.append(deepcopy(r))

        # Deterministic sort: updated_at ASC, primary_key ASC
        extracted.sort(key=lambda item: (str(item.get(watermark_column)), str(item.get(pk_col))))
        return extracted
