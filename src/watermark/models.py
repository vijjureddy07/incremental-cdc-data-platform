"""Domain models, composite cursors, and exceptions for watermark incremental ingestion."""

from dataclasses import dataclass, field
from enum import StrEnum
from functools import total_ordering
from typing import Any


class WatermarkError(Exception):
    """Base exception for all watermark operations."""


class WatermarkConcurrencyError(WatermarkError):
    """Raised when an optimistic concurrency version check fails during watermark commit."""


class WatermarkExtractionError(WatermarkError):
    """Raised when an error occurs during source data extraction."""


class WatermarkCommitError(WatermarkError):
    """Raised when an error occurs while persisting watermark checkpoint metadata."""


class WatermarkRunStatus(StrEnum):
    """Status lifecycle for a watermark extraction execution attempt."""

    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    NO_DATA = "NO_DATA"
    FAILED = "FAILED"


@total_ordering
@dataclass(frozen=True)
class CompositeWatermark:
    """Composite cursor representing (timestamp, primary_key).

    Ordering is strictly defined as:
    1. timestamp (None is treated as negative infinity)
    2. primary_key tie-breaker (None is treated as negative infinity)
    """

    timestamp: str | None = None
    key: str | None = None

    @property
    def is_initial(self) -> bool:
        """Return True if this watermark represents the uninitialized / full-load state."""
        return self.timestamp is None and self.key is None

    def _as_tuple(self) -> tuple[int, str, str]:
        """Convert to comparable tuple with None handled as minimum value."""
        # Flag 0 for None (infinitesimal), 1 for present
        if self.timestamp is None:
            return (0, "", "")
        return (1, self.timestamp, self.key or "")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CompositeWatermark):
            return NotImplemented
        return self._as_tuple() == other._as_tuple()

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, CompositeWatermark):
            return NotImplemented
        return self._as_tuple() < other._as_tuple()

    def to_dict(self) -> dict[str, str | None]:
        """Serialize composite watermark to dictionary."""
        return {
            "timestamp": self.timestamp,
            "key": self.key,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "CompositeWatermark":
        """Deserialize composite watermark from dictionary."""
        if not data:
            return cls(timestamp=None, key=None)
        return cls(
            timestamp=data.get("timestamp"),
            key=data.get("key"),
        )

    def __str__(self) -> str:
        if self.is_initial:
            return "CompositeWatermark(INITIAL)"
        return f"CompositeWatermark(timestamp={self.timestamp}, key={self.key})"


@dataclass
class WatermarkState:
    """Persisted watermark state for a source table."""

    table_name: str
    watermark_column: str = "updated_at"
    tie_breaker_column: str = "id"
    last_watermark: CompositeWatermark = field(default_factory=CompositeWatermark)
    version: int = 1
    last_success_run_id: str | None = None
    updated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize watermark state to dictionary."""
        return {
            "table_name": self.table_name,
            "watermark_column": self.watermark_column,
            "tie_breaker_column": self.tie_breaker_column,
            "last_watermark_timestamp": self.last_watermark.timestamp,
            "last_watermark_key": self.last_watermark.key,
            "version": self.version,
            "last_success_run_id": self.last_success_run_id,
            "updated_at": self.updated_at,
        }


@dataclass
class WatermarkRunAudit:
    """Audit log entry for a watermark extraction execution attempt."""

    run_id: str
    table_name: str
    batch_id: str
    low_watermark: CompositeWatermark
    high_watermark: CompositeWatermark
    status: WatermarkRunStatus
    rows_extracted: int = 0
    landing_path: str | None = None
    started_at: str = ""
    completed_at: str | None = None
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize audit log to dictionary."""
        return {
            "run_id": self.run_id,
            "table_name": self.table_name,
            "batch_id": self.batch_id,
            "low_watermark_timestamp": self.low_watermark.timestamp,
            "low_watermark_key": self.low_watermark.key,
            "high_watermark_timestamp": self.high_watermark.timestamp,
            "high_watermark_key": self.high_watermark.key,
            "status": self.status.value,
            "rows_extracted": self.rows_extracted,
            "landing_path": self.landing_path,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error_message": self.error_message,
        }


@dataclass
class ExtractionResult:
    """Result of a watermark extraction operation."""

    table_name: str
    run_id: str
    batch_id: str
    status: WatermarkRunStatus
    low_watermark: CompositeWatermark
    high_watermark: CompositeWatermark
    rows_extracted: int
    landing_path: str | None
    records: list[dict[str, Any]] = field(default_factory=list)
