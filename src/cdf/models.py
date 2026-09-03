"""Domain models, results, and exceptions for Delta Change Data Feed downstream consumers."""

from dataclasses import dataclass
from typing import Final

SUPPORTED_CHANGE_TYPES: Final[set[str]] = {
    "insert",
    "update_preimage",
    "update_postimage",
    "delete",
}

CDF_METADATA_COLUMNS: Final[list[str]] = [
    "_change_type",
    "_commit_version",
    "_commit_timestamp",
]


class CDFError(Exception):
    """Base exception for Delta Change Data Feed operations."""


class CDFSourceNotFoundError(CDFError):
    """Raised when a specified Delta table source does not exist."""


class CDFSourceAlreadyRegisteredError(CDFError):
    """Raised when attempting to re-register an already registered source with if_exists='error'."""


class CDFNotEnabledError(CDFError):
    """Raised when Change Data Feed is not enabled on a Delta table."""


class CDFInvalidRangeError(CDFError):
    """Raised when an invalid commit version range is requested."""


@dataclass(frozen=True)
class CDFSourceRegistration:
    """Downstream consumer registration state for a single Delta source table."""

    source_table: str
    source_path: str
    cdf_start_version: int
    last_processed_version: int
    registered_at: str
    last_updated_at: str


@dataclass(frozen=True)
class CDFConsumptionResult:
    """Execution summary and metrics for a downstream consumption run."""

    source_table: str
    start_version: int
    end_version: int
    input_change_rows: int
    archive_rows_inserted: int
    checkpoint_before: int
    checkpoint_after: int
    no_op: bool = False
