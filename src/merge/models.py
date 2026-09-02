"""Domain models, enums, exception types, and schemas for Delta Lake MERGE and replay recovery."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)


class DeletePolicy(StrEnum):
    """Delete propagation policy."""

    HARD = "HARD"
    SOFT = "SOFT"


class LedgerStatus(StrEnum):
    """State of an event in the application ledger."""

    PENDING = "PENDING"
    APPLIED = "APPLIED"


class EventClassification(StrEnum):
    """Classification of incoming CDC event against ledger state."""

    FRESH = "FRESH"
    RECOVERY_PENDING = "RECOVERY_PENDING"
    EXACT_REPLAY_APPLIED = "EXACT_REPLAY_APPLIED"
    STALE_SKIPPED = "STALE_SKIPPED"


class MergeError(Exception):
    """Base exception for Delta MERGE and recovery errors."""

    pass


class PendingRecoveryError(MergeError):
    """Raised when unrelated new processing is attempted while unresolved PENDING events exist in the ledger."""

    pass


class AppliedEventConflictError(MergeError):
    """Raised when an incoming event_id already exists in ledger with a different fingerprint."""

    pass


class AppliedSequenceConflictError(MergeError):
    """Raised when an incoming event has sequence_number == max_applied_sequence for that entity but a different fingerprint."""

    pass


class MergeAmbiguityError(MergeError):
    """Raised when multiple events in the same wave target the same primary key in Delta MERGE."""

    pass


class TargetAlreadyInitializedError(MergeError):
    """Raised when attempting to initialize already initialized Delta target tables without overwrite=True."""

    pass


# Delta Current-State Metadata Column Definitions
TARGET_METADATA_FIELDS = [
    StructField("_last_sequence_number", LongType(), False),
    StructField("_last_event_id", StringType(), False),
    StructField("_last_operation", StringType(), False),
    StructField("_last_event_fingerprint", StringType(), False),
    StructField("_last_source_commit_timestamp", StringType(), False),
    StructField("_last_processing_id", StringType(), False),
    StructField("_is_deleted", BooleanType(), False),
    StructField("_deleted_at", StringType(), True),
]

# Event Application Ledger Schema
EVENT_LEDGER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_fingerprint", StringType(), False),
        StructField("entity_sequence_key", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("business_key_canonical", StringType(), False),
        StructField("sequence_number", LongType(), False),
        StructField("operation", StringType(), False),
        StructField("source_commit_timestamp", StringType(), False),
        StructField("processing_id", StringType(), False),
        StructField("source_file", StringType(), False),
        StructField("status", StringType(), False),  # PENDING or APPLIED
    ]
)


@dataclass(frozen=True)
class EventLedgerRecord:
    """Represents a row in the event application ledger."""

    event_id: str
    event_fingerprint: str
    entity_sequence_key: str
    table_name: str
    business_key_canonical: str
    sequence_number: int
    operation: str
    source_commit_timestamp: str
    processing_id: str
    source_file: str
    status: str

    def to_dict(self) -> dict[str, Any]:
        """Convert ledger record to dictionary."""
        return {
            "event_id": self.event_id,
            "event_fingerprint": self.event_fingerprint,
            "entity_sequence_key": self.entity_sequence_key,
            "table_name": self.table_name,
            "business_key_canonical": self.business_key_canonical,
            "sequence_number": self.sequence_number,
            "operation": self.operation,
            "source_commit_timestamp": self.source_commit_timestamp,
            "processing_id": self.processing_id,
            "source_file": self.source_file,
            "status": self.status,
        }


@dataclass
class MergePipelineResult:
    """Structured result metrics for a Delta Lake MERGE and recovery pipeline execution."""

    run_id: str
    processing_id: str
    events_received: int
    fresh_events: int
    recovered_pending_events: int
    events_applied: int
    replay_events_skipped: int
    stale_events_skipped: int
    insert_events_applied: int
    update_events_applied: int
    delete_events_applied: int
    groups_completed: int
    pending_events_remaining: int
    status: str  # SUCCESS | SUCCESS_WITH_SKIPS | FAILED_RECOVERABLE
