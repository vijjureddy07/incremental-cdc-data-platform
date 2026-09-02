"""Data models, quarantine reason codes, and audit metrics for CDC normalization."""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class QuarantineReasonCode(StrEnum):
    """Machine-readable taxonomy of quarantine reason codes for CDC normalization."""

    MISSING_EVENT_ID = "MISSING_EVENT_ID"
    UNKNOWN_TABLE = "UNKNOWN_TABLE"
    UNSUPPORTED_OPERATION = "UNSUPPORTED_OPERATION"
    MISSING_BUSINESS_KEY = "MISSING_BUSINESS_KEY"
    INVALID_BUSINESS_KEY = "INVALID_BUSINESS_KEY"
    MISSING_SEQUENCE = "MISSING_SEQUENCE"
    INVALID_SEQUENCE = "INVALID_SEQUENCE"
    MISSING_EVENT_TIMESTAMP = "MISSING_EVENT_TIMESTAMP"
    MISSING_COMMIT_TIMESTAMP = "MISSING_COMMIT_TIMESTAMP"
    MISSING_SOURCE_SYSTEM = "MISSING_SOURCE_SYSTEM"
    MISSING_PAYLOAD = "MISSING_PAYLOAD"
    MISSING_BEFORE_IMAGE = "MISSING_BEFORE_IMAGE"
    UNEXPECTED_DELETE_PAYLOAD = "UNEXPECTED_DELETE_PAYLOAD"
    BUSINESS_KEY_PAYLOAD_MISMATCH = "BUSINESS_KEY_PAYLOAD_MISMATCH"
    DUPLICATE_EVENT_CONFLICT = "DUPLICATE_EVENT_CONFLICT"
    SEQUENCE_CONFLICT = "SEQUENCE_CONFLICT"
    MALFORMED_JSON = "MALFORMED_JSON"


@dataclass
class NormalizedCDCEvent:
    """Canonical normalized CDC event contract for downstream processing."""

    event_id: str
    table_name: str
    operation: str
    business_key: dict[str, Any]
    business_key_canonical: str
    entity_sequence_key: str
    sequence_number: int
    event_timestamp: str
    source_commit_timestamp: str
    batch_id: str
    source_system: str
    payload: dict[str, Any] | None
    before_payload: dict[str, Any] | None
    event_fingerprint: str
    ingestion_batch_id: str
    source_file: str
    is_late_arrival: bool = False
    normalized_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert normalized event to a standard Python dictionary."""
        return {
            "event_id": self.event_id,
            "table_name": self.table_name,
            "operation": self.operation,
            "business_key": dict(self.business_key),
            "business_key_canonical": self.business_key_canonical,
            "entity_sequence_key": self.entity_sequence_key,
            "sequence_number": self.sequence_number,
            "event_timestamp": self.event_timestamp,
            "source_commit_timestamp": self.source_commit_timestamp,
            "batch_id": self.batch_id,
            "source_system": self.source_system,
            "payload": dict(self.payload) if self.payload is not None else None,
            "before_payload": dict(self.before_payload) if self.before_payload is not None else None,
            "event_fingerprint": self.event_fingerprint,
            "ingestion_batch_id": self.ingestion_batch_id,
            "source_file": self.source_file,
            "is_late_arrival": self.is_late_arrival,
            "normalized_at": self.normalized_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "NormalizedCDCEvent":
        """Instantiate normalized event from dictionary."""
        return cls(
            event_id=str(data["event_id"]),
            table_name=str(data["table_name"]),
            operation=str(data["operation"]),
            business_key=dict(data["business_key"]),
            business_key_canonical=str(data["business_key_canonical"]),
            entity_sequence_key=str(data["entity_sequence_key"]),
            sequence_number=int(data["sequence_number"]),
            event_timestamp=str(data["event_timestamp"]),
            source_commit_timestamp=str(data["source_commit_timestamp"]),
            batch_id=str(data["batch_id"]),
            source_system=str(data["source_system"]),
            payload=dict(data["payload"]) if data.get("payload") is not None else None,
            before_payload=dict(data["before_payload"]) if data.get("before_payload") is not None else None,
            event_fingerprint=str(data["event_fingerprint"]),
            ingestion_batch_id=str(data.get("ingestion_batch_id", "")),
            source_file=str(data.get("source_file", "")),
            is_late_arrival=bool(data.get("is_late_arrival", False)),
            normalized_at=str(data.get("normalized_at", "")),
        )


@dataclass
class QuarantinedEvent:
    """Quarantined dead-letter record preserving failure context and original payload."""

    quarantine_code: str
    quarantine_reason: str
    raw_record: dict[str, Any] | str | None
    event_id: str | None = None
    table_name: str | None = None
    source_file: str | None = None
    batch_id: str | None = None
    quarantined_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert quarantined event to standard dictionary."""
        return {
            "quarantine_code": (
                self.quarantine_code.value
                if isinstance(self.quarantine_code, QuarantineReasonCode)
                else str(self.quarantine_code)
            ),
            "quarantine_reason": self.quarantine_reason,
            "raw_record": self.raw_record,
            "event_id": self.event_id,
            "table_name": self.table_name,
            "source_file": self.source_file,
            "batch_id": self.batch_id,
            "quarantined_at": self.quarantined_at,
        }


@dataclass
class NormalizationAuditMetrics:
    """Operational audit metrics emitted by the normalization pipeline."""

    run_id: str
    processing_id: str
    files_read: list[str] = field(default_factory=list)
    raw_records_seen: int = 0
    parsed_records: int = 0
    accepted_records: int = 0
    exact_duplicates_dropped: int = 0
    quarantined_records: int = 0
    malformed_json_records: int = 0
    duplicate_event_conflicts: int = 0
    sequence_conflicts: int = 0
    tables_seen: list[str] = field(default_factory=list)
    min_sequence_by_table: dict[str, int] = field(default_factory=dict)
    max_sequence_by_table: dict[str, int] = field(default_factory=dict)
    started_at: str = ""
    completed_at: str = ""
    status: str = "SUCCESS"

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "run_id": self.run_id,
            "processing_id": self.processing_id,
            "files_read": list(self.files_read),
            "raw_records_seen": self.raw_records_seen,
            "parsed_records": self.parsed_records,
            "accepted_records": self.accepted_records,
            "exact_duplicates_dropped": self.exact_duplicates_dropped,
            "quarantined_records": self.quarantined_records,
            "malformed_json_records": self.malformed_json_records,
            "duplicate_event_conflicts": self.duplicate_event_conflicts,
            "sequence_conflicts": self.sequence_conflicts,
            "tables_seen": list(self.tables_seen),
            "min_sequence_by_table": dict(self.min_sequence_by_table),
            "max_sequence_by_table": dict(self.max_sequence_by_table),
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "status": self.status,
        }
