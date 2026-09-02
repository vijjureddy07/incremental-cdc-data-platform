"""CDC Event Data Model and Operation Types."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class CDCOperation(StrEnum):
    """Supported Change Data Capture operations."""

    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"

    @classmethod
    def has_value(cls, value: str) -> bool:
        """Check whether a string value is a valid CDC operation."""
        return value in {item.value for item in cls}


@dataclass
class CDCEvent:
    """Canonical Change Data Capture event contract.

    Attributes:
        event_id: Unique identifier for the change event.
        table_name: Target source table (e.g., 'accounts', 'subscriptions').
        operation: 'INSERT', 'UPDATE', or 'DELETE'.
        business_key: Dictionary identifying primary key(s) (e.g., {'account_id': 'ACC-0001'}).
        sequence_number: Authoritative per-entity monotonically increasing sequence.
        event_timestamp: ISO 8601 timestamp when event occurred in business application.
        source_commit_timestamp: ISO 8601 timestamp when transaction committed to database WAL.
        batch_id: Logical ingestion batch identifier.
        payload: After-image dictionary (None for DELETE).
        before_payload: Before-image dictionary (None for INSERT).
        source_system: Identifier of the originating source system.
    """

    event_id: str
    table_name: str
    operation: str
    business_key: dict[str, Any]
    sequence_number: int
    event_timestamp: str
    source_commit_timestamp: str
    batch_id: str
    payload: dict[str, Any] | None
    before_payload: dict[str, Any] | None
    source_system: str = "b2b_saas_postgres"

    def to_dict(self) -> dict[str, Any]:
        """Convert CDCEvent instance to a standard Python dictionary."""
        return {
            "event_id": self.event_id,
            "table_name": self.table_name,
            "operation": self.operation,
            "business_key": dict(self.business_key),
            "sequence_number": self.sequence_number,
            "event_timestamp": self.event_timestamp,
            "source_commit_timestamp": self.source_commit_timestamp,
            "batch_id": self.batch_id,
            "payload": dict(self.payload) if self.payload is not None else None,
            "before_payload": dict(self.before_payload) if self.before_payload is not None else None,
            "source_system": self.source_system,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CDCEvent":
        """Instantiate CDCEvent from a dictionary."""
        return cls(
            event_id=str(data.get("event_id", "")),
            table_name=str(data.get("table_name", "")),
            operation=str(data.get("operation", "")),
            business_key=dict(data.get("business_key", {})),
            sequence_number=int(data.get("sequence_number", 0)),
            event_timestamp=str(data.get("event_timestamp", "")),
            source_commit_timestamp=str(data.get("source_commit_timestamp", "")),
            batch_id=str(data.get("batch_id", "")),
            payload=dict(data["payload"]) if data.get("payload") is not None else None,
            before_payload=dict(data["before_payload"]) if data.get("before_payload") is not None else None,
            source_system=str(data.get("source_system", "b2b_saas_postgres")),
        )
