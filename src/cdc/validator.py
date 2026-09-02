"""CDC Event Structural and Semantic Validator."""

from dataclasses import dataclass, field
from typing import Any

from src.cdc.models import CDCEvent, CDCOperation
from src.source.schemas import TABLE_PRIMARY_KEYS, TABLE_SCHEMAS_MAP
from src.utils.helpers import parse_iso_timestamp


@dataclass
class ValidationResult:
    """Structured validation report for a CDC event."""

    is_valid: bool
    errors: list[str] = field(default_factory=list)
    event_id: str | None = None
    table_name: str | None = None

    def add_error(self, message: str) -> None:
        """Append an error message and set validity to False."""
        self.is_valid = False
        self.errors.append(message)


class CDCValidator:
    """Validates CDC change events against the canonical contract."""

    ALLOWED_OPERATIONS = {CDCOperation.INSERT.value, CDCOperation.UPDATE.value, CDCOperation.DELETE.value}
    ALLOWED_TABLES = set(TABLE_SCHEMAS_MAP.keys())

    @classmethod
    def validate(cls, event: CDCEvent | dict[str, Any]) -> ValidationResult:
        """Validate a CDCEvent or raw event dictionary against all quality rules."""
        if isinstance(event, CDCEvent):
            data = event.to_dict()
        elif isinstance(event, dict):
            data = event
        else:
            return ValidationResult(
                is_valid=False,
                errors=[f"Unsupported event type: {type(event).__name__}"],
            )

        result = ValidationResult(
            is_valid=True,
            errors=[],
            event_id=data.get("event_id"),
            table_name=data.get("table_name"),
        )

        # 1. event_id validation
        event_id = data.get("event_id")
        if not event_id or not isinstance(event_id, str) or not event_id.strip():
            result.add_error("Missing or empty 'event_id'")

        # 2. table_name validation
        table_name = data.get("table_name")
        if not table_name or not isinstance(table_name, str):
            result.add_error("Missing or invalid 'table_name'")
        elif table_name not in cls.ALLOWED_TABLES:
            result.add_error(
                f"Unsupported 'table_name': '{table_name}'. Must be one of {sorted(cls.ALLOWED_TABLES)}"
            )

        # 3. operation validation
        operation = data.get("operation")
        if not operation or not isinstance(operation, str):
            result.add_error("Missing or invalid 'operation'")
        elif operation not in cls.ALLOWED_OPERATIONS:
            result.add_error(
                f"Unsupported 'operation': '{operation}'. Must be one of {sorted(cls.ALLOWED_OPERATIONS)}"
            )

        # 4. business_key validation
        business_key = data.get("business_key")
        if not business_key or not isinstance(business_key, dict) or len(business_key) == 0:
            result.add_error("Missing, empty, or non-dict 'business_key'")
        elif table_name in TABLE_PRIMARY_KEYS:
            expected_pk = TABLE_PRIMARY_KEYS[table_name]
            if expected_pk not in business_key or not business_key[expected_pk]:
                result.add_error(
                    f"Business key missing primary key '{expected_pk}' for table '{table_name}'"
                )

        # 5. sequence_number validation
        seq_num = data.get("sequence_number")
        if seq_num is None or not isinstance(seq_num, int) or isinstance(seq_num, bool) or seq_num <= 0:
            result.add_error(f"Invalid 'sequence_number': {seq_num}. Must be a strictly positive integer (> 0)")

        # 6. timestamps validation
        event_ts = data.get("event_timestamp")
        if not event_ts or not isinstance(event_ts, str):
            result.add_error("Missing or invalid 'event_timestamp'")
        else:
            try:
                parse_iso_timestamp(event_ts)
            except Exception as e:
                result.add_error(f"Malformed 'event_timestamp' ({event_ts}): {e}")

        commit_ts = data.get("source_commit_timestamp")
        if not commit_ts or not isinstance(commit_ts, str):
            result.add_error("Missing or invalid 'source_commit_timestamp'")
        else:
            try:
                parse_iso_timestamp(commit_ts)
            except Exception as e:
                result.add_error(f"Malformed 'source_commit_timestamp' ({commit_ts}): {e}")

        # 7. batch_id & source_system validation
        batch_id = data.get("batch_id")
        if not batch_id or not isinstance(batch_id, str) or not batch_id.strip():
            result.add_error("Missing or empty 'batch_id'")

        source_system = data.get("source_system")
        if not source_system or not isinstance(source_system, str) or not source_system.strip():
            result.add_error("Missing or empty 'source_system'")

        # 8. Payload rules per operation
        payload = data.get("payload")
        before_payload = data.get("before_payload")

        if operation == CDCOperation.INSERT.value:
            if payload is None or not isinstance(payload, dict) or len(payload) == 0:
                result.add_error("INSERT operation must have a non-empty 'payload' (after-image)")
            if before_payload is not None:
                result.add_error("INSERT operation must have null 'before_payload'")

        elif operation == CDCOperation.UPDATE.value:
            if payload is None or not isinstance(payload, dict) or len(payload) == 0:
                result.add_error("UPDATE operation must have a non-empty 'payload' (after-image)")
            if before_payload is None or not isinstance(before_payload, dict) or len(before_payload) == 0:
                result.add_error("UPDATE operation must have a non-empty 'before_payload' (before-image)")

        elif operation == CDCOperation.DELETE.value:
            if payload is not None:
                result.add_error("DELETE operation must have null 'payload'")
            if before_payload is None or not isinstance(before_payload, dict) or len(before_payload) == 0:
                result.add_error("DELETE operation must have a non-empty 'before_payload' (before-image)")

        return result
