"""Structural and semantic validation rules for raw CDC events."""

from typing import Any

from src.normalization.models import QuarantineReasonCode
from src.utils.helpers import parse_iso_timestamp

TABLE_PRIMARY_KEYS: dict[str, str] = {
    "accounts": "account_id",
    "subscriptions": "subscription_id",
    "invoices": "invoice_id",
    "payments": "payment_id",
}

VALID_OPERATIONS: set[str] = {"INSERT", "UPDATE", "DELETE"}


def validate_raw_cdc_record(
    record: dict[str, Any],
) -> tuple[bool, QuarantineReasonCode | None, str | None]:
    """Perform structural and semantic validation on a raw CDC record.

    Returns:
        tuple of (is_valid, quarantine_code, quarantine_reason)
    """
    # 1. Check event_id
    event_id = record.get("event_id")
    if not event_id or not str(event_id).strip():
        return False, QuarantineReasonCode.MISSING_EVENT_ID, "Event is missing a valid non-empty 'event_id'."

    # 2. Check table_name
    table_name = record.get("table_name")
    if not table_name or not str(table_name).strip():
        return False, QuarantineReasonCode.UNKNOWN_TABLE, "Event is missing a valid 'table_name'."
    if str(table_name) not in TABLE_PRIMARY_KEYS:
        return (
            False,
            QuarantineReasonCode.UNKNOWN_TABLE,
            f"Unknown source table '{table_name}'. Supported tables: {list(TABLE_PRIMARY_KEYS.keys())}.",
        )

    expected_pk = TABLE_PRIMARY_KEYS[str(table_name)]

    # 3. Check operation
    operation = record.get("operation")
    if not operation or str(operation).upper() not in VALID_OPERATIONS:
        return (
            False,
            QuarantineReasonCode.UNSUPPORTED_OPERATION,
            f"Unsupported CDC operation '{operation}'. Allowed: {sorted(VALID_OPERATIONS)}.",
        )
    op = str(operation).upper()

    # 4. Check business_key
    business_key = record.get("business_key")
    if not isinstance(business_key, dict) or not business_key:
        return (
            False,
            QuarantineReasonCode.MISSING_BUSINESS_KEY,
            "Event is missing a non-empty 'business_key' dictionary.",
        )
    if expected_pk not in business_key or not str(business_key[expected_pk]).strip():
        return (
            False,
            QuarantineReasonCode.INVALID_BUSINESS_KEY,
            f"Business key for table '{table_name}' must contain primary key column '{expected_pk}'.",
        )

    # 5. Check sequence_number
    if "sequence_number" not in record or record["sequence_number"] is None:
        return False, QuarantineReasonCode.MISSING_SEQUENCE, "Event is missing required 'sequence_number' field."
    try:
        seq_num = int(record["sequence_number"])
        if seq_num <= 0:
            return (
                False,
                QuarantineReasonCode.INVALID_SEQUENCE,
                f"Sequence number must be a strictly positive integer, got {seq_num}.",
            )
    except (ValueError, TypeError):
        return (
            False,
            QuarantineReasonCode.INVALID_SEQUENCE,
            f"Sequence number must be a valid integer, got {record.get('sequence_number')}.",
        )

    # 6. Check timestamps and validate ISO format
    event_timestamp = record.get("event_timestamp")
    if not event_timestamp or not str(event_timestamp).strip():
        return (
            False,
            QuarantineReasonCode.MISSING_EVENT_TIMESTAMP,
            "Event is missing a valid non-empty 'event_timestamp'.",
        )
    try:
        parse_iso_timestamp(str(event_timestamp))
    except Exception as err:
        return (
            False,
            QuarantineReasonCode.INVALID_EVENT_TIMESTAMP,
            f"Malformed event_timestamp '{event_timestamp}': {err}",
        )

    commit_timestamp = record.get("source_commit_timestamp")
    if not commit_timestamp or not str(commit_timestamp).strip():
        return (
            False,
            QuarantineReasonCode.MISSING_COMMIT_TIMESTAMP,
            "Event is missing a valid non-empty 'source_commit_timestamp'.",
        )
    try:
        parse_iso_timestamp(str(commit_timestamp))
    except Exception as err:
        return (
            False,
            QuarantineReasonCode.INVALID_COMMIT_TIMESTAMP,
            f"Malformed source_commit_timestamp '{commit_timestamp}': {err}",
        )

    # 7. Check source_system
    source_system = record.get("source_system")
    if not source_system or not str(source_system).strip():
        return (
            False,
            QuarantineReasonCode.MISSING_SOURCE_SYSTEM,
            "Event is missing a valid non-empty 'source_system'.",
        )

    payload = record.get("payload")
    before_payload = record.get("before_payload")

    # 8. Operation-semantic requirements
    if op == "INSERT":
        if not isinstance(payload, dict) or not payload:
            return (
                False,
                QuarantineReasonCode.MISSING_PAYLOAD,
                "INSERT operation requires a non-empty after-image 'payload'.",
            )
    elif op == "UPDATE":
        if not isinstance(payload, dict) or not payload:
            return (
                False,
                QuarantineReasonCode.MISSING_PAYLOAD,
                "UPDATE operation requires a non-empty after-image 'payload'.",
            )
        if not isinstance(before_payload, dict) or not before_payload:
            return (
                False,
                QuarantineReasonCode.MISSING_BEFORE_IMAGE,
                "UPDATE operation requires a non-empty before-image 'before_payload'.",
            )
    elif op == "DELETE":
        if not isinstance(before_payload, dict) or not before_payload:
            return (
                False,
                QuarantineReasonCode.MISSING_BEFORE_IMAGE,
                "DELETE operation requires a non-empty before-image 'before_payload'.",
            )
        if payload is not None:
            return (
                False,
                QuarantineReasonCode.UNEXPECTED_DELETE_PAYLOAD,
                "DELETE operation must have 'payload' set to None.",
            )

    # 9. Required Primary Key Presence and Value Consistency in Present Images
    expected_pk_val = str(business_key[expected_pk])

    if isinstance(payload, dict):
        if expected_pk not in payload:
            return (
                False,
                QuarantineReasonCode.BUSINESS_KEY_PAYLOAD_MISMATCH,
                (
                    f"Payload is missing required primary key column '{expected_pk}' "
                    f"for table '{table_name}'."
                ),
            )
        if str(payload[expected_pk]) != expected_pk_val:
            return (
                False,
                QuarantineReasonCode.BUSINESS_KEY_PAYLOAD_MISMATCH,
                (
                    f"Payload primary key '{payload[expected_pk]}' does not match "
                    f"business key '{expected_pk_val}' for table '{table_name}'."
                ),
            )

    if isinstance(before_payload, dict):
        if expected_pk not in before_payload:
            return (
                False,
                QuarantineReasonCode.BUSINESS_KEY_PAYLOAD_MISMATCH,
                (
                    f"Before-payload is missing required primary key column '{expected_pk}' "
                    f"for table '{table_name}'."
                ),
            )
        if str(before_payload[expected_pk]) != expected_pk_val:
            return (
                False,
                QuarantineReasonCode.BUSINESS_KEY_PAYLOAD_MISMATCH,
                (
                    f"Before-payload primary key '{before_payload[expected_pk]}' does not match "
                    f"business key '{expected_pk_val}' for table '{table_name}'."
                ),
            )

    return True, None, None
