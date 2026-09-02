"""Unit tests for structural and semantic validation rules of raw CDC events."""

from src.normalization.models import QuarantineReasonCode
from src.normalization.validator import validate_raw_cdc_record


def test_validator_valid_insert():
    """Verify a valid INSERT event passes validation."""
    record = {
        "event_id": "evt_ins_001",
        "table_name": "accounts",
        "operation": "INSERT",
        "business_key": {"account_id": "ACC-0041"},
        "sequence_number": 1,
        "event_timestamp": "2026-05-11T01:05:00Z",
        "source_commit_timestamp": "2026-05-11T01:05:01Z",
        "batch_id": "batch_001",
        "payload": {"account_id": "ACC-0041", "account_name": "Apex Co"},
        "before_payload": None,
        "source_system": "b2b_saas_postgres",
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is True
    assert q_code is None
    assert q_reason is None


def test_validator_valid_update():
    """Verify a valid UPDATE event passes validation."""
    record = {
        "event_id": "evt_upd_001",
        "table_name": "subscriptions",
        "operation": "UPDATE",
        "business_key": {"subscription_id": "SUB-0001"},
        "sequence_number": 10,
        "event_timestamp": "2026-05-11T01:30:00Z",
        "source_commit_timestamp": "2026-05-11T01:30:01Z",
        "batch_id": "batch_001",
        "payload": {"subscription_id": "SUB-0001", "plan_name": "ENTERPRISE"},
        "before_payload": {"subscription_id": "SUB-0001", "plan_name": "STARTER"},
        "source_system": "b2b_saas_postgres",
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is True
    assert q_code is None


def test_validator_valid_delete():
    """Verify a valid DELETE event passes validation."""
    record = {
        "event_id": "evt_del_001",
        "table_name": "payments",
        "operation": "DELETE",
        "business_key": {"payment_id": "PAY-0002"},
        "sequence_number": 30,
        "event_timestamp": "2026-05-13T01:00:00Z",
        "source_commit_timestamp": "2026-05-13T01:00:01Z",
        "batch_id": "batch_002",
        "payload": None,
        "before_payload": {"payment_id": "PAY-0002", "payment_amount": "199.00"},
        "source_system": "b2b_saas_postgres",
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is True
    assert q_code is None


def test_validator_missing_event_id():
    """Verify missing event_id triggers MISSING_EVENT_ID quarantine code."""
    record = {
        "event_id": "",
        "table_name": "accounts",
        "operation": "INSERT",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 1,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "payload": {"account_id": "ACC-0001"},
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is False
    assert q_code == QuarantineReasonCode.MISSING_EVENT_ID


def test_validator_unknown_table():
    """Verify unknown table name triggers UNKNOWN_TABLE quarantine code."""
    record = {
        "event_id": "evt_001",
        "table_name": "non_existent_table",
        "operation": "INSERT",
        "business_key": {"id": "1"},
        "sequence_number": 1,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "payload": {"id": "1"},
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is False
    assert q_code == QuarantineReasonCode.UNKNOWN_TABLE


def test_validator_unsupported_operation():
    """Verify unsupported operation triggers UNSUPPORTED_OPERATION quarantine code."""
    record = {
        "event_id": "evt_001",
        "table_name": "accounts",
        "operation": "UPSERT",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 1,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "payload": {"account_id": "ACC-0001"},
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is False
    assert q_code == QuarantineReasonCode.UNSUPPORTED_OPERATION


def test_validator_invalid_business_key():
    """Verify business key missing primary key column triggers INVALID_BUSINESS_KEY."""
    record = {
        "event_id": "evt_001",
        "table_name": "accounts",
        "operation": "INSERT",
        "business_key": {"wrong_column": "VAL-1"},  # Expected account_id
        "sequence_number": 1,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "payload": {"account_id": "ACC-0001"},
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is False
    assert q_code == QuarantineReasonCode.INVALID_BUSINESS_KEY


def test_validator_invalid_sequence_number():
    """Verify negative or non-positive sequence number triggers INVALID_SEQUENCE."""
    record = {
        "event_id": "evt_001",
        "table_name": "accounts",
        "operation": "INSERT",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": -5,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "payload": {"account_id": "ACC-0001"},
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is False
    assert q_code == QuarantineReasonCode.INVALID_SEQUENCE


def test_validator_insert_missing_payload():
    """Verify INSERT without payload triggers MISSING_PAYLOAD."""
    record = {
        "event_id": "evt_001",
        "table_name": "accounts",
        "operation": "INSERT",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 1,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "source_system": "b2b_saas_postgres",
        "payload": None,
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is False
    assert q_code == QuarantineReasonCode.MISSING_PAYLOAD


def test_validator_update_missing_before_payload():
    """Verify UPDATE without before_payload triggers MISSING_BEFORE_IMAGE."""
    record = {
        "event_id": "evt_001",
        "table_name": "accounts",
        "operation": "UPDATE",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 2,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "source_system": "b2b_saas_postgres",
        "payload": {"account_id": "ACC-0001"},
        "before_payload": None,
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is False
    assert q_code == QuarantineReasonCode.MISSING_BEFORE_IMAGE


def test_validator_delete_with_unexpected_payload():
    """Verify DELETE with unexpected after-image payload triggers UNEXPECTED_DELETE_PAYLOAD."""
    record = {
        "event_id": "evt_001",
        "table_name": "accounts",
        "operation": "DELETE",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 3,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "source_system": "b2b_saas_postgres",
        "payload": {"account_id": "ACC-0001"},  # Unexpected
        "before_payload": {"account_id": "ACC-0001"},
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is False
    assert q_code == QuarantineReasonCode.UNEXPECTED_DELETE_PAYLOAD


def test_validator_business_key_payload_mismatch():
    """Verify payload primary key differing from business_key triggers BUSINESS_KEY_PAYLOAD_MISMATCH."""
    record = {
        "event_id": "evt_001",
        "table_name": "accounts",
        "operation": "INSERT",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 1,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "source_system": "b2b_saas_postgres",
        "payload": {"account_id": "ACC-9999", "status": "ACTIVE"},  # Mismatched primary key
        "before_payload": None,
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is False
    assert q_code == QuarantineReasonCode.BUSINESS_KEY_PAYLOAD_MISMATCH


def test_validator_insert_payload_missing_primary_key():
    """Verify INSERT payload missing the expected PK column is quarantined."""
    record = {
        "event_id": "evt_001",
        "table_name": "accounts",
        "operation": "INSERT",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 1,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "source_system": "b2b_saas_postgres",
        "payload": {"status": "ACTIVE"},  # Missing account_id in payload
        "before_payload": None,
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is False
    assert q_code == QuarantineReasonCode.BUSINESS_KEY_PAYLOAD_MISMATCH
    assert "missing required primary key column 'account_id'" in str(q_reason)


def test_validator_update_payload_missing_primary_key():
    """Verify UPDATE payload missing the expected PK column is quarantined."""
    record = {
        "event_id": "evt_002",
        "table_name": "subscriptions",
        "operation": "UPDATE",
        "business_key": {"subscription_id": "SUB-0001"},
        "sequence_number": 2,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "source_system": "b2b_saas_postgres",
        "payload": {"plan_name": "PRO"},  # Missing subscription_id
        "before_payload": {"subscription_id": "SUB-0001", "plan_name": "BASIC"},
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is False
    assert q_code == QuarantineReasonCode.BUSINESS_KEY_PAYLOAD_MISMATCH
    assert "missing required primary key column 'subscription_id'" in str(q_reason)


def test_validator_update_before_payload_missing_primary_key():
    """Verify UPDATE before_payload missing expected PK column is quarantined."""
    record = {
        "event_id": "evt_003",
        "table_name": "subscriptions",
        "operation": "UPDATE",
        "business_key": {"subscription_id": "SUB-0001"},
        "sequence_number": 3,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "source_system": "b2b_saas_postgres",
        "payload": {"subscription_id": "SUB-0001", "plan_name": "PRO"},
        "before_payload": {"plan_name": "BASIC"},  # Missing subscription_id
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is False
    assert q_code == QuarantineReasonCode.BUSINESS_KEY_PAYLOAD_MISMATCH
    assert "Before-payload is missing required primary key column 'subscription_id'" in str(
        q_reason
    )


def test_validator_delete_before_payload_missing_primary_key():
    """Verify DELETE before_payload missing expected PK column is quarantined."""
    record = {
        "event_id": "evt_004",
        "table_name": "payments",
        "operation": "DELETE",
        "business_key": {"payment_id": "PAY-0001"},
        "sequence_number": 4,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "source_system": "b2b_saas_postgres",
        "payload": None,
        "before_payload": {"amount": "100.00"},  # Missing payment_id
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is False
    assert q_code == QuarantineReasonCode.BUSINESS_KEY_PAYLOAD_MISMATCH
    assert "Before-payload is missing required primary key column 'payment_id'" in str(q_reason)


def test_validator_invalid_event_timestamp_format():
    """Verify malformed event_timestamp triggers INVALID_EVENT_TIMESTAMP."""
    record = {
        "event_id": "evt_005",
        "table_name": "accounts",
        "operation": "INSERT",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 1,
        "event_timestamp": "NOT_AN_ISO_TIMESTAMP",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "source_system": "b2b_saas_postgres",
        "payload": {"account_id": "ACC-0001"},
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is False
    assert q_code == QuarantineReasonCode.INVALID_EVENT_TIMESTAMP


def test_validator_invalid_commit_timestamp_format():
    """Verify malformed source_commit_timestamp triggers INVALID_COMMIT_TIMESTAMP."""
    record = {
        "event_id": "evt_006",
        "table_name": "accounts",
        "operation": "INSERT",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 1,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "INVALID_COMMIT_TIME",
        "source_system": "b2b_saas_postgres",
        "payload": {"account_id": "ACC-0001"},
    }
    is_valid, q_code, q_reason = validate_raw_cdc_record(record)
    assert is_valid is False
    assert q_code == QuarantineReasonCode.INVALID_COMMIT_TIMESTAMP
