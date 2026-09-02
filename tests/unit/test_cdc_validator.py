"""Unit tests for CDC structural and semantic validator."""


from src.cdc.models import CDCEvent
from src.cdc.validator import CDCValidator


def test_validator_valid_insert():
    """Verify validation passes for a properly formed INSERT event."""
    ev = CDCEvent(
        event_id="evt_valid_ins",
        table_name="accounts",
        operation="INSERT",
        business_key={"account_id": "ACC-0041"},
        sequence_number=1,
        event_timestamp="2026-04-01T10:00:00Z",
        source_commit_timestamp="2026-04-01T10:00:01Z",
        batch_id="batch_001",
        payload={"account_id": "ACC-0041", "status": "ACTIVE"},
        before_payload=None,
    )
    result = CDCValidator.validate(ev)
    assert result.is_valid
    assert len(result.errors) == 0


def test_validator_valid_update():
    """Verify validation passes for a properly formed UPDATE event with before and after images."""
    ev = CDCEvent(
        event_id="evt_valid_upd",
        table_name="subscriptions",
        operation="UPDATE",
        business_key={"subscription_id": "SUB-0001"},
        sequence_number=10,
        event_timestamp="2026-04-01T10:05:00Z",
        source_commit_timestamp="2026-04-01T10:05:01Z",
        batch_id="batch_001",
        before_payload={"subscription_id": "SUB-0001", "plan_name": "STARTER"},
        payload={"subscription_id": "SUB-0001", "plan_name": "ENTERPRISE"},
    )
    result = CDCValidator.validate(ev)
    assert result.is_valid
    assert len(result.errors) == 0


def test_validator_valid_delete():
    """Verify validation passes for a properly formed DELETE event with before image and null payload."""
    ev = CDCEvent(
        event_id="evt_valid_del",
        table_name="payments",
        operation="DELETE",
        business_key={"payment_id": "PAY-0002"},
        sequence_number=30,
        event_timestamp="2026-04-01T10:10:00Z",
        source_commit_timestamp="2026-04-01T10:10:01Z",
        batch_id="batch_002",
        before_payload={"payment_id": "PAY-0002", "payment_amount": "199.00"},
        payload=None,
    )
    result = CDCValidator.validate(ev)
    assert result.is_valid
    assert len(result.errors) == 0


def test_validator_rejection_invalid_operation():
    """Verify rejection when operation is unsupported (e.g. TRUNCATE)."""
    raw_ev = {
        "event_id": "evt_bad_op",
        "table_name": "accounts",
        "operation": "TRUNCATE",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 1,
        "event_timestamp": "2026-04-01T10:00:00Z",
        "source_commit_timestamp": "2026-04-01T10:00:01Z",
        "batch_id": "batch_001",
        "payload": {},
        "before_payload": None,
        "source_system": "b2b_saas_postgres",
    }
    result = CDCValidator.validate(raw_ev)
    assert not result.is_valid
    assert any("Unsupported 'operation'" in err for err in result.errors)


def test_validator_rejection_missing_business_key():
    """Verify rejection when business_key is missing or empty."""
    raw_ev = {
        "event_id": "evt_bad_pk",
        "table_name": "accounts",
        "operation": "INSERT",
        "business_key": {},
        "sequence_number": 1,
        "event_timestamp": "2026-04-01T10:00:00Z",
        "source_commit_timestamp": "2026-04-01T10:00:01Z",
        "batch_id": "batch_001",
        "payload": {"account_id": "ACC-0001"},
        "before_payload": None,
        "source_system": "b2b_saas_postgres",
    }
    result = CDCValidator.validate(raw_ev)
    assert not result.is_valid
    assert any("business_key" in err.lower() for err in result.errors)


def test_validator_rejection_missing_or_negative_sequence():
    """Verify rejection when sequence_number is non-positive or missing."""
    raw_ev = {
        "event_id": "evt_bad_seq",
        "table_name": "invoices",
        "operation": "UPDATE",
        "business_key": {"invoice_id": "INV-0001"},
        "sequence_number": -1,
        "event_timestamp": "2026-04-01T10:00:00Z",
        "source_commit_timestamp": "2026-04-01T10:00:01Z",
        "batch_id": "batch_001",
        "payload": {"invoice_id": "INV-0001", "invoice_status": "PAID"},
        "before_payload": {"invoice_id": "INV-0001", "invoice_status": "ISSUED"},
        "source_system": "b2b_saas_postgres",
    }
    result = CDCValidator.validate(raw_ev)
    assert not result.is_valid
    assert any("sequence_number" in err.lower() for err in result.errors)


def test_validator_rejection_delete_missing_before_payload():
    """Verify rejection when DELETE event lacks before_payload."""
    raw_ev = {
        "event_id": "evt_bad_del",
        "table_name": "payments",
        "operation": "DELETE",
        "business_key": {"payment_id": "PAY-0001"},
        "sequence_number": 5,
        "event_timestamp": "2026-04-01T10:00:00Z",
        "source_commit_timestamp": "2026-04-01T10:00:01Z",
        "batch_id": "batch_001",
        "payload": None,
        "before_payload": None,
        "source_system": "b2b_saas_postgres",
    }
    result = CDCValidator.validate(raw_ev)
    assert not result.is_valid
    assert any("before_payload" in err.lower() for err in result.errors)
