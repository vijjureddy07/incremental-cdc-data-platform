"""Unit tests for the snapshot mutation engine and state reconciliation oracle."""


from src.cdc.models import CDCEvent, CDCOperation
from src.source.mutation_engine import SourceMutationEngine


def test_mutation_engine_insert(mutation_engine: SourceMutationEngine):
    """Verify that valid INSERT event adds record to in-memory state."""
    initial_acc_count = mutation_engine.get_table_row_count("accounts")
    ins_ev = CDCEvent(
        event_id="evt_test_ins",
        table_name="accounts",
        operation=CDCOperation.INSERT.value,
        business_key={"account_id": "ACC-0999"},
        sequence_number=1,
        event_timestamp="2026-04-01T10:00:00Z",
        source_commit_timestamp="2026-04-01T10:00:01Z",
        batch_id="batch_001",
        payload={
            "account_id": "ACC-0999",
            "account_name": "Test Cloud Org",
            "industry": "Fintech",
            "country": "US",
            "status": "ACTIVE",
            "created_at": "2026-04-01T10:00:00Z",
            "updated_at": "2026-04-01T10:00:00Z",
        },
        before_payload=None,
    )

    applied = mutation_engine.apply_event(ins_ev)
    assert applied
    assert mutation_engine.get_table_row_count("accounts") == initial_acc_count + 1

    rec = mutation_engine.get_record("accounts", "ACC-0999")
    assert rec is not None
    assert rec["account_name"] == "Test Cloud Org"


def test_mutation_engine_update(mutation_engine: SourceMutationEngine):
    """Verify that UPDATE modifies record fields and updates updated_at timestamp."""
    upd_ev = CDCEvent(
        event_id="evt_test_upd",
        table_name="accounts",
        operation=CDCOperation.UPDATE.value,
        business_key={"account_id": "ACC-0001"},
        sequence_number=50,
        event_timestamp="2026-04-01T11:00:00Z",
        source_commit_timestamp="2026-04-01T11:00:01Z",
        batch_id="batch_001",
        before_payload={"account_id": "ACC-0001", "status": "ACTIVE"},
        payload={
            "account_id": "ACC-0001",
            "account_name": "Fintech Solutions 1 Updated",
            "industry": "Fintech",
            "country": "US",
            "status": "SUSPENDED",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-04-01T11:00:00Z",
        },
    )

    applied = mutation_engine.apply_event(upd_ev)
    assert applied

    rec = mutation_engine.get_record("accounts", "ACC-0001")
    assert rec is not None
    assert rec["status"] == "SUSPENDED"
    assert rec["account_name"] == "Fintech Solutions 1 Updated"
    assert rec["updated_at"] == "2026-04-01T11:00:00Z"


def test_mutation_engine_delete(mutation_engine: SourceMutationEngine):
    """Verify that DELETE removes record from in-memory state."""
    initial_pay_count = mutation_engine.get_table_row_count("payments")
    assert mutation_engine.get_record("payments", "PAY-0002") is not None

    del_ev = CDCEvent(
        event_id="evt_test_del",
        table_name="payments",
        operation=CDCOperation.DELETE.value,
        business_key={"payment_id": "PAY-0002"},
        sequence_number=99,
        event_timestamp="2026-04-01T12:00:00Z",
        source_commit_timestamp="2026-04-01T12:00:01Z",
        batch_id="batch_002",
        before_payload={"payment_id": "PAY-0002", "payment_amount": "199.00"},
        payload=None,
    )

    applied = mutation_engine.apply_event(del_ev)
    assert applied
    assert mutation_engine.get_table_row_count("payments") == initial_pay_count - 1
    assert mutation_engine.get_record("payments", "PAY-0002") is None


def test_mutation_engine_out_of_order_sequence_resolution(mutation_engine: SourceMutationEngine):
    """Verify that applying out-of-order events converges to highest sequence state."""
    # seq 102 (final desired state)
    ev_102 = CDCEvent(
        event_id="evt_ooo_102",
        table_name="accounts",
        operation=CDCOperation.UPDATE.value,
        business_key={"account_id": "ACC-0003"},
        sequence_number=102,
        event_timestamp="2026-04-01T12:10:00Z",
        source_commit_timestamp="2026-04-01T12:10:01Z",
        batch_id="batch_002",
        before_payload={"account_id": "ACC-0003", "status": "TRIAL"},
        payload={
            "account_id": "ACC-0003",
            "account_name": "Account 3 Final State",
            "industry": "SaaS",
            "country": "US",
            "status": "ACTIVE",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-04-01T12:10:00Z",
        },
    )

    # seq 101 (earlier state arriving late)
    ev_101 = CDCEvent(
        event_id="evt_ooo_101",
        table_name="accounts",
        operation=CDCOperation.UPDATE.value,
        business_key={"account_id": "ACC-0003"},
        sequence_number=101,
        event_timestamp="2026-04-01T12:05:00Z",
        source_commit_timestamp="2026-04-01T12:05:01Z",
        batch_id="batch_002",
        before_payload={"account_id": "ACC-0003", "status": "TRIAL"},
        payload={
            "account_id": "ACC-0003",
            "account_name": "Account 3 Intermediate State",
            "industry": "SaaS",
            "country": "US",
            "status": "TRIAL",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-04-01T12:05:00Z",
        },
    )

    # Apply batch containing [ev_102, ev_101]
    res = mutation_engine.apply_batch([ev_102, ev_101], sort_by_sequence=True)
    assert res.applied_count == 2

    # State must be the seq 102 state!
    rec = mutation_engine.get_record("accounts", "ACC-0003")
    assert rec is not None
    assert rec["account_name"] == "Account 3 Final State"
    assert rec["status"] == "ACTIVE"


def test_mutation_engine_idempotent_duplicate_rejection(mutation_engine: SourceMutationEngine):
    """Verify exact duplicate event_ids are ignored and do not corrupt state."""
    ins_ev = CDCEvent(
        event_id="evt_dup_test",
        table_name="accounts",
        operation=CDCOperation.INSERT.value,
        business_key={"account_id": "ACC-0888"},
        sequence_number=1,
        event_timestamp="2026-04-01T10:00:00Z",
        source_commit_timestamp="2026-04-01T10:00:01Z",
        batch_id="batch_001",
        payload={
            "account_id": "ACC-0888",
            "account_name": "Dupe Org",
            "industry": "Logistics",
            "country": "DE",
            "status": "ACTIVE",
            "created_at": "2026-04-01T10:00:00Z",
            "updated_at": "2026-04-01T10:00:00Z",
        },
        before_payload=None,
    )

    app1 = mutation_engine.apply_event(ins_ev)
    assert app1

    # Second application with same event_id
    app2 = mutation_engine.apply_event(ins_ev)
    assert not app2
