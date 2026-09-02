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


def test_mutation_engine_out_of_order_sequence_resolution_sorted(
    mutation_engine: SourceMutationEngine,
):
    """Verify that applying out-of-order events with sort_by_sequence=True converges to highest sequence."""
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

    # In sorted mode: [ev_102, ev_101] is pre-sorted to [ev_101, ev_102] and both applied in order
    res = mutation_engine.apply_batch([ev_102, ev_101], sort_by_sequence=True)
    assert res.applied_count == 2

    # State must be the seq 102 state
    rec = mutation_engine.get_record("accounts", "ACC-0003")
    assert rec is not None
    assert rec["account_name"] == "Account 3 Final State"
    assert rec["status"] == "ACTIVE"


def test_mutation_engine_real_out_of_order_arrival_without_presort(
    mutation_engine: SourceMutationEngine,
):
    """Verify real out-of-order delivery (102 arriving before 101) with sort_by_sequence=False."""
    ev_102 = CDCEvent(
        event_id="evt_real_ooo_102",
        table_name="accounts",
        operation=CDCOperation.UPDATE.value,
        business_key={"account_id": "ACC-0004"},
        sequence_number=102,
        event_timestamp="2026-04-01T12:10:00Z",
        source_commit_timestamp="2026-04-01T12:10:01Z",
        batch_id="batch_002",
        before_payload={"account_id": "ACC-0004", "status": "TRIAL"},
        payload={
            "account_id": "ACC-0004",
            "account_name": "Account 4 Seq102 State",
            "industry": "SaaS",
            "country": "US",
            "status": "ACTIVE",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-04-01T12:10:00Z",
        },
    )

    ev_101 = CDCEvent(
        event_id="evt_real_ooo_101",
        table_name="accounts",
        operation=CDCOperation.UPDATE.value,
        business_key={"account_id": "ACC-0004"},
        sequence_number=101,
        event_timestamp="2026-04-01T12:05:00Z",
        source_commit_timestamp="2026-04-01T12:05:01Z",
        batch_id="batch_002",
        before_payload={"account_id": "ACC-0004", "status": "TRIAL"},
        payload={
            "account_id": "ACC-0004",
            "account_name": "Account 4 Stale Seq101 State",
            "industry": "SaaS",
            "country": "US",
            "status": "TRIAL",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-04-01T12:05:00Z",
        },
    )

    # Apply in real arrival order: [ev_102, ev_101] without pre-sorting
    res = mutation_engine.apply_batch([ev_102, ev_101], sort_by_sequence=False)

    # ev_102 accepted, ev_101 rejected as stale (101 <= 102)
    assert res.applied_count == 1
    assert res.stale_sequence_count == 1

    # Final state remains from seq 102
    rec = mutation_engine.get_record("accounts", "ACC-0004")
    assert rec is not None
    assert rec["account_name"] == "Account 4 Seq102 State"
    assert rec["status"] == "ACTIVE"


def test_mutation_engine_strict_monotonicity_equal_sequence_rejection(
    mutation_engine: SourceMutationEngine,
):
    """Verify that a different event_id with equal sequence_number (seq 50 then seq 50) is rejected."""
    ev_50_a = CDCEvent(
        event_id="evt_seq50_first",
        table_name="accounts",
        operation=CDCOperation.UPDATE.value,
        business_key={"account_id": "ACC-0005"},
        sequence_number=50,
        event_timestamp="2026-04-01T10:00:00Z",
        source_commit_timestamp="2026-04-01T10:00:01Z",
        batch_id="batch_001",
        before_payload={"account_id": "ACC-0005", "status": "ACTIVE"},
        payload={
            "account_id": "ACC-0005",
            "account_name": "Account 5 First Seq50 State",
            "industry": "Fintech",
            "country": "US",
            "status": "ACTIVE",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-04-01T10:00:00Z",
        },
    )

    ev_50_b = CDCEvent(
        event_id="evt_seq50_second_different_id",
        table_name="accounts",
        operation=CDCOperation.UPDATE.value,
        business_key={"account_id": "ACC-0005"},
        sequence_number=50,  # Same sequence number
        event_timestamp="2026-04-01T10:05:00Z",  # Even with later timestamp!
        source_commit_timestamp="2026-04-01T10:05:01Z",
        batch_id="batch_001",
        before_payload={"account_id": "ACC-0005", "status": "ACTIVE"},
        payload={
            "account_id": "ACC-0005",
            "account_name": "Account 5 Conflicting Seq50 State",
            "industry": "Fintech",
            "country": "US",
            "status": "SUSPENDED",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-04-01T10:05:00Z",
        },
    )

    # First event seq 50 applied
    applied_first = mutation_engine.apply_event(ev_50_a)
    assert applied_first

    # Second event with equal sequence 50 must NOT mutate state
    applied_second = mutation_engine.apply_event(ev_50_b)
    assert not applied_second

    # State must remain from the first accepted event
    rec = mutation_engine.get_record("accounts", "ACC-0005")
    assert rec is not None
    assert rec["account_name"] == "Account 5 First Seq50 State"
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


def test_mutation_engine_snapshot_state_returns_actual_deep_copy(
    mutation_engine: SourceMutationEngine,
):
    """Verify get_snapshot_state returns a true deep copy so caller mutations do not leak."""
    initial_rec = mutation_engine.get_record("accounts", "ACC-0001")
    assert initial_rec is not None
    original_name = initial_rec["account_name"]

    # Retrieve snapshot state and modify returned dictionary
    snapshot = mutation_engine.get_snapshot_state()
    acc_0001 = next(r for r in snapshot["accounts"] if r["account_id"] == "ACC-0001")
    acc_0001["account_name"] = "Malicious External Mutation"

    # Internal oracle state must remain completely unaltered
    fresh_rec = mutation_engine.get_record("accounts", "ACC-0001")
    assert fresh_rec is not None
    assert fresh_rec["account_name"] == original_name
    assert fresh_rec["account_name"] != "Malicious External Mutation"
