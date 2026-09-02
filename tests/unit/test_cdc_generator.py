"""Unit tests for CDC scenario generation across all business batches."""


from src.cdc.generator import CDCScenarioGenerator
from src.cdc.models import CDCOperation


def test_batch_1_inserts_and_updates(cdc_gen: CDCScenarioGenerator):
    """Verify Batch 1 contains expected inserts (Scenario A) and updates (Scenario B)."""
    batch_1 = cdc_gen.generate_batch_1_inserts_and_updates("batch_001")
    assert len(batch_1) == 8

    # 4 Inserts across 4 tables
    inserts = [e for e in batch_1 if e.operation == CDCOperation.INSERT.value]
    assert len(inserts) == 4
    insert_tables = {e.table_name for e in inserts}
    assert insert_tables == {"accounts", "subscriptions", "invoices", "payments"}

    # 4 Updates across 4 tables
    updates = [e for e in batch_1 if e.operation == CDCOperation.UPDATE.value]
    assert len(updates) == 4
    update_tables = {e.table_name for e in updates}
    assert update_tables == {"accounts", "subscriptions", "invoices", "payments"}

    # Verify updated_at is properly modified in payload
    for u in updates:
        assert u.payload is not None
        assert u.before_payload is not None
        assert u.payload["updated_at"] != u.before_payload["updated_at"]


def test_batch_2_advanced_scenarios(cdc_gen: CDCScenarioGenerator):
    """Verify Batch 2 contains delete (C), duplicate (D), out-of-order (E), late arrival (F)."""
    batch_2 = cdc_gen.generate_batch_2_advanced_cdc_scenarios("batch_002")
    assert len(batch_2) == 5

    # Scenario C: Delete
    deletes = [e for e in batch_2 if e.operation == CDCOperation.DELETE.value]
    assert len(deletes) == 1
    assert deletes[0].table_name == "payments"
    assert deletes[0].payload is None
    assert deletes[0].before_payload is not None

    # Scenario D: Duplicate event
    dups = [e for e in batch_2 if e.event_id == "evt_ins_acc_0041"]
    assert len(dups) == 1

    # Scenario E: Out-of-order events
    ooo_events = [e for e in batch_2 if e.business_key.get("account_id") == "ACC-0002"]
    assert len(ooo_events) == 2
    # Verify sequence 102 comes before sequence 101 in list
    assert ooo_events[0].sequence_number == 102
    assert ooo_events[1].sequence_number == 101

    # Scenario F: Late-arriving event
    late_events = [e for e in batch_2 if e.event_id == "evt_late_sub_0002"]
    assert len(late_events) == 1
    assert late_events[0].sequence_number == 5


def test_batch_3_quarantine_fixtures(cdc_gen: CDCScenarioGenerator):
    """Verify Batch 3 contains intentional malformed/invalid event fixtures."""
    fixtures = cdc_gen.generate_batch_3_quarantine_fixtures("batch_003_quarantine")
    assert len(fixtures) == 6
    assert any(f.get("operation") == "TRUNCATE" for f in fixtures)
    assert any(f.get("business_key") == {} for f in fixtures)
    assert any(f.get("sequence_number") == -5 for f in fixtures)
