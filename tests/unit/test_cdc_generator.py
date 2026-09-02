"""Unit tests for CDC scenario generation and source-consistency contracts."""

from src.cdc.generator import CDCScenarioGenerator
from src.cdc.models import CDCOperation
from src.source.generator import SourceGenerator
from src.utils.helpers import parse_iso_timestamp


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


def test_update_before_images_derive_from_actual_source_snapshot(
    source_gen: SourceGenerator,
    cdc_gen: CDCScenarioGenerator,
):
    """Verify all UPDATE before-images match the actual deterministic source snapshot records."""
    raw_snapshot = source_gen.generate_snapshot_dicts()
    acc_map = {r["account_id"]: r for r in raw_snapshot["accounts"]}
    sub_map = {r["subscription_id"]: r for r in raw_snapshot["subscriptions"]}
    inv_map = {r["invoice_id"]: r for r in raw_snapshot["invoices"]}
    pay_map = {r["payment_id"]: r for r in raw_snapshot["payments"]}

    batch_1 = cdc_gen.generate_batch_1_inserts_and_updates("batch_001")

    # A: ACC-0001
    ev_acc = next(e for e in batch_1 if e.business_key.get("account_id") == "ACC-0001")
    assert ev_acc.before_payload == acc_map["ACC-0001"]

    # B: SUB-0001
    ev_sub = next(e for e in batch_1 if e.business_key.get("subscription_id") == "SUB-0001")
    assert ev_sub.before_payload == sub_map["SUB-0001"]

    # C: INV-0001
    ev_inv = next(e for e in batch_1 if e.business_key.get("invoice_id") == "INV-0001")
    assert ev_inv.before_payload == inv_map["INV-0001"]

    # D: PAY-0001
    ev_pay = next(e for e in batch_1 if e.business_key.get("payment_id") == "PAY-0001")
    assert ev_pay.before_payload == pay_map["PAY-0001"]


def test_delete_before_image_derives_from_actual_source_snapshot(
    source_gen: SourceGenerator,
    cdc_gen: CDCScenarioGenerator,
):
    """Verify DELETE before-image matches actual PAY-0002 source snapshot record (Test E)."""
    raw_snapshot = source_gen.generate_snapshot_dicts()
    pay_map = {r["payment_id"]: r for r in raw_snapshot["payments"]}

    batch_2 = cdc_gen.generate_batch_2_advanced_cdc_scenarios("batch_002")
    del_ev = next(e for e in batch_2 if e.operation == CDCOperation.DELETE.value)

    assert del_ev.business_key["payment_id"] == "PAY-0002"
    assert del_ev.before_payload == pay_map["PAY-0002"]
    assert del_ev.payload is None


def test_updates_preserve_unchanged_columns(cdc_gen: CDCScenarioGenerator):
    """Verify that for every UPDATE, all untouched columns remain identical between before and after (Test F)."""
    all_events = cdc_gen.generate_batch_1_inserts_and_updates(
        "batch_001"
    ) + cdc_gen.generate_batch_2_advanced_cdc_scenarios("batch_002")
    updates = [e for e in all_events if e.operation == CDCOperation.UPDATE.value]

    for ev in updates:
        assert ev.before_payload is not None
        assert ev.payload is not None

        # Check ACC-0001: only status and updated_at change
        if ev.business_key.get("account_id") == "ACC-0001":
            for k in ["account_id", "account_name", "industry", "country", "created_at"]:
                assert ev.payload[k] == ev.before_payload[k], f"Field {k} unexpectedly changed"
            assert ev.payload["status"] != ev.before_payload["status"]

        # Check SUB-0001: only plan_name, monthly_amount, and updated_at change
        elif ev.business_key.get("subscription_id") == "SUB-0001":
            for k in [
                "subscription_id",
                "account_id",
                "billing_cycle",
                "status",
                "start_date",
                "end_date",
                "created_at",
            ]:
                assert ev.payload[k] == ev.before_payload[k], f"Field {k} unexpectedly changed"
            assert ev.payload["plan_name"] != ev.before_payload["plan_name"]
            assert ev.payload["monthly_amount"] != ev.before_payload["monthly_amount"]


def test_updates_advance_updated_at_timestamp(cdc_gen: CDCScenarioGenerator):
    """Verify that every UPDATE advances updated_at with a later timestamp (Test G)."""
    all_events = cdc_gen.generate_batch_1_inserts_and_updates(
        "batch_001"
    ) + cdc_gen.generate_batch_2_advanced_cdc_scenarios("batch_002")
    updates = [e for e in all_events if e.operation == CDCOperation.UPDATE.value]

    for ev in updates:
        assert ev.before_payload is not None
        assert ev.payload is not None
        assert ev.payload["updated_at"] != ev.before_payload["updated_at"]

        t_before = parse_iso_timestamp(ev.before_payload["updated_at"])
        t_after = parse_iso_timestamp(ev.payload["updated_at"])
        assert t_after > t_before, f"Expected {t_after} > {t_before} for event {ev.event_id}"


def test_out_of_order_logical_chain_internal_consistency(
    source_gen: SourceGenerator,
    cdc_gen: CDCScenarioGenerator,
):
    """Verify logical chain for ACC-0002: S0 -> Seq 101 -> Seq 102 (Test H)."""
    raw_snapshot = source_gen.generate_snapshot_dicts()
    acc_map = {r["account_id"]: r for r in raw_snapshot["accounts"]}

    batch_2 = cdc_gen.generate_batch_2_advanced_cdc_scenarios("batch_002")
    ev_101 = next(e for e in batch_2 if e.sequence_number == 101)
    ev_102 = next(e for e in batch_2 if e.sequence_number == 102)

    # seq101 before-image must equal baseline snapshot S0
    assert ev_101.before_payload == acc_map["ACC-0002"]

    # seq102 before-image must equal seq101 payload (after-image S1)
    assert ev_102.before_payload == ev_101.payload

    # Arrival order in Batch 2 list is intentionally out-of-order: seq 102 arrives before seq 101
    idx_102 = batch_2.index(ev_102)
    idx_101 = batch_2.index(ev_101)
    assert idx_102 < idx_101


def test_batch_3_quarantine_fixtures(cdc_gen: CDCScenarioGenerator):
    """Verify Batch 3 contains all intentional malformed/invalid event fixtures."""
    fixtures = cdc_gen.generate_batch_3_quarantine_fixtures("batch_003_quarantine")
    assert len(fixtures) == 7

    assert any(f.get("operation") == "TRUNCATE" for f in fixtures)
    assert any(f.get("business_key") == {} for f in fixtures)
    assert any(f.get("sequence_number") == -5 for f in fixtures)
    assert any("sequence_number" not in f for f in fixtures)
    assert any(f.get("table_name") == "" for f in fixtures)
    assert any(f.get("operation") == "DELETE" and f.get("before_payload") is None for f in fixtures)
