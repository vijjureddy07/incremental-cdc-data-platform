"""Unit tests for deterministic source generator and initial snapshot."""

import tempfile
from decimal import Decimal

import pyarrow.parquet as pq

from src.source.generator import SnapshotConfig, SourceGenerator


def test_deterministic_source_generation_and_seed_repeatability():
    """Verify that multiple runs with the same seed generate identical datasets."""
    gen1 = SourceGenerator(SnapshotConfig(seed=42))
    snap1 = gen1.generate_snapshot_dicts()

    gen2 = SourceGenerator(SnapshotConfig(seed=42))
    snap2 = gen2.generate_snapshot_dicts()

    assert snap1 == snap2


def test_exact_source_row_counts(initial_snapshot: dict[str, list[dict]]):
    """Verify exact row counts: 40 accounts, 60 subscriptions, 120 invoices, 90 payments."""
    assert len(initial_snapshot["accounts"]) == 40
    assert len(initial_snapshot["subscriptions"]) == 60
    assert len(initial_snapshot["invoices"]) == 120
    assert len(initial_snapshot["payments"]) == 90


def test_referential_relationships(initial_snapshot: dict[str, list[dict]]):
    """Verify all foreign keys point to valid existing parent records."""
    account_ids = {acc["account_id"] for acc in initial_snapshot["accounts"]}
    subscription_ids = {sub["subscription_id"] for sub in initial_snapshot["subscriptions"]}
    invoice_ids = {inv["invoice_id"] for inv in initial_snapshot["invoices"]}

    # Subscriptions -> Accounts
    for sub in initial_snapshot["subscriptions"]:
        assert sub["account_id"] in account_ids, f"Orphan subscription: {sub}"

    # Invoices -> Subscriptions
    for inv in initial_snapshot["invoices"]:
        assert inv["subscription_id"] in subscription_ids, f"Orphan invoice: {inv}"

    # Payments -> Invoices
    for pay in initial_snapshot["payments"]:
        assert pay["invoice_id"] in invoice_ids, f"Orphan payment: {pay}"


def test_decimal_currency_fields(source_gen: SourceGenerator):
    """Verify all monetary fields are instances of Decimal with valid scales."""
    _, subs, invs, pays = source_gen.generate_snapshot_entities()

    for s in subs:
        assert isinstance(s.monthly_amount, Decimal)
        assert s.monthly_amount > Decimal("0.00")

    for i in invs:
        assert isinstance(i.invoice_amount, Decimal)
        assert i.invoice_amount > Decimal("0.00")

    for p in pays:
        assert isinstance(p.payment_amount, Decimal)
        assert p.payment_amount > Decimal("0.00")


def test_parquet_snapshot_export(source_gen: SourceGenerator):
    """Verify Parquet snapshot files can be written and read back via PyArrow."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        paths = source_gen.persist_snapshot_parquet(base_dir=tmp_dir)

        assert "accounts" in paths
        assert "subscriptions" in paths
        assert "invoices" in paths
        assert "payments" in paths

        # Read back accounts parquet
        acc_table = pq.read_table(paths["accounts"])
        assert acc_table.num_rows == 40
        assert "account_id" in acc_table.column_names
        assert "updated_at" in acc_table.column_names
