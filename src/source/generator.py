"""Deterministic Source Data Generator for initial B2B SaaS snapshot.

Produces consistent synthetic source datasets for:
- accounts
- subscriptions
- invoices
- payments

Maintains strict referential integrity and exports to local Parquet files.
"""

import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from src.source.schemas import (
    Account,
    Invoice,
    Payment,
    Subscription,
)
from src.utils.helpers import ensure_dir

# Deterministic Seed Categories
INDUSTRIES = [
    "Fintech",
    "Healthcare",
    "E-commerce",
    "SaaS",
    "Cybersecurity",
    "Logistics",
    "Edtech",
    "Manufacturing",
]

COUNTRIES = ["US", "CA", "GB", "DE", "FR", "AU", "SG", "IN", "JP", "BR"]

ACCOUNT_STATUSES = ["ACTIVE", "ACTIVE", "ACTIVE", "ACTIVE", "TRIAL", "SUSPENDED"]

PLAN_TIERS = [
    ("STARTER", Decimal("49.00")),
    ("GROWTH", Decimal("199.00")),
    ("SCALE", Decimal("499.00")),
    ("ENTERPRISE", Decimal("1299.00")),
]

BILLING_CYCLES = ["MONTHLY", "MONTHLY", "MONTHLY", "ANNUAL"]

SUBSCRIPTION_STATUSES = ["ACTIVE", "ACTIVE", "ACTIVE", "PAUSED", "CANCELLED"]

INVOICE_STATUSES = ["PAID", "PAID", "PAID", "ISSUED", "OVERDUE"]

PAYMENT_METHODS = ["CREDIT_CARD", "CREDIT_CARD", "ACH", "WIRE_TRANSFER"]

PAYMENT_STATUSES = ["SUCCESS", "SUCCESS", "SUCCESS", "FAILED"]


@dataclass
class SnapshotConfig:
    """Configuration parameters for deterministic initial snapshot generation."""

    num_accounts: int = 40
    num_subscriptions: int = 60
    num_invoices: int = 120
    num_payments: int = 90
    seed: int = 42
    base_timestamp: datetime = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)


class SourceGenerator:
    """Generates deterministic initial snapshot datasets for B2B SaaS entities."""

    def __init__(self, config: SnapshotConfig | None = None) -> None:
        self.config = config or SnapshotConfig()
        self.rng = random.Random(self.config.seed)

    def generate_accounts(self) -> list[Account]:
        """Generate deterministic accounts."""
        accounts: list[Account] = []
        base_ts = self.config.base_timestamp

        for i in range(1, self.config.num_accounts + 1):
            acc_id = f"ACC-{i:04d}"
            industry = INDUSTRIES[(i - 1) % len(INDUSTRIES)]
            country = COUNTRIES[(i - 1) % len(COUNTRIES)]
            status = ACCOUNT_STATUSES[self.rng.randint(0, len(ACCOUNT_STATUSES) - 1)]

            created_delta = timedelta(days=self.rng.randint(0, 60), hours=self.rng.randint(0, 23))
            created_at = base_ts + created_delta
            updated_at = created_at + timedelta(hours=self.rng.randint(0, 12))

            accounts.append(
                Account(
                    account_id=acc_id,
                    account_name=f"{industry} Solutions {i}",
                    industry=industry,
                    country=country,
                    status=status,
                    created_at=created_at,
                    updated_at=updated_at,
                )
            )
        return accounts

    def generate_subscriptions(self, accounts: list[Account]) -> list[Subscription]:
        """Generate deterministic subscriptions referencing existing accounts."""
        subscriptions: list[Subscription] = []
        acc_ids = [acc.account_id for acc in accounts]

        for i in range(1, self.config.num_subscriptions + 1):
            sub_id = f"SUB-{i:04d}"
            acc_id = acc_ids[(i - 1) % len(acc_ids)]
            plan_name, monthly_amount = PLAN_TIERS[self.rng.randint(0, len(PLAN_TIERS) - 1)]
            billing_cycle = BILLING_CYCLES[self.rng.randint(0, len(BILLING_CYCLES) - 1)]
            status = SUBSCRIPTION_STATUSES[self.rng.randint(0, len(SUBSCRIPTION_STATUSES) - 1)]

            start_date_obj = (
                self.config.base_timestamp + timedelta(days=self.rng.randint(5, 75))
            ).date()
            end_date_obj = start_date_obj + timedelta(days=365) if status == "CANCELLED" else None

            created_at = datetime.combine(start_date_obj, datetime.min.time(), tzinfo=UTC)
            updated_at = created_at + timedelta(hours=self.rng.randint(0, 24))

            subscriptions.append(
                Subscription(
                    subscription_id=sub_id,
                    account_id=acc_id,
                    plan_name=plan_name,
                    billing_cycle=billing_cycle,
                    monthly_amount=monthly_amount,
                    status=status,
                    start_date=start_date_obj,
                    end_date=end_date_obj,
                    created_at=created_at,
                    updated_at=updated_at,
                )
            )
        return subscriptions

    def generate_invoices(self, subscriptions: list[Subscription]) -> list[Invoice]:
        """Generate deterministic invoices referencing existing subscriptions."""
        invoices: list[Invoice] = []
        sub_map = {sub.subscription_id: sub for sub in subscriptions}
        sub_ids = list(sub_map.keys())

        for i in range(1, self.config.num_invoices + 1):
            inv_id = f"INV-{i:04d}"
            sub_id = sub_ids[(i - 1) % len(sub_ids)]
            parent_sub = sub_map[sub_id]

            inv_date = parent_sub.start_date + timedelta(days=((i - 1) // len(sub_ids)) * 30)
            due_date = inv_date + timedelta(days=15)
            inv_amount = (
                parent_sub.monthly_amount * Decimal("12.00")
                if parent_sub.billing_cycle == "ANNUAL"
                else parent_sub.monthly_amount
            )
            inv_status = INVOICE_STATUSES[self.rng.randint(0, len(INVOICE_STATUSES) - 1)]

            created_at = datetime.combine(inv_date, datetime.min.time(), tzinfo=UTC)
            updated_at = created_at + timedelta(days=self.rng.randint(1, 10))

            invoices.append(
                Invoice(
                    invoice_id=inv_id,
                    subscription_id=sub_id,
                    invoice_date=inv_date,
                    due_date=due_date,
                    invoice_amount=inv_amount,
                    invoice_status=inv_status,
                    created_at=created_at,
                    updated_at=updated_at,
                )
            )
        return invoices

    def generate_payments(self, invoices: list[Invoice]) -> list[Payment]:
        """Generate deterministic payments referencing existing invoices."""
        payments: list[Payment] = []
        inv_map = {inv.invoice_id: inv for inv in invoices}
        inv_ids = list(inv_map.keys())

        for i in range(1, self.config.num_payments + 1):
            pay_id = f"PAY-{i:04d}"
            inv_id = inv_ids[(i - 1) % len(inv_ids)]
            parent_inv = inv_map[inv_id]

            pay_date = parent_inv.invoice_date + timedelta(days=self.rng.randint(1, 14))
            pay_amount = parent_inv.invoice_amount
            pay_method = PAYMENT_METHODS[self.rng.randint(0, len(PAYMENT_METHODS) - 1)]
            pay_status = (
                "SUCCESS"
                if parent_inv.invoice_status == "PAID"
                else PAYMENT_STATUSES[self.rng.randint(0, len(PAYMENT_STATUSES) - 1)]
            )

            created_at = datetime.combine(pay_date, datetime.min.time(), tzinfo=UTC)
            updated_at = created_at + timedelta(hours=self.rng.randint(0, 6))

            payments.append(
                Payment(
                    payment_id=pay_id,
                    invoice_id=inv_id,
                    payment_date=pay_date,
                    payment_amount=pay_amount,
                    payment_method=pay_method,
                    payment_status=pay_status,
                    created_at=created_at,
                    updated_at=updated_at,
                )
            )
        return payments

    def generate_snapshot_entities(
        self,
    ) -> tuple[list[Account], list[Subscription], list[Invoice], list[Payment]]:
        """Generate all initial snapshot entity collections."""
        # Re-initialize RNG to guarantee exact determinism on every run
        self.rng = random.Random(self.config.seed)
        accounts = self.generate_accounts()
        subscriptions = self.generate_subscriptions(accounts)
        invoices = self.generate_invoices(subscriptions)
        payments = self.generate_payments(invoices)
        return accounts, subscriptions, invoices, payments

    def generate_snapshot_dicts(self) -> dict[str, list[dict[str, Any]]]:
        """Generate initial snapshot as dictionary mapping table name to record dicts."""
        accounts, subscriptions, invoices, payments = self.generate_snapshot_entities()
        return {
            "accounts": [acc.to_dict() for acc in accounts],
            "subscriptions": [sub.to_dict() for sub in subscriptions],
            "invoices": [inv.to_dict() for inv in invoices],
            "payments": [pay.to_dict() for pay in payments],
        }

    def persist_snapshot_parquet(
        self,
        base_dir: Path | str = "data/source_snapshot",
    ) -> dict[str, Path]:
        """Persist snapshot tables to Parquet files using PyArrow.

        Directory structure:
        base_dir/accounts/snapshot.parquet
        base_dir/subscriptions/snapshot.parquet
        base_dir/invoices/snapshot.parquet
        base_dir/payments/snapshot.parquet
        """
        base_path = Path(base_dir)
        accounts, subscriptions, invoices, payments = self.generate_snapshot_entities()

        output_paths: dict[str, Path] = {}

        # 1. Accounts
        acc_dir = ensure_dir(base_path / "accounts")
        acc_path = acc_dir / "snapshot.parquet"
        acc_table = pa.Table.from_arrays(
            [
                pa.array([a.account_id for a in accounts], type=pa.string()),
                pa.array([a.account_name for a in accounts], type=pa.string()),
                pa.array([a.industry for a in accounts], type=pa.string()),
                pa.array([a.country for a in accounts], type=pa.string()),
                pa.array([a.status for a in accounts], type=pa.string()),
                pa.array([a.created_at for a in accounts], type=pa.timestamp("us", tz="UTC")),
                pa.array([a.updated_at for a in accounts], type=pa.timestamp("us", tz="UTC")),
            ],
            names=[
                "account_id",
                "account_name",
                "industry",
                "country",
                "status",
                "created_at",
                "updated_at",
            ],
        )
        pq.write_table(acc_table, acc_path)
        output_paths["accounts"] = acc_path

        # 2. Subscriptions
        sub_dir = ensure_dir(base_path / "subscriptions")
        sub_path = sub_dir / "snapshot.parquet"
        sub_table = pa.Table.from_arrays(
            [
                pa.array([s.subscription_id for s in subscriptions], type=pa.string()),
                pa.array([s.account_id for s in subscriptions], type=pa.string()),
                pa.array([s.plan_name for s in subscriptions], type=pa.string()),
                pa.array([s.billing_cycle for s in subscriptions], type=pa.string()),
                pa.array(
                    [s.monthly_amount for s in subscriptions],
                    type=pa.decimal128(10, 2),
                ),
                pa.array([s.status for s in subscriptions], type=pa.string()),
                pa.array([s.start_date for s in subscriptions], type=pa.date32()),
                pa.array([s.end_date for s in subscriptions], type=pa.date32()),
                pa.array([s.created_at for s in subscriptions], type=pa.timestamp("us", tz="UTC")),
                pa.array([s.updated_at for s in subscriptions], type=pa.timestamp("us", tz="UTC")),
            ],
            names=[
                "subscription_id",
                "account_id",
                "plan_name",
                "billing_cycle",
                "monthly_amount",
                "status",
                "start_date",
                "end_date",
                "created_at",
                "updated_at",
            ],
        )
        pq.write_table(sub_table, sub_path)
        output_paths["subscriptions"] = sub_path

        # 3. Invoices
        inv_dir = ensure_dir(base_path / "invoices")
        inv_path = inv_dir / "snapshot.parquet"
        inv_table = pa.Table.from_arrays(
            [
                pa.array([i.invoice_id for i in invoices], type=pa.string()),
                pa.array([i.subscription_id for i in invoices], type=pa.string()),
                pa.array([i.invoice_date for i in invoices], type=pa.date32()),
                pa.array([i.due_date for i in invoices], type=pa.date32()),
                pa.array(
                    [i.invoice_amount for i in invoices],
                    type=pa.decimal128(10, 2),
                ),
                pa.array([i.invoice_status for i in invoices], type=pa.string()),
                pa.array([i.created_at for i in invoices], type=pa.timestamp("us", tz="UTC")),
                pa.array([i.updated_at for i in invoices], type=pa.timestamp("us", tz="UTC")),
            ],
            names=[
                "invoice_id",
                "subscription_id",
                "invoice_date",
                "due_date",
                "invoice_amount",
                "invoice_status",
                "created_at",
                "updated_at",
            ],
        )
        pq.write_table(inv_table, inv_path)
        output_paths["invoices"] = inv_path

        # 4. Payments
        pay_dir = ensure_dir(base_path / "payments")
        pay_path = pay_dir / "snapshot.parquet"
        pay_table = pa.Table.from_arrays(
            [
                pa.array([p.payment_id for p in payments], type=pa.string()),
                pa.array([p.invoice_id for p in payments], type=pa.string()),
                pa.array([p.payment_date for p in payments], type=pa.date32()),
                pa.array(
                    [p.payment_amount for p in payments],
                    type=pa.decimal128(10, 2),
                ),
                pa.array([p.payment_method for p in payments], type=pa.string()),
                pa.array([p.payment_status for p in payments], type=pa.string()),
                pa.array([p.created_at for p in payments], type=pa.timestamp("us", tz="UTC")),
                pa.array([p.updated_at for p in payments], type=pa.timestamp("us", tz="UTC")),
            ],
            names=[
                "payment_id",
                "invoice_id",
                "payment_date",
                "payment_amount",
                "payment_method",
                "payment_status",
                "created_at",
                "updated_at",
            ],
        )
        pq.write_table(pay_table, pay_path)
        output_paths["payments"] = pay_path

        return output_paths
