"""Deterministic CDC Scenario Generator.

Generates structured change event batches derived directly from the source snapshot state:
- Scenario A: Inserts (new account, subscription, invoice, payment)
- Scenario B: Updates (account status, subscription tier, invoice status, payment status)
- Scenario C: Deletes (valid delete event with before-image from source snapshot)
- Scenario D: Duplicate CDC events (exact duplicate event_id)
- Scenario E: Out-of-order arrival (seq 102 emitted before seq 101 with consistent source history)
- Scenario F: Late-arriving event (valid older event derived from source state)
- Scenario G: Invalid / Malformed events (quarantine fixtures including missing sequence)
"""

from copy import deepcopy
from datetime import timedelta
from typing import Any

from src.cdc.models import CDCEvent, CDCOperation
from src.source.generator import SourceGenerator
from src.utils.helpers import (
    format_iso_date,
    format_iso_timestamp,
)


class CDCScenarioGenerator:
    """Produces deterministic change batches for test suites and CDC simulation pipelines."""

    def __init__(self, source_generator: SourceGenerator | None = None) -> None:
        self.source_gen = source_generator or SourceGenerator()
        self.base_time = self.source_gen.config.base_timestamp + timedelta(days=130)
        self._init_snapshot_state()

    def _init_snapshot_state(self) -> None:
        """Generate and index initial snapshot records by table and primary key."""
        raw_snapshots = self.source_gen.generate_snapshot_dicts()
        self._accounts: dict[str, dict[str, Any]] = {
            r["account_id"]: deepcopy(r) for r in raw_snapshots["accounts"]
        }
        self._subscriptions: dict[str, dict[str, Any]] = {
            r["subscription_id"]: deepcopy(r) for r in raw_snapshots["subscriptions"]
        }
        self._invoices: dict[str, dict[str, Any]] = {
            r["invoice_id"]: deepcopy(r) for r in raw_snapshots["invoices"]
        }
        self._payments: dict[str, dict[str, Any]] = {
            r["payment_id"]: deepcopy(r) for r in raw_snapshots["payments"]
        }

    def generate_batch_1_inserts_and_updates(
        self,
        batch_id: str = "batch_001",
    ) -> list[CDCEvent]:
        """Generate Batch 1: Clean Inserts (Scenario A) and Updates (Scenario B)."""
        events: list[CDCEvent] = []
        t0 = self.base_time + timedelta(hours=1)

        # ----------------------------------------------------------------------
        # Scenario A: Inserts
        # ----------------------------------------------------------------------
        # 1. New Account: ACC-0041
        acc_ts = t0 + timedelta(minutes=5)
        events.append(
            CDCEvent(
                event_id="evt_ins_acc_0041",
                table_name="accounts",
                operation=CDCOperation.INSERT.value,
                business_key={"account_id": "ACC-0041"},
                sequence_number=1,
                event_timestamp=format_iso_timestamp(acc_ts),
                source_commit_timestamp=format_iso_timestamp(acc_ts + timedelta(seconds=1)),
                batch_id=batch_id,
                payload={
                    "account_id": "ACC-0041",
                    "account_name": "Apex Cloud Analytics",
                    "industry": "Fintech",
                    "country": "US",
                    "status": "ACTIVE",
                    "created_at": format_iso_timestamp(acc_ts),
                    "updated_at": format_iso_timestamp(acc_ts),
                },
                before_payload=None,
                source_system="b2b_saas_postgres",
            )
        )

        # 2. New Subscription: SUB-0061
        sub_ts = t0 + timedelta(minutes=10)
        events.append(
            CDCEvent(
                event_id="evt_ins_sub_0061",
                table_name="subscriptions",
                operation=CDCOperation.INSERT.value,
                business_key={"subscription_id": "SUB-0061"},
                sequence_number=1,
                event_timestamp=format_iso_timestamp(sub_ts),
                source_commit_timestamp=format_iso_timestamp(sub_ts + timedelta(seconds=1)),
                batch_id=batch_id,
                payload={
                    "subscription_id": "SUB-0061",
                    "account_id": "ACC-0041",
                    "plan_name": "GROWTH",
                    "billing_cycle": "MONTHLY",
                    "monthly_amount": "199.00",
                    "status": "ACTIVE",
                    "start_date": format_iso_date(sub_ts.date()),
                    "end_date": None,
                    "created_at": format_iso_timestamp(sub_ts),
                    "updated_at": format_iso_timestamp(sub_ts),
                },
                before_payload=None,
                source_system="b2b_saas_postgres",
            )
        )

        # 3. New Invoice: INV-0121
        inv_ts = t0 + timedelta(minutes=15)
        events.append(
            CDCEvent(
                event_id="evt_ins_inv_0121",
                table_name="invoices",
                operation=CDCOperation.INSERT.value,
                business_key={"invoice_id": "INV-0121"},
                sequence_number=1,
                event_timestamp=format_iso_timestamp(inv_ts),
                source_commit_timestamp=format_iso_timestamp(inv_ts + timedelta(seconds=2)),
                batch_id=batch_id,
                payload={
                    "invoice_id": "INV-0121",
                    "subscription_id": "SUB-0061",
                    "invoice_date": format_iso_date(inv_ts.date()),
                    "due_date": format_iso_date(inv_ts.date() + timedelta(days=15)),
                    "invoice_amount": "199.00",
                    "invoice_status": "ISSUED",
                    "created_at": format_iso_timestamp(inv_ts),
                    "updated_at": format_iso_timestamp(inv_ts),
                },
                before_payload=None,
                source_system="b2b_saas_postgres",
            )
        )

        # 4. New Payment: PAY-0091
        pay_ts = t0 + timedelta(minutes=20)
        events.append(
            CDCEvent(
                event_id="evt_ins_pay_0091",
                table_name="payments",
                operation=CDCOperation.INSERT.value,
                business_key={"payment_id": "PAY-0091"},
                sequence_number=1,
                event_timestamp=format_iso_timestamp(pay_ts),
                source_commit_timestamp=format_iso_timestamp(pay_ts + timedelta(seconds=1)),
                batch_id=batch_id,
                payload={
                    "payment_id": "PAY-0091",
                    "invoice_id": "INV-0121",
                    "payment_date": format_iso_date(pay_ts.date()),
                    "payment_amount": "199.00",
                    "payment_method": "CREDIT_CARD",
                    "payment_status": "SUCCESS",
                    "created_at": format_iso_timestamp(pay_ts),
                    "updated_at": format_iso_timestamp(pay_ts),
                },
                before_payload=None,
                source_system="b2b_saas_postgres",
            )
        )

        # ----------------------------------------------------------------------
        # Scenario B: Updates (Derived from real snapshot baseline)
        # ----------------------------------------------------------------------
        # 5. Account status change: ACC-0001 (Toggle status from baseline)
        acc_upd_ts = t0 + timedelta(minutes=30)
        acc_0001_before = deepcopy(self._accounts["ACC-0001"])
        acc_0001_after = deepcopy(acc_0001_before)
        acc_0001_after["status"] = (
            "ACTIVE" if acc_0001_before["status"] == "SUSPENDED" else "SUSPENDED"
        )
        acc_0001_after["updated_at"] = format_iso_timestamp(acc_upd_ts)

        events.append(
            CDCEvent(
                event_id="evt_upd_acc_0001",
                table_name="accounts",
                operation=CDCOperation.UPDATE.value,
                business_key={"account_id": "ACC-0001"},
                sequence_number=10,
                event_timestamp=format_iso_timestamp(acc_upd_ts),
                source_commit_timestamp=format_iso_timestamp(acc_upd_ts + timedelta(seconds=2)),
                batch_id=batch_id,
                before_payload=acc_0001_before,
                payload=acc_0001_after,
                source_system="b2b_saas_postgres",
            )
        )

        # 6. Subscription plan upgrade: SUB-0001 (STARTER -> ENTERPRISE)
        sub_upd_ts = t0 + timedelta(minutes=35)
        sub_0001_before = deepcopy(self._subscriptions["SUB-0001"])
        sub_0001_after = deepcopy(sub_0001_before)
        sub_0001_after["plan_name"] = "ENTERPRISE"
        sub_0001_after["monthly_amount"] = "1299.00"
        sub_0001_after["updated_at"] = format_iso_timestamp(sub_upd_ts)

        events.append(
            CDCEvent(
                event_id="evt_upd_sub_0001",
                table_name="subscriptions",
                operation=CDCOperation.UPDATE.value,
                business_key={"subscription_id": "SUB-0001"},
                sequence_number=15,
                event_timestamp=format_iso_timestamp(sub_upd_ts),
                source_commit_timestamp=format_iso_timestamp(sub_upd_ts + timedelta(seconds=1)),
                batch_id=batch_id,
                before_payload=sub_0001_before,
                payload=sub_0001_after,
                source_system="b2b_saas_postgres",
            )
        )

        # 7. Invoice status change: INV-0001 (Toggle status from baseline)
        inv_upd_ts = t0 + timedelta(minutes=40)
        inv_0001_before = deepcopy(self._invoices["INV-0001"])
        inv_0001_after = deepcopy(inv_0001_before)
        inv_0001_after["invoice_status"] = (
            "VOID" if inv_0001_before["invoice_status"] == "PAID" else "PAID"
        )
        inv_0001_after["updated_at"] = format_iso_timestamp(inv_upd_ts)

        events.append(
            CDCEvent(
                event_id="evt_upd_inv_0001",
                table_name="invoices",
                operation=CDCOperation.UPDATE.value,
                business_key={"invoice_id": "INV-0001"},
                sequence_number=20,
                event_timestamp=format_iso_timestamp(inv_upd_ts),
                source_commit_timestamp=format_iso_timestamp(inv_upd_ts + timedelta(seconds=3)),
                batch_id=batch_id,
                before_payload=inv_0001_before,
                payload=inv_0001_after,
                source_system="b2b_saas_postgres",
            )
        )

        # 8. Payment status change: PAY-0001 (SUCCESS -> REFUNDED)
        pay_upd_ts = t0 + timedelta(minutes=45)
        pay_0001_before = deepcopy(self._payments["PAY-0001"])
        pay_0001_after = deepcopy(pay_0001_before)
        pay_0001_after["payment_status"] = "REFUNDED"
        pay_0001_after["updated_at"] = format_iso_timestamp(pay_upd_ts)

        events.append(
            CDCEvent(
                event_id="evt_upd_pay_0001",
                table_name="payments",
                operation=CDCOperation.UPDATE.value,
                business_key={"payment_id": "PAY-0001"},
                sequence_number=25,
                event_timestamp=format_iso_timestamp(pay_upd_ts),
                source_commit_timestamp=format_iso_timestamp(pay_upd_ts + timedelta(seconds=1)),
                batch_id=batch_id,
                before_payload=pay_0001_before,
                payload=pay_0001_after,
                source_system="b2b_saas_postgres",
            )
        )

        return events

    def generate_batch_2_advanced_cdc_scenarios(
        self,
        batch_id: str = "batch_002",
    ) -> list[CDCEvent]:
        """Generate Batch 2: Deletes (C), Duplicates (D), Out-of-Order (E), Late Arrivals (F)."""
        events: list[CDCEvent] = []
        t1 = self.base_time + timedelta(days=2)

        # ----------------------------------------------------------------------
        # Scenario C: Valid Delete (Real source snapshot before-image)
        # ----------------------------------------------------------------------
        del_ts = t1 + timedelta(minutes=5)
        pay_0002_before = deepcopy(self._payments["PAY-0002"])

        events.append(
            CDCEvent(
                event_id="evt_del_pay_0002",
                table_name="payments",
                operation=CDCOperation.DELETE.value,
                business_key={"payment_id": "PAY-0002"},
                sequence_number=30,
                event_timestamp=format_iso_timestamp(del_ts),
                source_commit_timestamp=format_iso_timestamp(del_ts + timedelta(seconds=1)),
                batch_id=batch_id,
                before_payload=pay_0002_before,
                payload=None,
                source_system="b2b_saas_postgres",
            )
        )

        # ----------------------------------------------------------------------
        # Scenario D: Duplicate CDC Event
        # ----------------------------------------------------------------------
        # Re-emitting exact duplicate of evt_ins_acc_0041 from Batch 1
        dup_acc_ts = self.base_time + timedelta(hours=1, minutes=5)
        events.append(
            CDCEvent(
                event_id="evt_ins_acc_0041",  # Same event_id as in Batch 1
                table_name="accounts",
                operation=CDCOperation.INSERT.value,
                business_key={"account_id": "ACC-0041"},
                sequence_number=1,
                event_timestamp=format_iso_timestamp(dup_acc_ts),
                source_commit_timestamp=format_iso_timestamp(dup_acc_ts + timedelta(seconds=1)),
                batch_id=batch_id,
                payload={
                    "account_id": "ACC-0041",
                    "account_name": "Apex Cloud Analytics",
                    "industry": "Fintech",
                    "country": "US",
                    "status": "ACTIVE",
                    "created_at": format_iso_timestamp(dup_acc_ts),
                    "updated_at": format_iso_timestamp(dup_acc_ts),
                },
                before_payload=None,
                source_system="b2b_saas_postgres",
            )
        )

        # ----------------------------------------------------------------------
        # Scenario E: Out-of-Order Arrival with Internally Consistent Transaction History
        # Sequence 102 arrives before Sequence 101 for ACC-0002
        #
        # Transaction history:
        # Baseline (S0) -> Seq 101 (S1: country to GB) -> Seq 102 (S2: status to TRIAL, Inc)
        # ----------------------------------------------------------------------
        ooo_ts1 = t1 + timedelta(minutes=15)
        ooo_ts2 = t1 + timedelta(minutes=20)

        # S0: Baseline source snapshot state
        s0 = deepcopy(self._accounts["ACC-0002"])

        # S1: Logical state after sequence 101 (country update)
        s1 = deepcopy(s0)
        s1["country"] = "GB"
        s1["updated_at"] = format_iso_timestamp(ooo_ts1)

        # S2: Logical state after sequence 102 (status and legal name update)
        s2 = deepcopy(s1)
        s2["status"] = "TRIAL"
        s2["account_name"] = f"{s1['account_name']} Inc"
        s2["updated_at"] = format_iso_timestamp(ooo_ts2)

        ev_seq101 = CDCEvent(
            event_id="evt_ooo_acc_0002_seq101",
            table_name="accounts",
            operation=CDCOperation.UPDATE.value,
            business_key={"account_id": "ACC-0002"},
            sequence_number=101,
            event_timestamp=format_iso_timestamp(ooo_ts1),
            source_commit_timestamp=format_iso_timestamp(ooo_ts1 + timedelta(seconds=1)),
            batch_id=batch_id,
            before_payload=deepcopy(s0),
            payload=deepcopy(s1),
            source_system="b2b_saas_postgres",
        )

        ev_seq102 = CDCEvent(
            event_id="evt_ooo_acc_0002_seq102",
            table_name="accounts",
            operation=CDCOperation.UPDATE.value,
            business_key={"account_id": "ACC-0002"},
            sequence_number=102,
            event_timestamp=format_iso_timestamp(ooo_ts2),
            source_commit_timestamp=format_iso_timestamp(ooo_ts2 + timedelta(seconds=2)),
            batch_id=batch_id,
            before_payload=deepcopy(s1),
            payload=deepcopy(s2),
            source_system="b2b_saas_postgres",
        )

        # Intentionally append in out-of-order arrival: 102 before 101
        events.append(ev_seq102)
        events.append(ev_seq101)

        # ----------------------------------------------------------------------
        # Scenario F: Late-Arriving Event
        # A valid older event from past history appearing in this current batch
        # ----------------------------------------------------------------------
        late_ts = self.base_time - timedelta(days=5)
        sub_0002_before = deepcopy(self._subscriptions["SUB-0002"])
        sub_0002_after = deepcopy(sub_0002_before)
        sub_0002_after["billing_cycle"] = "ANNUAL"
        sub_0002_after["updated_at"] = format_iso_timestamp(late_ts)

        events.append(
            CDCEvent(
                event_id="evt_late_sub_0002",
                table_name="subscriptions",
                operation=CDCOperation.UPDATE.value,
                business_key={"subscription_id": "SUB-0002"},
                sequence_number=5,
                event_timestamp=format_iso_timestamp(late_ts),
                source_commit_timestamp=format_iso_timestamp(late_ts + timedelta(seconds=5)),
                batch_id=batch_id,
                before_payload=sub_0002_before,
                payload=sub_0002_after,
                source_system="b2b_saas_postgres",
            )
        )

        return events

    def generate_batch_3_quarantine_fixtures(
        self,
        batch_id: str = "batch_003_quarantine",
    ) -> list[dict[str, Any]]:
        """Generate Scenario G: Malformed and Invalid events for quarantine & validation testing."""
        t2 = self.base_time + timedelta(days=5)

        invalid_fixtures: list[dict[str, Any]] = [
            # 1. Missing business key
            {
                "event_id": "evt_inv_001_missing_pk",
                "table_name": "accounts",
                "operation": "INSERT",
                "business_key": {},
                "sequence_number": 1,
                "event_timestamp": format_iso_timestamp(t2),
                "source_commit_timestamp": format_iso_timestamp(t2),
                "batch_id": batch_id,
                "payload": {"account_id": "ACC-9999", "account_name": "Malformed Co"},
                "before_payload": None,
                "source_system": "b2b_saas_postgres",
            },
            # 2. Unsupported operation
            {
                "event_id": "evt_inv_002_unsupported_op",
                "table_name": "accounts",
                "operation": "TRUNCATE",
                "business_key": {"account_id": "ACC-0001"},
                "sequence_number": 100,
                "event_timestamp": format_iso_timestamp(t2),
                "source_commit_timestamp": format_iso_timestamp(t2),
                "batch_id": batch_id,
                "payload": None,
                "before_payload": None,
                "source_system": "b2b_saas_postgres",
            },
            # 3. Malformed payload: INSERT with null payload
            {
                "event_id": "evt_inv_003_null_insert_payload",
                "table_name": "subscriptions",
                "operation": "INSERT",
                "business_key": {"subscription_id": "SUB-9999"},
                "sequence_number": 1,
                "event_timestamp": format_iso_timestamp(t2),
                "source_commit_timestamp": format_iso_timestamp(t2),
                "batch_id": batch_id,
                "payload": None,
                "before_payload": None,
                "source_system": "b2b_saas_postgres",
            },
            # 4. Negative / non-positive sequence number
            {
                "event_id": "evt_inv_004_invalid_seq",
                "table_name": "invoices",
                "operation": "UPDATE",
                "business_key": {"invoice_id": "INV-0001"},
                "sequence_number": -5,
                "event_timestamp": format_iso_timestamp(t2),
                "source_commit_timestamp": format_iso_timestamp(t2),
                "batch_id": batch_id,
                "payload": {"invoice_id": "INV-0001", "invoice_status": "VOID"},
                "before_payload": {"invoice_id": "INV-0001", "invoice_status": "ISSUED"},
                "source_system": "b2b_saas_postgres",
            },
            # 5. Missing sequence number entirely
            {
                "event_id": "evt_inv_005_missing_seq_key",
                "table_name": "accounts",
                "operation": "INSERT",
                "business_key": {"account_id": "ACC-9998"},
                "event_timestamp": format_iso_timestamp(t2),
                "source_commit_timestamp": format_iso_timestamp(t2),
                "batch_id": batch_id,
                "payload": {"account_id": "ACC-9998", "account_name": "No Seq Co"},
                "before_payload": None,
                "source_system": "b2b_saas_postgres",
            },
            # 6. Missing source table name
            {
                "event_id": "evt_inv_006_missing_table",
                "table_name": "",
                "operation": "INSERT",
                "business_key": {"id": "1"},
                "sequence_number": 1,
                "event_timestamp": format_iso_timestamp(t2),
                "source_commit_timestamp": format_iso_timestamp(t2),
                "batch_id": batch_id,
                "payload": {"id": "1"},
                "before_payload": None,
                "source_system": "b2b_saas_postgres",
            },
            # 7. DELETE missing before_payload
            {
                "event_id": "evt_inv_007_delete_missing_before",
                "table_name": "payments",
                "operation": "DELETE",
                "business_key": {"payment_id": "PAY-0005"},
                "sequence_number": 50,
                "event_timestamp": format_iso_timestamp(t2),
                "source_commit_timestamp": format_iso_timestamp(t2),
                "batch_id": batch_id,
                "payload": None,
                "before_payload": None,
                "source_system": "b2b_saas_postgres",
            },
        ]

        return invalid_fixtures

    def generate_all_batches(self) -> dict[str, list[Any]]:
        """Generate all batches mapped by batch_id."""
        return {
            "batch_001": self.generate_batch_1_inserts_and_updates("batch_001"),
            "batch_002": self.generate_batch_2_advanced_cdc_scenarios("batch_002"),
            "batch_003_quarantine": self.generate_batch_3_quarantine_fixtures(
                "batch_003_quarantine"
            ),
        }
