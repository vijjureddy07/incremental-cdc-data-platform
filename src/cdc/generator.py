"""Deterministic CDC Scenario Generator.

Generates structured change event batches covering:
- Scenario A: Inserts (new account, subscription, invoice, payment)
- Scenario B: Updates (account status, subscription tier, invoice status, payment status)
- Scenario C: Deletes (valid delete event with before-image)
- Scenario D: Duplicate CDC events (exact duplicate event_id)
- Scenario E: Out-of-order arrival (seq 102 emitted before seq 101)
- Scenario F: Late-arriving event (valid older event emitted in a later batch)
- Scenario G: Invalid / Malformed events (quarantine fixtures)
"""

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
        self.base_time = self.source_gen.config.base_timestamp + timedelta(days=90)

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
        # Scenario B: Updates
        # ----------------------------------------------------------------------
        # 5. Account status change: ACC-0001 (ACTIVE -> SUSPENDED)
        acc_upd_ts = t0 + timedelta(minutes=30)
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
                before_payload={
                    "account_id": "ACC-0001",
                    "account_name": "Fintech Solutions 1",
                    "industry": "Fintech",
                    "country": "US",
                    "status": "ACTIVE",
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:00Z",
                },
                payload={
                    "account_id": "ACC-0001",
                    "account_name": "Fintech Solutions 1",
                    "industry": "Fintech",
                    "country": "US",
                    "status": "SUSPENDED",
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": format_iso_timestamp(acc_upd_ts),
                },
                source_system="b2b_saas_postgres",
            )
        )

        # 6. Subscription plan upgrade: SUB-0001 (STARTER -> ENTERPRISE)
        sub_upd_ts = t0 + timedelta(minutes=35)
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
                before_payload={
                    "subscription_id": "SUB-0001",
                    "account_id": "ACC-0001",
                    "plan_name": "STARTER",
                    "billing_cycle": "MONTHLY",
                    "monthly_amount": "49.00",
                    "status": "ACTIVE",
                    "start_date": "2026-01-10",
                    "end_date": None,
                    "created_at": "2026-01-10T00:00:00Z",
                    "updated_at": "2026-01-10T00:00:00Z",
                },
                payload={
                    "subscription_id": "SUB-0001",
                    "account_id": "ACC-0001",
                    "plan_name": "ENTERPRISE",
                    "billing_cycle": "MONTHLY",
                    "monthly_amount": "1299.00",
                    "status": "ACTIVE",
                    "start_date": "2026-01-10",
                    "end_date": None,
                    "created_at": "2026-01-10T00:00:00Z",
                    "updated_at": format_iso_timestamp(sub_upd_ts),
                },
                source_system="b2b_saas_postgres",
            )
        )

        # 7. Invoice status change: INV-0001 (ISSUED -> PAID)
        inv_upd_ts = t0 + timedelta(minutes=40)
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
                before_payload={
                    "invoice_id": "INV-0001",
                    "subscription_id": "SUB-0001",
                    "invoice_date": "2026-01-10",
                    "due_date": "2026-01-25",
                    "invoice_amount": "49.00",
                    "invoice_status": "ISSUED",
                    "created_at": "2026-01-10T00:00:00Z",
                    "updated_at": "2026-01-10T00:00:00Z",
                },
                payload={
                    "invoice_id": "INV-0001",
                    "subscription_id": "SUB-0001",
                    "invoice_date": "2026-01-10",
                    "due_date": "2026-01-25",
                    "invoice_amount": "49.00",
                    "invoice_status": "PAID",
                    "created_at": "2026-01-10T00:00:00Z",
                    "updated_at": format_iso_timestamp(inv_upd_ts),
                },
                source_system="b2b_saas_postgres",
            )
        )

        # 8. Payment status change: PAY-0001 (SUCCESS -> REFUNDED)
        pay_upd_ts = t0 + timedelta(minutes=45)
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
                before_payload={
                    "payment_id": "PAY-0001",
                    "invoice_id": "INV-0001",
                    "payment_date": "2026-01-12",
                    "payment_amount": "49.00",
                    "payment_method": "CREDIT_CARD",
                    "payment_status": "SUCCESS",
                    "created_at": "2026-01-12T00:00:00Z",
                    "updated_at": "2026-01-12T00:00:00Z",
                },
                payload={
                    "payment_id": "PAY-0001",
                    "invoice_id": "INV-0001",
                    "payment_date": "2026-01-12",
                    "payment_amount": "49.00",
                    "payment_method": "CREDIT_CARD",
                    "payment_status": "REFUNDED",
                    "created_at": "2026-01-12T00:00:00Z",
                    "updated_at": format_iso_timestamp(pay_upd_ts),
                },
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
        # Scenario C: Valid Delete
        # ----------------------------------------------------------------------
        # Delete Payment PAY-0002
        del_ts = t1 + timedelta(minutes=5)
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
                before_payload={
                    "payment_id": "PAY-0002",
                    "invoice_id": "INV-0002",
                    "payment_date": "2026-01-15",
                    "payment_amount": "199.00",
                    "payment_method": "ACH",
                    "payment_status": "FAILED",
                    "created_at": "2026-01-15T00:00:00Z",
                    "updated_at": "2026-01-15T00:00:00Z",
                },
                payload=None,
                source_system="b2b_saas_postgres",
            )
        )

        # ----------------------------------------------------------------------
        # Scenario D: Duplicate CDC Event
        # ----------------------------------------------------------------------
        # Re-emitting exact duplicate of evt_ins_acc_0041
        events.append(
            CDCEvent(
                event_id="evt_ins_acc_0041",  # Same event_id as in Batch 1
                table_name="accounts",
                operation=CDCOperation.INSERT.value,
                business_key={"account_id": "ACC-0041"},
                sequence_number=1,
                event_timestamp="2026-04-01T01:05:00Z",
                source_commit_timestamp="2026-04-01T01:05:01Z",
                batch_id=batch_id,
                payload={
                    "account_id": "ACC-0041",
                    "account_name": "Apex Cloud Analytics",
                    "industry": "Fintech",
                    "country": "US",
                    "status": "ACTIVE",
                    "created_at": "2026-04-01T01:05:00Z",
                    "updated_at": "2026-04-01T01:05:00Z",
                },
                before_payload=None,
                source_system="b2b_saas_postgres",
            )
        )

        # ----------------------------------------------------------------------
        # Scenario E: Out-of-Order Arrival
        # Sequence 102 emitted BEFORE Sequence 101 for ACC-0002
        # ----------------------------------------------------------------------
        # First write: sequence 102 (later state: TRIAL -> ACTIVE)
        ooo_ts2 = t1 + timedelta(minutes=20)
        events.append(
            CDCEvent(
                event_id="evt_ooo_acc_0002_seq102",
                table_name="accounts",
                operation=CDCOperation.UPDATE.value,
                business_key={"account_id": "ACC-0002"},
                sequence_number=102,
                event_timestamp=format_iso_timestamp(ooo_ts2),
                source_commit_timestamp=format_iso_timestamp(ooo_ts2 + timedelta(seconds=2)),
                batch_id=batch_id,
                before_payload={
                    "account_id": "ACC-0002",
                    "account_name": "Healthcare Solutions 2",
                    "industry": "Healthcare",
                    "country": "CA",
                    "status": "TRIAL",
                    "created_at": "2026-01-02T00:00:00Z",
                    "updated_at": "2026-01-02T00:00:00Z",
                },
                payload={
                    "account_id": "ACC-0002",
                    "account_name": "Healthcare Solutions 2 Inc",
                    "industry": "Healthcare",
                    "country": "CA",
                    "status": "ACTIVE",
                    "created_at": "2026-01-02T00:00:00Z",
                    "updated_at": format_iso_timestamp(ooo_ts2),
                },
                source_system="b2b_saas_postgres",
            )
        )

        # Second write: sequence 101 (intermediate state: country update)
        ooo_ts1 = t1 + timedelta(minutes=15)
        events.append(
            CDCEvent(
                event_id="evt_ooo_acc_0002_seq101",
                table_name="accounts",
                operation=CDCOperation.UPDATE.value,
                business_key={"account_id": "ACC-0002"},
                sequence_number=101,
                event_timestamp=format_iso_timestamp(ooo_ts1),
                source_commit_timestamp=format_iso_timestamp(ooo_ts1 + timedelta(seconds=1)),
                batch_id=batch_id,
                before_payload={
                    "account_id": "ACC-0002",
                    "account_name": "Healthcare Solutions 2",
                    "industry": "Healthcare",
                    "country": "US",
                    "status": "TRIAL",
                    "created_at": "2026-01-02T00:00:00Z",
                    "updated_at": "2026-01-02T00:00:00Z",
                },
                payload={
                    "account_id": "ACC-0002",
                    "account_name": "Healthcare Solutions 2",
                    "industry": "Healthcare",
                    "country": "CA",
                    "status": "TRIAL",
                    "created_at": "2026-01-02T00:00:00Z",
                    "updated_at": format_iso_timestamp(ooo_ts1),
                },
                source_system="b2b_saas_postgres",
            )
        )

        # ----------------------------------------------------------------------
        # Scenario F: Late-Arriving Event
        # A valid older event from 5 days ago appearing in this current batch
        # ----------------------------------------------------------------------
        late_ts = self.base_time - timedelta(days=5)
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
                before_payload={
                    "subscription_id": "SUB-0002",
                    "account_id": "ACC-0002",
                    "plan_name": "GROWTH",
                    "billing_cycle": "MONTHLY",
                    "monthly_amount": "199.00",
                    "status": "ACTIVE",
                    "start_date": "2026-01-15",
                    "end_date": None,
                    "created_at": "2026-01-15T00:00:00Z",
                    "updated_at": "2026-01-15T00:00:00Z",
                },
                payload={
                    "subscription_id": "SUB-0002",
                    "account_id": "ACC-0002",
                    "plan_name": "GROWTH",
                    "billing_cycle": "ANNUAL",
                    "monthly_amount": "199.00",
                    "status": "ACTIVE",
                    "start_date": "2026-01-15",
                    "end_date": None,
                    "created_at": "2026-01-15T00:00:00Z",
                    "updated_at": format_iso_timestamp(late_ts),
                },
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
            # 5. Missing source table name
            {
                "event_id": "evt_inv_005_missing_table",
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
            # 6. DELETE missing before_payload
            {
                "event_id": "evt_inv_006_delete_missing_before",
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
            "batch_003_quarantine": self.generate_batch_3_quarantine_fixtures("batch_003_quarantine"),
        }
