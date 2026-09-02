"""Unit tests for source table schemas and entity contracts."""

from datetime import UTC, date, datetime
from decimal import Decimal

from pyspark.sql.types import DateType, DecimalType, StringType, TimestampType

from src.source.schemas import (
    ACCOUNTS_SCHEMA,
    INVOICES_SCHEMA,
    PAYMENTS_SCHEMA,
    SUBSCRIPTIONS_SCHEMA,
    Subscription,
)


def test_accounts_schema_definition():
    """Verify PySpark StructType schema for accounts table."""
    fields = {f.name: f.dataType for f in ACCOUNTS_SCHEMA.fields}
    assert "account_id" in fields
    assert isinstance(fields["account_id"], StringType)
    assert isinstance(fields["created_at"], TimestampType)
    assert isinstance(fields["updated_at"], TimestampType)
    assert len(ACCOUNTS_SCHEMA.fields) == 7


def test_subscriptions_schema_currency_and_dates():
    """Verify subscriptions schema includes DecimalType(10, 2) and date types."""
    fields = {f.name: f.dataType for f in SUBSCRIPTIONS_SCHEMA.fields}
    assert isinstance(fields["monthly_amount"], DecimalType)
    assert fields["monthly_amount"].precision == 10
    assert fields["monthly_amount"].scale == 2
    assert isinstance(fields["start_date"], DateType)
    assert isinstance(fields["end_date"], DateType)


def test_invoices_schema_currency_and_dates():
    """Verify invoices schema includes DecimalType(10, 2) for invoice_amount."""
    fields = {f.name: f.dataType for f in INVOICES_SCHEMA.fields}
    assert isinstance(fields["invoice_amount"], DecimalType)
    assert fields["invoice_amount"].precision == 10
    assert fields["invoice_amount"].scale == 2
    assert isinstance(fields["invoice_date"], DateType)
    assert isinstance(fields["due_date"], DateType)


def test_payments_schema_currency_and_dates():
    """Verify payments schema includes DecimalType(10, 2) for payment_amount."""
    fields = {f.name: f.dataType for f in PAYMENTS_SCHEMA.fields}
    assert isinstance(fields["payment_amount"], DecimalType)
    assert fields["payment_amount"].precision == 10
    assert fields["payment_amount"].scale == 2
    assert isinstance(fields["payment_date"], DateType)


def test_dataclass_roundtrip_conversions():
    """Verify serialization and deserialization of entity dataclasses."""
    now = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
    today = date(2026, 1, 15)

    sub = Subscription(
        subscription_id="SUB-0099",
        account_id="ACC-0001",
        plan_name="GROWTH",
        billing_cycle="MONTHLY",
        monthly_amount=Decimal("199.50"),
        status="ACTIVE",
        start_date=today,
        end_date=None,
        created_at=now,
        updated_at=now,
    )

    sub_dict = sub.to_dict()
    assert sub_dict["monthly_amount"] == "199.50"
    assert sub_dict["start_date"] == "2026-01-15"
    assert sub_dict["end_date"] is None

    restored_sub = Subscription.from_dict(sub_dict)
    assert restored_sub.subscription_id == "SUB-0099"
    assert restored_sub.monthly_amount == Decimal("199.50")
    assert restored_sub.start_date == today
    assert restored_sub.end_date is None
