"""Source table schemas and entity data models.

Defines PySpark StructType schemas, primary keys, relationships,
and typed Python dataclasses for all B2B SaaS entities.
"""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.utils.helpers import (
    format_decimal,
    format_iso_date,
    format_iso_timestamp,
    parse_iso_date,
    parse_iso_timestamp,
)

# ==============================================================================
# PySpark Schema Contracts
# ==============================================================================

ACCOUNTS_SCHEMA = StructType(
    [
        StructField("account_id", StringType(), False),
        StructField("account_name", StringType(), False),
        StructField("industry", StringType(), False),
        StructField("country", StringType(), False),
        StructField("status", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)

SUBSCRIPTIONS_SCHEMA = StructType(
    [
        StructField("subscription_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("plan_name", StringType(), False),
        StructField("billing_cycle", StringType(), False),
        StructField("monthly_amount", DecimalType(10, 2), False),
        StructField("status", StringType(), False),
        StructField("start_date", DateType(), False),
        StructField("end_date", DateType(), True),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)

INVOICES_SCHEMA = StructType(
    [
        StructField("invoice_id", StringType(), False),
        StructField("subscription_id", StringType(), False),
        StructField("invoice_date", DateType(), False),
        StructField("due_date", DateType(), False),
        StructField("invoice_amount", DecimalType(10, 2), False),
        StructField("invoice_status", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)

PAYMENTS_SCHEMA = StructType(
    [
        StructField("payment_id", StringType(), False),
        StructField("invoice_id", StringType(), False),
        StructField("payment_date", DateType(), False),
        StructField("payment_amount", DecimalType(10, 2), False),
        StructField("payment_method", StringType(), False),
        StructField("payment_status", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)

TABLE_SCHEMAS_MAP: dict[str, StructType] = {
    "accounts": ACCOUNTS_SCHEMA,
    "subscriptions": SUBSCRIPTIONS_SCHEMA,
    "invoices": INVOICES_SCHEMA,
    "payments": PAYMENTS_SCHEMA,
}

TABLE_PRIMARY_KEYS: dict[str, str] = {
    "accounts": "account_id",
    "subscriptions": "subscription_id",
    "invoices": "invoice_id",
    "payments": "payment_id",
}

# ==============================================================================
# Strongly Typed Dataclasses
# ==============================================================================


@dataclass
class Account:
    """Represents a B2B customer account."""

    account_id: str
    account_name: str
    industry: str
    country: str
    status: str
    created_at: datetime
    updated_at: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert account dataclass to dictionary with serializable values."""
        return {
            "account_id": self.account_id,
            "account_name": self.account_name,
            "industry": self.industry,
            "country": self.country,
            "status": self.status,
            "created_at": format_iso_timestamp(self.created_at),
            "updated_at": format_iso_timestamp(self.updated_at),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Account":
        """Instantiate Account from a dictionary."""
        return cls(
            account_id=str(data["account_id"]),
            account_name=str(data["account_name"]),
            industry=str(data["industry"]),
            country=str(data["country"]),
            status=str(data["status"]),
            created_at=(
                parse_iso_timestamp(data["created_at"])
                if isinstance(data["created_at"], str)
                else data["created_at"]
            ),
            updated_at=(
                parse_iso_timestamp(data["updated_at"])
                if isinstance(data["updated_at"], str)
                else data["updated_at"]
            ),
        )


@dataclass
class Subscription:
    """Represents a SaaS subscription tier and billing cycle."""

    subscription_id: str
    account_id: str
    plan_name: str
    billing_cycle: str
    monthly_amount: Decimal
    status: str
    start_date: date
    end_date: date | None
    created_at: datetime
    updated_at: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert subscription dataclass to dictionary with serializable values."""
        return {
            "subscription_id": self.subscription_id,
            "account_id": self.account_id,
            "plan_name": self.plan_name,
            "billing_cycle": self.billing_cycle,
            "monthly_amount": format_decimal(self.monthly_amount, 2),
            "status": self.status,
            "start_date": format_iso_date(self.start_date),
            "end_date": format_iso_date(self.end_date) if self.end_date else None,
            "created_at": format_iso_timestamp(self.created_at),
            "updated_at": format_iso_timestamp(self.updated_at),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Subscription":
        """Instantiate Subscription from a dictionary."""
        return cls(
            subscription_id=str(data["subscription_id"]),
            account_id=str(data["account_id"]),
            plan_name=str(data["plan_name"]),
            billing_cycle=str(data["billing_cycle"]),
            monthly_amount=Decimal(str(data["monthly_amount"])),
            status=str(data["status"]),
            start_date=(
                parse_iso_date(data["start_date"])
                if isinstance(data["start_date"], str)
                else data["start_date"]
            ),
            end_date=(
                parse_iso_date(data["end_date"])
                if isinstance(data["end_date"], str) and data["end_date"]
                else data.get("end_date")
            ),
            created_at=(
                parse_iso_timestamp(data["created_at"])
                if isinstance(data["created_at"], str)
                else data["created_at"]
            ),
            updated_at=(
                parse_iso_timestamp(data["updated_at"])
                if isinstance(data["updated_at"], str)
                else data["updated_at"]
            ),
        )


@dataclass
class Invoice:
    """Represents a periodic billing invoice."""

    invoice_id: str
    subscription_id: str
    invoice_date: date
    due_date: date
    invoice_amount: Decimal
    invoice_status: str
    created_at: datetime
    updated_at: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert invoice dataclass to dictionary with serializable values."""
        return {
            "invoice_id": self.invoice_id,
            "subscription_id": self.subscription_id,
            "invoice_date": format_iso_date(self.invoice_date),
            "due_date": format_iso_date(self.due_date),
            "invoice_amount": format_decimal(self.invoice_amount, 2),
            "invoice_status": self.invoice_status,
            "created_at": format_iso_timestamp(self.created_at),
            "updated_at": format_iso_timestamp(self.updated_at),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Invoice":
        """Instantiate Invoice from a dictionary."""
        return cls(
            invoice_id=str(data["invoice_id"]),
            subscription_id=str(data["subscription_id"]),
            invoice_date=(
                parse_iso_date(data["invoice_date"])
                if isinstance(data["invoice_date"], str)
                else data["invoice_date"]
            ),
            due_date=(
                parse_iso_date(data["due_date"])
                if isinstance(data["due_date"], str)
                else data["due_date"]
            ),
            invoice_amount=Decimal(str(data["invoice_amount"])),
            invoice_status=str(data["invoice_status"]),
            created_at=(
                parse_iso_timestamp(data["created_at"])
                if isinstance(data["created_at"], str)
                else data["created_at"]
            ),
            updated_at=(
                parse_iso_timestamp(data["updated_at"])
                if isinstance(data["updated_at"], str)
                else data["updated_at"]
            ),
        )


@dataclass
class Payment:
    """Represents a settlement transaction against an invoice."""

    payment_id: str
    invoice_id: str
    payment_date: date
    payment_amount: Decimal
    payment_method: str
    payment_status: str
    created_at: datetime
    updated_at: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert payment dataclass to dictionary with serializable values."""
        return {
            "payment_id": self.payment_id,
            "invoice_id": self.invoice_id,
            "payment_date": format_iso_date(self.payment_date),
            "payment_amount": format_decimal(self.payment_amount, 2),
            "payment_method": self.payment_method,
            "payment_status": self.payment_status,
            "created_at": format_iso_timestamp(self.created_at),
            "updated_at": format_iso_timestamp(self.updated_at),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Payment":
        """Instantiate Payment from a dictionary."""
        return cls(
            payment_id=str(data["payment_id"]),
            invoice_id=str(data["invoice_id"]),
            payment_date=(
                parse_iso_date(data["payment_date"])
                if isinstance(data["payment_date"], str)
                else data["payment_date"]
            ),
            payment_amount=Decimal(str(data["payment_amount"])),
            payment_method=str(data["payment_method"]),
            payment_status=str(data["payment_status"]),
            created_at=(
                parse_iso_timestamp(data["created_at"])
                if isinstance(data["created_at"], str)
                else data["created_at"]
            ),
            updated_at=(
                parse_iso_timestamp(data["updated_at"])
                if isinstance(data["updated_at"], str)
                else data["updated_at"]
            ),
        )
