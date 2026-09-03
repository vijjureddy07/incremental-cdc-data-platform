"""Declarative table specifications and contracts for Databricks Lakeflow AUTO CDC."""

from dataclasses import dataclass, field

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.source.schemas import TABLE_SCHEMAS_MAP


@dataclass(frozen=True)
class TableCDCSpec:
    """Table-level declarative specification for Databricks Lakeflow AUTO CDC."""

    source_table: str
    primary_key: str
    business_columns: list[str]
    target_table_current: str
    snapshot_source_name: str
    cdc_source_name: str
    hydration_flow_name: str
    continuous_flow_name: str
    target_table_history: str | None = None
    history_hydration_flow_name: str | None = None
    history_continuous_flow_name: str | None = None
    history_track_columns: list[str] | None = None
    excluded_columns: list[str] = field(
        default_factory=lambda: ["operation", "sequence_number"]
    )


def expected_lakeflow_projection_schema(table_name: str) -> StructType:
    """Derive the authoritative Lakeflow AUTO CDC source projection schema for a table."""
    base_schema = TABLE_SCHEMAS_MAP[table_name]
    fields = list(base_schema.fields)
    fields.extend(
        [
            StructField("operation", StringType(), False),
            StructField("sequence_number", LongType(), False),
            StructField("latest_event_id", StringType(), True),
            StructField("latest_event_fingerprint", StringType(), True),
            StructField("latest_source_commit_timestamp", StringType(), True),
        ]
    )
    return StructType(fields)


TABLE_CDC_SPECS: dict[str, TableCDCSpec] = {
    "accounts": TableCDCSpec(
        source_table="accounts",
        primary_key="account_id",
        business_columns=[
            "account_id",
            "account_name",
            "industry",
            "country",
            "status",
            "created_at",
            "updated_at",
        ],
        target_table_current="accounts_current",
        snapshot_source_name="accounts_snapshot_source",
        cdc_source_name="accounts_cdc_source",
        hydration_flow_name="accounts_initial_hydration",
        continuous_flow_name="accounts_continuous_cdc",
    ),
    "subscriptions": TableCDCSpec(
        source_table="subscriptions",
        primary_key="subscription_id",
        business_columns=[
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
        target_table_current="subscriptions_current",
        snapshot_source_name="subscriptions_snapshot_source",
        cdc_source_name="subscriptions_cdc_source",
        hydration_flow_name="subscriptions_initial_hydration",
        continuous_flow_name="subscriptions_continuous_cdc",
        target_table_history="subscriptions_history",
        history_hydration_flow_name="subscriptions_history_initial_hydration",
        history_continuous_flow_name="subscriptions_history_continuous_cdc",
        history_track_columns=[
            "account_id",
            "plan_name",
            "billing_cycle",
            "monthly_amount",
            "status",
            "start_date",
            "end_date",
        ],
    ),
    "invoices": TableCDCSpec(
        source_table="invoices",
        primary_key="invoice_id",
        business_columns=[
            "invoice_id",
            "subscription_id",
            "invoice_date",
            "due_date",
            "invoice_amount",
            "invoice_status",
            "created_at",
            "updated_at",
        ],
        target_table_current="invoices_current",
        snapshot_source_name="invoices_snapshot_source",
        cdc_source_name="invoices_cdc_source",
        hydration_flow_name="invoices_initial_hydration",
        continuous_flow_name="invoices_continuous_cdc",
    ),
    "payments": TableCDCSpec(
        source_table="payments",
        primary_key="payment_id",
        business_columns=[
            "payment_id",
            "invoice_id",
            "payment_date",
            "payment_amount",
            "payment_method",
            "payment_status",
            "created_at",
            "updated_at",
        ],
        target_table_current="payments_current",
        snapshot_source_name="payments_snapshot_source",
        cdc_source_name="payments_cdc_source",
        hydration_flow_name="payments_initial_hydration",
        continuous_flow_name="payments_continuous_cdc",
    ),
}
