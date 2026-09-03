"""Unit tests for Databricks Lakeflow source dataset projections and schema alignment."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from databricks.lakeflow.contracts import (
    TABLE_CDC_SPECS,
    expected_lakeflow_projection_schema,
)
from src.source.schemas import TABLE_SCHEMAS_MAP


def test_accounts_cdc_projection_columns_and_isolation(spark_session: SparkSession):
    """Verify accounts CDC projection contains all business columns and isolates from other table domains."""
    schema = StructType(
        [
            StructField("table_name", StringType()),
            StructField("operation", StringType()),
            StructField("sequence_number", LongType()),
            StructField("event_id", StringType()),
            StructField("event_fingerprint", StringType()),
            StructField("source_commit_timestamp", StringType()),
            StructField(
                "payload",
                StructType(
                    [
                        StructField("account_id", StringType()),
                        StructField("account_name", StringType()),
                        StructField("industry", StringType()),
                        StructField("country", StringType()),
                        StructField("status", StringType()),
                        StructField("created_at", StringType()),
                        StructField("updated_at", StringType()),
                        StructField("invoice_amount", StringType()),  # Unrelated field
                    ]
                ),
            ),
            StructField(
                "before_payload",
                StructType([StructField("account_id", StringType())]),
            ),
            StructField(
                "business_key",
                StructType([StructField("account_id", StringType())]),
            ),
        ]
    )

    data = [
        (
            "accounts",
            "INSERT",
            10,
            "evt_001",
            "fp_001",
            "2026-05-11T01:00:00Z",
            (
                "ACC-0001",
                "Acme Inc",
                "Software",
                "US",
                "ACTIVE",
                "2026-05-11T01:00:00Z",
                "2026-05-11T01:00:00Z",
                "100.00",
            ),
            None,
            ("ACC-0001",),
        )
    ]

    df = spark_session.createDataFrame(data, schema=schema)
    spec = TABLE_CDC_SPECS["accounts"]
    base_schema = TABLE_SCHEMAS_MAP["accounts"]

    pk = spec.primary_key
    pk_field = base_schema[pk]
    select_exprs = [
        F.coalesce(
            F.col(f"payload.{pk}"),
            F.col(f"before_payload.{pk}"),
            F.col(f"business_key.{pk}"),
        )
        .cast(pk_field.dataType)
        .alias(pk)
    ]
    for f in base_schema.fields:
        if f.name != pk:
            select_exprs.append(F.col(f"payload.{f.name}").cast(f.dataType).alias(f.name))

    select_exprs.extend(
        [
            F.col("operation").cast("string").alias("operation"),
            F.col("sequence_number").cast("long").alias("sequence_number"),
            F.col("event_id").cast("string").alias("latest_event_id"),
            F.col("event_fingerprint").cast("string").alias("latest_event_fingerprint"),
            F.col("source_commit_timestamp").cast("string").alias("latest_source_commit_timestamp"),
        ]
    )

    projected_df = df.filter(F.col("table_name") == "accounts").select(*select_exprs)
    cols = projected_df.columns

    # Assert expected columns
    for expected in spec.business_columns:
        assert expected in cols

    assert "operation" in cols
    assert "sequence_number" in cols
    assert "latest_event_id" in cols
    assert "latest_event_fingerprint" in cols
    assert "latest_source_commit_timestamp" in cols

    # Assert unrelated field was excluded
    assert "invoice_amount" not in cols


def test_payment_delete_projection_preserves_primary_key(spark_session: SparkSession):
    """Verify that a DELETE event where payload is None preserves payment_id from before_payload or business_key."""
    schema = StructType(
        [
            StructField("table_name", StringType()),
            StructField("operation", StringType()),
            StructField("sequence_number", LongType()),
            StructField("event_id", StringType()),
            StructField("event_fingerprint", StringType()),
            StructField("source_commit_timestamp", StringType()),
            StructField(
                "payload",
                StructType([StructField("payment_id", StringType())]),
            ),
            StructField(
                "before_payload",
                StructType([StructField("payment_id", StringType())]),
            ),
            StructField(
                "business_key",
                StructType([StructField("payment_id", StringType())]),
            ),
        ]
    )

    # DELETE record: payload is None, before_payload has payment_id
    data = [
        (
            "payments",
            "DELETE",
            20,
            "evt_del_01",
            "fp_del_01",
            "2026-05-11T02:00:00Z",
            None,
            ("PAY-0002",),
            ("PAY-0002",),
        )
    ]

    df = spark_session.createDataFrame(data, schema=schema)
    spec = TABLE_CDC_SPECS["payments"]
    base_schema = TABLE_SCHEMAS_MAP["payments"]
    pk = spec.primary_key
    pk_field = base_schema[pk]

    select_exprs = [
        F.coalesce(
            F.col(f"payload.{pk}"),
            F.col(f"before_payload.{pk}"),
            F.col(f"business_key.{pk}"),
        )
        .cast(pk_field.dataType)
        .alias(pk)
    ]
    select_exprs.extend(
        [
            F.col("operation").cast("string").alias("operation"),
            F.col("sequence_number").cast("long").alias("sequence_number"),
            F.col("event_id").cast("string").alias("latest_event_id"),
            F.col("event_fingerprint").cast("string").alias("latest_event_fingerprint"),
            F.col("source_commit_timestamp").cast("string").alias("latest_source_commit_timestamp"),
        ]
    )

    projected_df = df.select(*select_exprs).filter(
        F.col("sequence_number").isNotNull() & F.col(pk).isNotNull()
    )
    assert projected_df.count() == 1
    row = projected_df.first()
    assert row["payment_id"] == "PAY-0002"
    assert row["operation"] == "DELETE"
    assert row["sequence_number"] == 20


def test_snapshot_hydration_projection_lineage_and_sequence_zero(spark_session: SparkSession):
    """Verify snapshot projection assigns sequence_number = 0, operation = SNAPSHOT, and typed NULL lineage fields."""
    base_schema = TABLE_SCHEMAS_MAP["accounts"]
    data = [
        (
            "ACC-0001",
            "Acme Inc",
            "Software",
            "US",
            "ACTIVE",
            "2026-05-11 00:00:00",
            "2026-05-11 00:00:00",
        ),
    ]
    raw_schema = StructType(
        [
            StructField("account_id", StringType()),
            StructField("account_name", StringType()),
            StructField("industry", StringType()),
            StructField("country", StringType()),
            StructField("status", StringType()),
            StructField("created_at", StringType()),
            StructField("updated_at", StringType()),
        ]
    )

    raw_df = spark_session.createDataFrame(data, schema=raw_schema)
    select_exprs = [F.col(f.name).cast(f.dataType).alias(f.name) for f in base_schema.fields]
    select_exprs.extend(
        [
            F.lit("SNAPSHOT").cast("string").alias("operation"),
            F.lit(0).cast("long").alias("sequence_number"),
            F.lit(None).cast("string").alias("latest_event_id"),
            F.lit(None).cast("string").alias("latest_event_fingerprint"),
            F.lit(None).cast("string").alias("latest_source_commit_timestamp"),
        ]
    )

    snapshot_df = raw_df.select(*select_exprs).filter(
        F.col("sequence_number").isNotNull() & F.col("account_id").isNotNull()
    )

    assert snapshot_df.count() == 1
    row = snapshot_df.first()
    assert row["sequence_number"] == 0
    assert row["operation"] == "SNAPSHOT"
    assert row["latest_event_id"] is None
    assert row["latest_event_fingerprint"] is None
    assert row["latest_source_commit_timestamp"] is None


def test_snapshot_and_cdc_schema_equality_across_all_tables(spark_session: SparkSession):
    """Verify snapshot projection and CDC projection produce identical field names and data types for all tables."""
    for table_name in ["accounts", "subscriptions", "invoices", "payments"]:
        expected_schema = expected_lakeflow_projection_schema(table_name)
        expected_field_dict = {f.name: f.dataType for f in expected_schema.fields}

        base_schema = TABLE_SCHEMAS_MAP[table_name]
        spec = TABLE_CDC_SPECS[table_name]

        # 1. Snapshot projection schema
        snap_select = [F.col(f.name).cast(f.dataType).alias(f.name) for f in base_schema.fields]
        snap_select.extend(
            [
                F.lit("SNAPSHOT").cast("string").alias("operation"),
                F.lit(0).cast("long").alias("sequence_number"),
                F.lit(None).cast("string").alias("latest_event_id"),
                F.lit(None).cast("string").alias("latest_event_fingerprint"),
                F.lit(None).cast("string").alias("latest_source_commit_timestamp"),
            ]
        )
        empty_snap_df = spark_session.createDataFrame([], schema=base_schema).select(*snap_select)
        snap_fields = {f.name: f.dataType for f in empty_snap_df.schema.fields}

        # 2. CDC projection schema
        pk = spec.primary_key
        pk_field = base_schema[pk]
        cdc_raw_schema = StructType(
            [
                StructField("table_name", StringType()),
                StructField("operation", StringType()),
                StructField("sequence_number", LongType()),
                StructField("event_id", StringType()),
                StructField("event_fingerprint", StringType()),
                StructField("source_commit_timestamp", StringType()),
                StructField("payload", base_schema),
                StructField("before_payload", StructType([StructField(pk, pk_field.dataType)])),
                StructField("business_key", StructType([StructField(pk, pk_field.dataType)])),
            ]
        )
        cdc_select = [
            F.coalesce(
                F.col(f"payload.{pk}"),
                F.col(f"before_payload.{pk}"),
                F.col(f"business_key.{pk}"),
            )
            .cast(pk_field.dataType)
            .alias(pk)
        ]
        for f in base_schema.fields:
            if f.name != pk:
                cdc_select.append(F.col(f"payload.{f.name}").cast(f.dataType).alias(f.name))
        cdc_select.extend(
            [
                F.col("operation").cast("string").alias("operation"),
                F.col("sequence_number").cast("long").alias("sequence_number"),
                F.col("event_id").cast("string").alias("latest_event_id"),
                F.col("event_fingerprint").cast("string").alias("latest_event_fingerprint"),
                F.col("source_commit_timestamp")
                .cast("string")
                .alias("latest_source_commit_timestamp"),
            ]
        )
        empty_cdc_df = spark_session.createDataFrame([], schema=cdc_raw_schema).select(*cdc_select)
        cdc_fields = {f.name: f.dataType for f in empty_cdc_df.schema.fields}

        # Assert field-by-field equality between snapshot and CDC projections
        assert snap_fields == cdc_fields
        assert snap_fields == expected_field_dict
