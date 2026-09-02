"""Unit tests for Databricks Lakeflow source dataset projections and transformations."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from databricks.lakeflow.contracts import TABLE_CDC_SPECS


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
            ("ACC-0001", "Acme Inc", "Software", "US", "ACTIVE", "2026-05-11T01:00:00Z", "2026-05-11T01:00:00Z", "100.00"),
            None,
            ("ACC-0001",),
        )
    ]

    df = spark_session.createDataFrame(data, schema=schema)
    spec = TABLE_CDC_SPECS["accounts"]

    pk = spec.primary_key
    select_exprs = [
        F.coalesce(F.col(f"payload.{pk}"), F.col(f"before_payload.{pk}"), F.col(f"business_key.{pk}")).alias(pk)
    ]
    for col_name in spec.business_columns:
        if col_name != pk:
            select_exprs.append(F.col(f"payload.{col_name}").alias(col_name))

    select_exprs.extend(
        [
            F.col("operation").alias("operation"),
            F.col("sequence_number").cast("long").alias("sequence_number"),
            F.col("event_id").alias("latest_event_id"),
            F.col("event_fingerprint").alias("latest_event_fingerprint"),
            F.col("source_commit_timestamp").alias("latest_source_commit_timestamp"),
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
    pk = spec.primary_key

    select_exprs = [
        F.coalesce(F.col(f"payload.{pk}"), F.col(f"before_payload.{pk}"), F.col(f"business_key.{pk}")).alias(pk)
    ]
    select_exprs.extend(
        [
            F.col("operation").alias("operation"),
            F.col("sequence_number").cast("long").alias("sequence_number"),
        ]
    )

    projected_df = df.select(*select_exprs).filter(F.col("sequence_number").isNotNull() & F.col(pk).isNotNull())
    assert projected_df.count() == 1
    row = projected_df.first()
    assert row["payment_id"] == "PAY-0002"
    assert row["operation"] == "DELETE"
    assert row["sequence_number"] == 20


def test_snapshot_hydration_projection_sequence_zero(spark_session: SparkSession):
    """Verify snapshot projection assigns deterministic baseline sequence_number = 0 and operation = SNAPSHOT."""
    data = [("ACC-0001", "Acme Inc"), ("ACC-0002", "Globex Corp")]
    schema = StructType([StructField("account_id", StringType()), StructField("account_name", StringType())])

    raw_df = spark_session.createDataFrame(data, schema=schema)
    snapshot_df = (
        raw_df.withColumn("sequence_number", F.lit(0).cast("long"))
        .withColumn("operation", F.lit("SNAPSHOT"))
        .filter(F.col("sequence_number").isNotNull() & F.col("account_id").isNotNull())
    )

    rows = snapshot_df.collect()
    assert len(rows) == 2
    for r in rows:
        assert r["sequence_number"] == 0
        assert r["operation"] == "SNAPSHOT"
        assert r["account_id"] is not None
