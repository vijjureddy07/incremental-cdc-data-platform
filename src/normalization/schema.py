"""Explicit PySpark StructType schemas for raw and normalized CDC data structures."""

from pyspark.sql.types import (
    BooleanType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

# Raw CDC Event Ingestion Schema
RAW_CDC_SPARK_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=True),
        StructField("table_name", StringType(), nullable=True),
        StructField("operation", StringType(), nullable=True),
        StructField("business_key", MapType(StringType(), StringType()), nullable=True),
        StructField("sequence_number", LongType(), nullable=True),
        StructField("event_timestamp", StringType(), nullable=True),
        StructField("source_commit_timestamp", StringType(), nullable=True),
        StructField("batch_id", StringType(), nullable=True),
        StructField("payload", StringType(), nullable=True),
        StructField("before_payload", StringType(), nullable=True),
        StructField("source_system", StringType(), nullable=True),
        StructField("source_file", StringType(), nullable=True),
        StructField("ingestion_batch_id", StringType(), nullable=True),
        StructField("ingestion_order", LongType(), nullable=True),
    ]
)

# Normalized Canonical CDC Event Schema
NORMALIZED_CDC_SPARK_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("table_name", StringType(), nullable=False),
        StructField("operation", StringType(), nullable=False),
        StructField("business_key", MapType(StringType(), StringType()), nullable=False),
        StructField("business_key_canonical", StringType(), nullable=False),
        StructField("entity_sequence_key", StringType(), nullable=False),
        StructField("sequence_number", LongType(), nullable=False),
        StructField("event_timestamp", StringType(), nullable=False),
        StructField("source_commit_timestamp", StringType(), nullable=False),
        StructField("batch_id", StringType(), nullable=False),
        StructField("source_system", StringType(), nullable=False),
        StructField("payload", StringType(), nullable=True),
        StructField("before_payload", StringType(), nullable=True),
        StructField("event_fingerprint", StringType(), nullable=False),
        StructField("ingestion_batch_id", StringType(), nullable=False),
        StructField("source_file", StringType(), nullable=False),
        StructField("is_late_arrival", BooleanType(), nullable=False),
        StructField("normalized_at", StringType(), nullable=False),
    ]
)
