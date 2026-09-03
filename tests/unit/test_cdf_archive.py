"""Unit tests for downstream Delta archive store, change ID stability, and MERGE idempotency."""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.cdf.archive import CDFArchiveStore


def test_prepare_cdf_records_deterministic_change_id(spark_session: SparkSession, tmp_path: Path):
    """Verify deterministic change ID derivation produces distinct hashes for pre/postimage and stable hashes on replay."""
    schema = StructType(
        [
            StructField("account_id", StringType()),
            StructField("account_name", StringType()),
            StructField("_change_type", StringType()),
            StructField("_commit_version", LongType()),
            StructField("_commit_timestamp", StringType()),
        ]
    )

    data = [
        ("ACC-001", "Old Name", "update_preimage", 3, "2026-05-11T00:00:00Z"),
        ("ACC-001", "New Name", "update_postimage", 3, "2026-05-11T00:00:00Z"),
    ]

    df = spark_session.createDataFrame(data, schema=schema)
    archive_store = CDFArchiveStore(spark_session, archive_base_dir=tmp_path / "archive")

    prepared_1 = archive_store.prepare_cdf_records("accounts", df, primary_key="account_id")
    rows_1 = prepared_1.collect()

    preimage_id_1 = rows_1[0]["_change_id"]
    postimage_id_1 = rows_1[1]["_change_id"]

    # Preimage and postimage must have different change IDs
    assert preimage_id_1 != postimage_id_1
    assert len(preimage_id_1) == 64  # SHA-256 hex string length
    assert len(postimage_id_1) == 64

    # Identical replay must produce exact same change IDs
    prepared_2 = archive_store.prepare_cdf_records("accounts", df, primary_key="account_id")
    rows_2 = prepared_2.collect()
    assert rows_2[0]["_change_id"] == preimage_id_1
    assert rows_2[1]["_change_id"] == postimage_id_1


def test_archive_store_idempotent_write_and_merge(spark_session: SparkSession, tmp_path: Path):
    """Verify first write inserts N records and replaying same range inserts 0 duplicates via MERGE."""
    schema = StructType(
        [
            StructField("account_id", StringType()),
            StructField("status", StringType()),
            StructField("_change_type", StringType()),
            StructField("_commit_version", LongType()),
            StructField("_commit_timestamp", StringType()),
        ]
    )

    data = [
        ("ACC-001", "ACTIVE", "insert", 1, "2026-05-11T00:00:00Z"),
        ("ACC-002", "PENDING", "insert", 1, "2026-05-11T00:00:00Z"),
    ]
    df = spark_session.createDataFrame(data, schema=schema)

    archive_store = CDFArchiveStore(spark_session, archive_base_dir=tmp_path / "archive")

    # Initial write: 2 new records
    inserted_1 = archive_store.write_changes("accounts", df, primary_key="account_id")
    assert inserted_1 == 2
    assert archive_store.archive_exists("accounts") is True

    # Check total rows in archive
    archive_df_1 = archive_store.read_archive("accounts")
    assert archive_df_1.count() == 2

    # Replay write of exact same CDF records: must insert 0 duplicates
    inserted_2 = archive_store.write_changes("accounts", df, primary_key="account_id")
    assert inserted_2 == 0

    archive_df_2 = archive_store.read_archive("accounts")
    assert archive_df_2.count() == 2


def test_prepare_cdf_records_missing_primary_key(spark_session: SparkSession, tmp_path: Path):
    """Verify ValueError is raised if the specified primary key column is absent."""
    schema = StructType([StructField("name", StringType())])
    df = spark_session.createDataFrame([("Alice",)], schema=schema)

    archive_store = CDFArchiveStore(spark_session, archive_base_dir=tmp_path / "archive")
    with pytest.raises(ValueError, match="Primary key 'id' not found"):
        archive_store.prepare_cdf_records("users", df, primary_key="id")
