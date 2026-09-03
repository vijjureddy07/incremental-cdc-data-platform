"""Unit tests for bounded Delta Change Data Feed reader and table properties."""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.cdf.models import (
    CDF_METADATA_COLUMNS,
    CDFInvalidRangeError,
    CDFSourceNotFoundError,
)
from src.cdf.reader import CDFReader


def test_reader_enable_and_check_cdf(spark_session: SparkSession, tmp_path: Path):
    """Verify enabling CDF modifies Delta table properties and returns resulting commit version."""
    table_path = tmp_path / "delta_test_cdf"
    df = spark_session.createDataFrame([(1, "Alpha"), (2, "Beta")], ["id", "val"])
    df.write.format("delta").save(str(table_path))

    reader = CDFReader(spark_session)
    assert reader.is_cdf_enabled(table_path) is False

    version = reader.enable_cdf(table_path)
    assert version == 1  # version 0 was write, version 1 was ALTER TABLE
    assert reader.is_cdf_enabled(table_path) is True


def test_reader_read_bounded_changes(spark_session: SparkSession, tmp_path: Path):
    """Verify read_changes loads canonical CDF columns within bounded version ranges."""
    table_path = tmp_path / "delta_bounded_cdf"
    df = spark_session.createDataFrame([(1, "A")], ["id", "val"])
    df.write.format("delta").save(str(table_path))

    reader = CDFReader(spark_session)
    reader.enable_cdf(table_path)

    # Version 2 write
    df2 = spark_session.createDataFrame([(2, "B")], ["id", "val"])
    df2.write.format("delta").mode("append").save(str(table_path))

    # Version 3 write
    df3 = spark_session.createDataFrame([(3, "C")], ["id", "val"])
    df3.write.format("delta").mode("append").save(str(table_path))

    # Read range [2, 2]
    changes_v2 = reader.read_changes(table_path, start_version=2, end_version=2)
    rows_v2 = changes_v2.collect()
    assert len(rows_v2) == 1
    assert rows_v2[0]["id"] == 2
    assert rows_v2[0]["_change_type"] == "insert"
    assert rows_v2[0]["_commit_version"] == 2

    for col_name in CDF_METADATA_COLUMNS:
        assert col_name in changes_v2.columns

    # Read range [2, 3]
    changes_v2_v3 = reader.read_changes(table_path, start_version=2, end_version=3)
    assert changes_v2_v3.count() == 2


def test_reader_invalid_version_ranges(spark_session: SparkSession, tmp_path: Path):
    """Verify invalid start/end version ranges raise CDFInvalidRangeError."""
    table_path = tmp_path / "delta_range_test"
    df = spark_session.createDataFrame([(1, "A")], ["id", "val"])
    df.write.format("delta").save(str(table_path))

    reader = CDFReader(spark_session)
    reader.enable_cdf(table_path)

    with pytest.raises(CDFInvalidRangeError):
        reader.read_changes(table_path, start_version=-1)

    with pytest.raises(CDFInvalidRangeError):
        reader.read_changes(table_path, start_version=5, end_version=3)


def test_reader_non_existent_table(spark_session: SparkSession, tmp_path: Path):
    """Verify attempting to read or enable CDF on non-existent table raises CDFSourceNotFoundError."""
    reader = CDFReader(spark_session)
    missing_path = tmp_path / "does_not_exist"

    with pytest.raises(CDFSourceNotFoundError):
        reader.enable_cdf(missing_path)

    with pytest.raises(CDFSourceNotFoundError):
        reader.read_changes(missing_path, start_version=0)
