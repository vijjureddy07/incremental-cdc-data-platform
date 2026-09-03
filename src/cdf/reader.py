"""Bounded Delta Change Data Feed reader and table configuration engine."""

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from src.cdf.models import (
    CDF_METADATA_COLUMNS,
    CDFInvalidRangeError,
    CDFNotEnabledError,
    CDFSourceNotFoundError,
)


class CDFReader:
    """Reads bounded version ranges from Delta tables using Delta Change Data Feed."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def enable_cdf(self, source_path: str | Path) -> int:
        """Enable legacy Change Data Feed on an existing Delta table and return the commit version.

        Args:
            source_path: Path to the target Delta table.

        Returns:
            The Delta commit version resulting from enabling the table property.
        """
        path_str = str(Path(source_path).resolve())
        if not DeltaTable.isDeltaTable(self.spark, path_str):
            raise CDFSourceNotFoundError(f"Delta table not found at {path_str}")

        self.spark.sql(
            f"ALTER TABLE delta.`{path_str}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )

        delta_table = DeltaTable.forPath(self.spark, path_str)
        history_row = delta_table.history().first()
        if history_row is None:
            return 0
        return int(history_row["version"])

    def is_cdf_enabled(self, source_path: str | Path) -> bool:
        """Check whether Change Data Feed is enabled on a Delta table."""
        path_str = str(Path(source_path).resolve())
        if not DeltaTable.isDeltaTable(self.spark, path_str):
            raise CDFSourceNotFoundError(f"Delta table not found at {path_str}")

        delta_table = DeltaTable.forPath(self.spark, path_str)
        detail_row = delta_table.detail().first()
        if detail_row is None:
            return False

        properties = detail_row["properties"] or {}
        return properties.get("delta.enableChangeDataFeed", "").lower() == "true"

    def read_changes(
        self,
        source_path: str | Path,
        start_version: int,
        end_version: int | None = None,
    ) -> DataFrame:
        """Read a bounded range of Change Data Feed commits from a Delta table.

        Args:
            source_path: Path to the Delta table.
            start_version: Inclusive starting Delta table version (must be >= 0).
            end_version: Optional inclusive ending Delta table version (must be >= start_version).

        Returns:
            DataFrame containing the change feed records including canonical metadata columns.
        """
        if start_version < 0:
            raise CDFInvalidRangeError(f"start_version must be >= 0, got {start_version}")
        if end_version is not None and end_version < start_version:
            raise CDFInvalidRangeError(
                f"end_version ({end_version}) cannot be less than start_version ({start_version})"
            )

        path_str = str(Path(source_path).resolve())
        if not DeltaTable.isDeltaTable(self.spark, path_str):
            raise CDFSourceNotFoundError(f"Delta table not found at {path_str}")

        reader = (
            self.spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", start_version)
        )
        if end_version is not None:
            reader = reader.option("endingVersion", end_version)

        df = reader.load(path_str)

        # Validate presence of canonical CDF metadata columns
        missing_cols = [c for c in CDF_METADATA_COLUMNS if c not in df.columns]
        if missing_cols:
            raise CDFNotEnabledError(
                f"Missing canonical CDF metadata columns {missing_cols} in {path_str}. "
                "Ensure delta.enableChangeDataFeed = true is set."
            )

        return df
