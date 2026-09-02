"""Delta Lake current-state target table store and bootstrap engine."""

from decimal import Decimal
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructType,
    TimestampType,
)

from src.merge.models import (
    TARGET_METADATA_FIELDS,
    TargetAlreadyInitializedError,
)
from src.source.schemas import TABLE_PRIMARY_KEYS, TABLE_SCHEMAS_MAP
from src.utils.helpers import parse_iso_date, parse_iso_timestamp


class DeltaTargetStore:
    """Manages local Delta Lake current-state tables and snapshot initialization."""

    def __init__(
        self,
        spark: SparkSession,
        target_base_dir: str | Path = "data/delta/current",
    ) -> None:
        self.spark = spark
        self.target_base_dir = Path(target_base_dir)

    def get_table_path(self, table_name: str) -> Path:
        """Get the directory path for a specific target table."""
        return self.target_base_dir / table_name

    def table_exists(self, table_name: str) -> bool:
        """Check whether a valid Delta table exists for the given table name."""
        path = self.get_table_path(table_name)
        if not path.exists():
            return False
        return DeltaTable.isDeltaTable(self.spark, str(path))

    def get_full_target_schema(self, table_name: str) -> StructType:
        """Construct the combined business + CDC metadata schema for a target table."""
        if table_name not in TABLE_SCHEMAS_MAP:
            raise ValueError(f"Unknown table: {table_name}")
        base_schema = TABLE_SCHEMAS_MAP[table_name]
        return StructType(list(base_schema.fields) + list(TARGET_METADATA_FIELDS))

    def initialize_targets(
        self,
        source_snapshot: dict[str, list[dict[str, Any]]],
        overwrite: bool = False,
    ) -> dict[str, int]:
        """Bootstrap the 4 current-state Delta tables from a deterministic source snapshot.

        Args:
            source_snapshot: Mapping of table_name -> list of row dictionaries.
            overwrite: If True, overwrites existing Delta tables. If False and table exists,
                       raises TargetAlreadyInitializedError.

        Returns:
            Dictionary of table_name -> initial row count.
        """
        counts: dict[str, int] = {}

        # Check existing tables first when overwrite is False
        if not overwrite:
            for table_name in TABLE_SCHEMAS_MAP:
                if self.table_exists(table_name):
                    raise TargetAlreadyInitializedError(
                        f"Target table '{table_name}' is already initialized at {self.get_table_path(table_name)}."
                    )

        for table_name, rows in source_snapshot.items():
            if table_name not in TABLE_SCHEMAS_MAP:
                continue

            table_path = self.get_table_path(table_name)
            table_path.mkdir(parents=True, exist_ok=True)

            base_schema = TABLE_SCHEMAS_MAP[table_name]
            converted_rows = []

            for r in rows:
                row_dict: dict[str, Any] = {}
                for field in base_schema.fields:
                    val = r.get(field.name)
                    if val is None:
                        row_dict[field.name] = None
                    elif isinstance(field.dataType, DecimalType):
                        row_dict[field.name] = Decimal(str(val))
                    elif isinstance(field.dataType, DateType):
                        row_dict[field.name] = (
                            parse_iso_date(str(val)) if isinstance(val, str) else val
                        )
                    elif isinstance(field.dataType, TimestampType):
                        row_dict[field.name] = (
                            parse_iso_timestamp(str(val)) if isinstance(val, str) else val
                        )
                    elif isinstance(field.dataType, StringType):
                        row_dict[field.name] = str(val)
                    else:
                        row_dict[field.name] = val

                # Operational metadata columns for initial snapshot rows
                created_at_str = str(r.get("created_at") or "2026-05-11T00:00:00Z")
                row_dict["_last_sequence_number"] = 0
                row_dict["_last_event_id"] = "snapshot_init"
                row_dict["_last_operation"] = "SNAPSHOT"
                row_dict["_last_event_fingerprint"] = "snapshot_init"
                row_dict["_last_source_commit_timestamp"] = created_at_str
                row_dict["_last_processing_id"] = "snapshot_bootstrap"
                row_dict["_is_deleted"] = False
                row_dict["_deleted_at"] = None

                converted_rows.append(row_dict)

            full_schema = self.get_full_target_schema(table_name)
            df = self.spark.createDataFrame(converted_rows, schema=full_schema)

            (
                df.write.format("delta")
                .mode("overwrite" if overwrite else "errorIfExists")
                .save(str(table_path))
            )
            counts[table_name] = len(converted_rows)

        return counts

    def read_current_table(
        self,
        table_name: str,
        include_metadata: bool = True,
        include_deleted: bool = False,
    ) -> DataFrame:
        """Read current state of a Delta table."""
        path = self.get_table_path(table_name)
        if not self.table_exists(table_name):
            raise FileNotFoundError(f"Delta table not found at {path}")

        df = self.spark.read.format("delta").load(str(path))
        if not include_deleted:
            df = df.filter(F.col("_is_deleted") == False)  # noqa: E712

        if not include_metadata:
            meta_cols = {f.name for f in TARGET_METADATA_FIELDS}
            keep_cols = [c for c in df.columns if c not in meta_cols]
            df = df.select(*keep_cols)

        return df

    def read_target_version(self, table_name: str, version: int) -> DataFrame:
        """Read a historical version of a Delta table via time travel."""
        path = self.get_table_path(table_name)
        if not self.table_exists(table_name):
            raise FileNotFoundError(f"Delta table not found at {path}")

        return self.spark.read.format("delta").option("versionAsOf", version).load(str(path))

    def get_delta_history(self, table_name: str) -> list[dict[str, Any]]:
        """Retrieve commit history for a Delta table."""
        path = self.get_table_path(table_name)
        if not self.table_exists(table_name):
            raise FileNotFoundError(f"Delta table not found at {path}")

        delta_table = DeltaTable.forPath(self.spark, str(path))
        history_df = delta_table.history()
        return [row.asDict() for row in history_df.collect()]

    def get_table_version(self, table_name: str) -> int:
        """Get the current latest commit version of a Delta table."""
        history = self.get_delta_history(table_name)
        if not history:
            return 0
        return int(history[0]["version"])

    def get_primary_key(self, table_name: str) -> str:
        """Get the primary key column name for a table."""
        if table_name not in TABLE_PRIMARY_KEYS:
            raise ValueError(f"Unknown table: {table_name}")
        return TABLE_PRIMARY_KEYS[table_name]
