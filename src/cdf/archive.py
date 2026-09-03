"""Durable downstream Delta Lake archive store with deterministic change tracking."""

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class CDFArchiveStore:
    """Manages permanent downstream Delta archive tables populated from Delta CDF."""

    def __init__(
        self,
        spark: SparkSession,
        archive_base_dir: str | Path = "data/delta/downstream/cdf_archive",
    ) -> None:
        self.spark = spark
        self.archive_base_dir = Path(archive_base_dir)

    def get_archive_path(self, source_table: str) -> Path:
        """Get the storage directory path for a specific source domain's archive table."""
        return self.archive_base_dir / source_table

    def archive_exists(self, source_table: str) -> bool:
        """Check whether a valid Delta archive table exists for the given source table."""
        path = self.get_archive_path(source_table)
        if not path.exists():
            return False
        return DeltaTable.isDeltaTable(self.spark, str(path.resolve()))

    def prepare_cdf_records(
        self,
        source_table: str,
        df: DataFrame,
        primary_key: str,
    ) -> DataFrame:
        """Enrich raw CDF DataFrame with _source_table and deterministic SHA-256 _change_id."""
        if primary_key not in df.columns:
            raise ValueError(
                f"Primary key '{primary_key}' not found in DataFrame columns {df.columns}"
            )

        # Derive deterministic components: table, version, change_type, primary_key, and row values
        excluded_from_hash = {"_change_id", "_source_table"}
        data_cols = sorted([c for c in df.columns if c not in excluded_from_hash])

        hash_components = [
            F.lit(source_table),
            F.col("_commit_version").cast("string"),
            F.col("_change_type").cast("string"),
            F.col(primary_key).cast("string"),
        ]

        for col_name in data_cols:
            hash_components.append(F.coalesce(F.col(col_name).cast("string"), F.lit("__NULL__")))

        change_id_col = F.sha2(F.concat_ws("||", *hash_components), 256).alias("_change_id")

        return df.withColumn("_source_table", F.lit(source_table).cast("string")).withColumn(
            "_change_id", change_id_col
        )

    def write_changes(
        self,
        source_table: str,
        df: DataFrame,
        primary_key: str,
    ) -> int:
        """Idempotently append newly observed CDF records to the downstream Delta archive.

        Args:
            source_table: Table name.
            df: Raw CDF records DataFrame.
            primary_key: Authoritative business primary key column name.

        Returns:
            Number of newly inserted archive rows (0 on identical replay).
        """
        if df.rdd.isEmpty():
            return 0

        prepared_df = self.prepare_cdf_records(source_table, df, primary_key)
        archive_path = self.get_archive_path(source_table)
        path_str = str(archive_path.resolve())

        if not self.archive_exists(source_table):
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            prepared_df.write.format("delta").mode("overwrite").save(path_str)
            return prepared_df.count()

        # Table exists: execute idempotent Delta MERGE on _change_id
        delta_table = DeltaTable.forPath(self.spark, path_str)
        count_before = delta_table.toDF().count()

        (
            delta_table.alias("target")
            .merge(
                source=prepared_df.alias("source"),
                condition="target._change_id = source._change_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        count_after = delta_table.toDF().count()
        return count_after - count_before

    def read_archive(self, source_table: str) -> DataFrame:
        """Read the durable downstream CDF archive table."""
        path = self.get_archive_path(source_table)
        path_str = str(path.resolve())
        if not self.archive_exists(source_table):
            raise FileNotFoundError(f"Delta archive not found at {path_str}")
        return self.spark.read.format("delta").load(path_str)
