"""Databricks Lakeflow Declarative Pipelines with Managed AUTO CDC.

This module defines declarative streaming tables, initial hydration flows (once=True),
and continuous AUTO CDC ingestion flows targeting SCD Type 1 current-state tables
and SCD Type 2 history tables.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from databricks.lakeflow.config import LakeflowConfig
from databricks.lakeflow.contracts import TABLE_CDC_SPECS, TableCDCSpec


def build_snapshot_projection(table_name: str, config: LakeflowConfig) -> str:
    """Register and return the snapshot hydration source dataset name."""
    spec = TABLE_CDC_SPECS[table_name]
    source_name = spec.snapshot_source_name

    # In Databricks Lakeflow runtime, table functions define streaming/batch datasets
    @dp.table(
        name=source_name,
        comment=f"Initial snapshot hydration source dataset for {table_name}",
    )
    def snapshot_dataset():
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        snapshot_path = f"{config.snapshot_base_path}/{table_name}.parquet"

        # Read snapshot Parquet files
        raw_df = spark.read.parquet(snapshot_path)

        # Attach deterministic snapshot sequence baseline (sequence_number = 0)
        return (
            raw_df.withColumn("sequence_number", F.lit(0).cast("long"))
            .withColumn("operation", F.lit("SNAPSHOT"))
            .filter(F.col("sequence_number").isNotNull() & F.col(spec.primary_key).isNotNull())
        )

    return source_name


def build_cdc_projection(table_name: str, config: LakeflowConfig) -> str:
    """Register and return the continuous normalized CDC source dataset name."""
    spec = TABLE_CDC_SPECS[table_name]
    source_name = spec.cdc_source_name

    @dp.table(
        name=source_name,
        comment=f"Continuous normalized CDC source dataset for {table_name}",
    )
    def cdc_dataset():
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        cdc_path = f"{config.normalized_cdc_base_path}/*/accepted.jsonl"

        # Read normalized CDC change stream via cloudFiles / JSON stream
        raw_stream = spark.readStream.format("json").load(cdc_path)

        # Filter strictly for this table's events
        table_events = raw_stream.filter(F.col("table_name") == spec.source_table)

        # Project flattened business columns + control columns
        select_exprs = []

        # 1. Primary key: preserve PK from payload for INSERT/UPDATE or before_payload/business_key for DELETE
        pk = spec.primary_key
        select_exprs.append(
            F.coalesce(
                F.col(f"payload.{pk}"),
                F.col(f"before_payload.{pk}"),
                F.col(f"business_key.{pk}"),
            ).alias(pk)
        )

        # 2. Non-PK business columns from payload
        for col_name in spec.business_columns:
            if col_name != pk:
                select_exprs.append(F.col(f"payload.{col_name}").alias(col_name))

        # 3. CDC operational and control columns
        select_exprs.extend(
            [
                F.col("operation").alias("operation"),
                F.col("sequence_number").cast("long").alias("sequence_number"),
                F.col("event_id").alias("latest_event_id"),
                F.col("event_fingerprint").alias("latest_event_fingerprint"),
                F.col("source_commit_timestamp").alias("latest_source_commit_timestamp"),
            ]
        )

        return (
            table_events.select(*select_exprs)
            .filter(F.col("sequence_number").isNotNull() & F.col(pk).isNotNull())
        )

    return source_name


def register_auto_cdc_flows_for_spec(spec: TableCDCSpec, config: LakeflowConfig) -> None:
    """Register SCD Type 1 current table and optional SCD Type 2 history table with AUTO CDC flows."""
    # ---------------------------------------------------------
    # 1. SCD Type 1 Current-State Target Table
    # ---------------------------------------------------------
    dp.create_streaming_table(
        name=spec.target_table_current,
        comment=f"Current-state {spec.source_table} table managed by Lakeflow AUTO CDC",
    )

    # Hydration Flow (once=True): seeds target with initial snapshot (sequence_number = 0)
    dp.create_auto_cdc_flow(
        name=spec.hydration_flow_name,
        once=True,
        target=spec.target_table_current,
        source=spec.snapshot_source_name,
        keys=[spec.primary_key],
        sequence_by="sequence_number",
        stored_as_scd_type=1,
        except_column_list=list(spec.excluded_columns),
        ignore_null_updates=config.ignore_null_updates,
    )

    # Continuous CDC Flow: applies ongoing INSERT / UPDATE / DELETE events
    dp.create_auto_cdc_flow(
        name=spec.continuous_flow_name,
        target=spec.target_table_current,
        source=spec.cdc_source_name,
        keys=[spec.primary_key],
        sequence_by="sequence_number",
        apply_as_deletes=F.expr("operation = 'DELETE'"),
        stored_as_scd_type=1,
        except_column_list=list(spec.excluded_columns),
        ignore_null_updates=config.ignore_null_updates,
    )

    # ---------------------------------------------------------
    # 2. SCD Type 2 Historical Audit Target Table (if configured)
    # ---------------------------------------------------------
    if (
        spec.target_table_history
        and spec.history_hydration_flow_name
        and spec.history_continuous_flow_name
        and spec.history_track_columns
    ):
        dp.create_streaming_table(
            name=spec.target_table_history,
            comment=f"Historical SCD Type 2 audit table for {spec.source_table} managed by Lakeflow AUTO CDC",
        )

        # SCD2 Initial Hydration Flow (once=True)
        dp.create_auto_cdc_flow(
            name=spec.history_hydration_flow_name,
            once=True,
            target=spec.target_table_history,
            source=spec.snapshot_source_name,
            keys=[spec.primary_key],
            sequence_by="sequence_number",
            stored_as_scd_type=2,
            track_history_column_list=list(spec.history_track_columns),
            except_column_list=list(spec.excluded_columns),
            ignore_null_updates=config.ignore_null_updates,
        )

        # SCD2 Continuous CDC Flow
        dp.create_auto_cdc_flow(
            name=spec.history_continuous_flow_name,
            target=spec.target_table_history,
            source=spec.cdc_source_name,
            keys=[spec.primary_key],
            sequence_by="sequence_number",
            apply_as_deletes=F.expr("operation = 'DELETE'"),
            stored_as_scd_type=2,
            track_history_column_list=list(spec.history_track_columns),
            except_column_list=list(spec.excluded_columns),
            ignore_null_updates=config.ignore_null_updates,
        )


def register_lakeflow_pipeline(config: LakeflowConfig | None = None) -> None:
    """Orchestrate full Lakeflow Declarative Pipeline registration."""
    cfg = config or LakeflowConfig.from_env()

    for table_name, spec in TABLE_CDC_SPECS.items():
        build_snapshot_projection(table_name, cfg)
        build_cdc_projection(table_name, cfg)
        register_auto_cdc_flows_for_spec(spec, cfg)


# Execute registration when executed as a Lakeflow pipeline script
if __name__ == "__main__" or "pyspark.pipelines" in globals():
    register_lakeflow_pipeline()
