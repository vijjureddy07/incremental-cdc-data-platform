"""Databricks Lakeflow Declarative Pipelines with Managed AUTO CDC.

This module defines declarative streaming tables, initial hydration flows (once=True),
and continuous AUTO CDC ingestion flows targeting SCD Type 1 current-state tables
and SCD Type 2 history tables.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from databricks.lakeflow.config import (
    LakeflowConfig,
    build_snapshot_directory,
)
from databricks.lakeflow.contracts import TABLE_CDC_SPECS, TableCDCSpec
from src.source.schemas import TABLE_SCHEMAS_MAP


def build_snapshot_projection(table_name: str, config: LakeflowConfig) -> str:
    """Register and return the snapshot hydration streaming temporary view name."""
    spec = TABLE_CDC_SPECS[table_name]
    source_name = spec.snapshot_source_name
    base_schema = TABLE_SCHEMAS_MAP[table_name]

    # In Databricks Lakeflow runtime, temporary_view defines intermediate streaming source views
    @dp.temporary_view(
        name=source_name,
        comment=f"Initial snapshot hydration streaming temporary view for {table_name}",
    )
    def snapshot_dataset():
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        snapshot_dir = build_snapshot_directory(table_name, config)

        # Read snapshot Parquet files as a stream via Auto Loader (cloudFiles)
        raw_stream = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.includeExistingFiles", "true")
            .load(snapshot_dir)
        )

        # Project explicit business types matching frozen schema + lineage placeholders
        select_exprs = [
            F.col(f.name).cast(f.dataType).alias(f.name)
            for f in base_schema.fields
        ]
        select_exprs.extend(
            [
                F.lit("SNAPSHOT").cast("string").alias("operation"),
                F.lit(0).cast("long").alias("sequence_number"),
                F.lit(None).cast("string").alias("latest_event_id"),
                F.lit(None).cast("string").alias("latest_event_fingerprint"),
                F.lit(None).cast("string").alias("latest_source_commit_timestamp"),
            ]
        )

        return (
            raw_stream.select(*select_exprs)
            .filter(F.col("sequence_number").isNotNull() & F.col(spec.primary_key).isNotNull())
        )

    return source_name


def build_cdc_projection(table_name: str, config: LakeflowConfig) -> str:
    """Register and return the continuous normalized CDC streaming temporary view name."""
    spec = TABLE_CDC_SPECS[table_name]
    source_name = spec.cdc_source_name
    base_schema = TABLE_SCHEMAS_MAP[table_name]

    @dp.temporary_view(
        name=source_name,
        comment=f"Continuous normalized CDC streaming temporary view for {table_name}",
    )
    def cdc_dataset():
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        cdc_path = f"{config.normalized_cdc_base_path}/*/accepted.jsonl"

        # Read normalized CDC change stream via Databricks Auto Loader (cloudFiles)
        raw_stream = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.includeExistingFiles", "true")
            .load(cdc_path)
        )

        # Filter strictly for this table's events
        table_events = raw_stream.filter(F.col("table_name") == spec.source_table)

        # Project explicit business types matching frozen schema + CDC operational metadata
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

        # Business non-PK columns cast to authoritative domain types
        for f in base_schema.fields:
            if f.name != pk:
                select_exprs.append(F.col(f"payload.{f.name}").cast(f.dataType).alias(f.name))

        # CDC operational and control columns
        select_exprs.extend(
            [
                F.col("operation").cast("string").alias("operation"),
                F.col("sequence_number").cast("long").alias("sequence_number"),
                F.col("event_id").cast("string").alias("latest_event_id"),
                F.col("event_fingerprint").cast("string").alias("latest_event_fingerprint"),
                F.col("source_commit_timestamp").cast("string").alias("latest_source_commit_timestamp"),
            ]
        )

        return (
            table_events.select(*select_exprs)
            .filter(F.col("sequence_number").isNotNull() & F.col(pk).isNotNull())
        )

    return source_name


def register_auto_cdc_flows_for_spec(spec: TableCDCSpec, config: LakeflowConfig) -> None:
    """Register SCD Type 1 current table and optional SCD Type 2 history table with AUTO CDC flows."""
    table_properties = {
        "pipelines.cdc.tombstoneGCThresholdInSeconds": str(config.tombstone_gc_threshold_seconds)
    }

    # ---------------------------------------------------------
    # 1. SCD Type 1 Current-State Target Table
    # ---------------------------------------------------------
    dp.create_streaming_table(
        name=spec.target_table_current,
        comment=f"Current-state {spec.source_table} table managed by Lakeflow AUTO CDC",
        table_properties=table_properties,
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
            table_properties=table_properties,
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

    for spec in TABLE_CDC_SPECS.values():
        build_snapshot_projection(spec.source_table, cfg)
        build_cdc_projection(spec.source_table, cfg)
        register_auto_cdc_flows_for_spec(spec, cfg)


# Register declarative pipeline definitions upon module evaluation
register_lakeflow_pipeline()
