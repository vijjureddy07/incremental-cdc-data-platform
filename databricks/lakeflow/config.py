"""Configuration contract for Databricks Lakeflow Declarative Pipelines and AUTO CDC."""

import os
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LakeflowConfig:
    """Deployment configuration for Databricks Lakeflow declarative pipelines."""

    catalog: str = "main"
    schema: str = "cdc_portfolio"
    snapshot_base_path: str = "/Volumes/main/cdc_portfolio/cdc_data/source_snapshot"
    normalized_cdc_base_path: str = "/Volumes/main/cdc_portfolio/cdc_data/normalized_cdc"
    tombstone_gc_threshold_seconds: int = 604800  # 7 days conservative default (must exceed max event lag)
    ignore_null_updates: bool = False  # Module 3 produces full after-images; nulls represent intentional field resets

    @classmethod
    def from_env(cls) -> "LakeflowConfig":
        """Instantiate configuration from environment variables with safe defaults and dynamic Volume paths."""
        catalog = os.getenv("LAKEFLOW_CATALOG", "main")
        schema = os.getenv("LAKEFLOW_SCHEMA", "cdc_portfolio")
        default_snapshot = f"/Volumes/{catalog}/{schema}/cdc_data/source_snapshot"
        default_cdc = f"/Volumes/{catalog}/{schema}/cdc_data/normalized_cdc"

        return cls(
            catalog=catalog,
            schema=schema,
            snapshot_base_path=os.getenv("LAKEFLOW_SNAPSHOT_PATH", default_snapshot),
            normalized_cdc_base_path=os.getenv("LAKEFLOW_NORMALIZED_CDC_PATH", default_cdc),
            tombstone_gc_threshold_seconds=int(
                os.getenv("LAKEFLOW_TOMBSTONE_GC_SECONDS", "604800")
            ),
            ignore_null_updates=os.getenv("LAKEFLOW_IGNORE_NULL_UPDATES", "false").lower() == "true",
        )

    @classmethod
    def from_spark_conf(cls, spark_session: Any) -> "LakeflowConfig":
        """Instantiate configuration from active Spark session conf with safe fallbacks."""
        conf = getattr(spark_session, "conf", None)
        if conf is None:
            return cls.from_env()

        def get_conf(key: str, default: str) -> str:
            try:
                return conf.get(key, default)
            except Exception:
                return default

        catalog = get_conf("lakeflow.catalog", os.getenv("LAKEFLOW_CATALOG", "main"))
        schema = get_conf("lakeflow.schema", os.getenv("LAKEFLOW_SCHEMA", "cdc_portfolio"))
        default_snapshot = f"/Volumes/{catalog}/{schema}/cdc_data/source_snapshot"
        default_cdc = f"/Volumes/{catalog}/{schema}/cdc_data/normalized_cdc"

        return cls(
            catalog=catalog,
            schema=schema,
            snapshot_base_path=get_conf(
                "lakeflow.snapshot_base_path",
                os.getenv("LAKEFLOW_SNAPSHOT_PATH", default_snapshot),
            ),
            normalized_cdc_base_path=get_conf(
                "lakeflow.normalized_cdc_base_path",
                os.getenv("LAKEFLOW_NORMALIZED_CDC_PATH", default_cdc),
            ),
            tombstone_gc_threshold_seconds=int(
                get_conf("lakeflow.tombstone_gc_seconds", os.getenv("LAKEFLOW_TOMBSTONE_GC_SECONDS", "604800"))
            ),
            ignore_null_updates=get_conf(
                "lakeflow.ignore_null_updates", os.getenv("LAKEFLOW_IGNORE_NULL_UPDATES", "false")
            ).lower() == "true",
        )


def build_snapshot_path(table_name: str, config: LakeflowConfig) -> str:
    """Construct the Parquet snapshot file path matching Module 1 directory layout."""
    return f"{config.snapshot_base_path}/{table_name}/snapshot.parquet"


def build_snapshot_directory(table_name: str, config: LakeflowConfig) -> str:
    """Construct the Parquet snapshot directory path matching Module 1 directory layout."""
    return f"{config.snapshot_base_path}/{table_name}"
