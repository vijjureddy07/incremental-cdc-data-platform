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
    target_prefix: str = ""
    ignore_null_updates: bool = False  # Module 3 produces full after-images; nulls represent intentional field resets

    @classmethod
    def from_env(cls) -> "LakeflowConfig":
        """Instantiate configuration from environment variables with safe defaults."""
        return cls(
            catalog=os.getenv("LAKEFLOW_CATALOG", "main"),
            schema=os.getenv("LAKEFLOW_SCHEMA", "cdc_portfolio"),
            snapshot_base_path=os.getenv(
                "LAKEFLOW_SNAPSHOT_PATH",
                "/Volumes/main/cdc_portfolio/cdc_data/source_snapshot",
            ),
            normalized_cdc_base_path=os.getenv(
                "LAKEFLOW_NORMALIZED_CDC_PATH",
                "/Volumes/main/cdc_portfolio/cdc_data/normalized_cdc",
            ),
            tombstone_gc_threshold_seconds=int(
                os.getenv("LAKEFLOW_TOMBSTONE_GC_SECONDS", "604800")
            ),
            target_prefix=os.getenv("LAKEFLOW_TARGET_PREFIX", ""),
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

        return cls(
            catalog=get_conf("lakeflow.catalog", os.getenv("LAKEFLOW_CATALOG", "main")),
            schema=get_conf("lakeflow.schema", os.getenv("LAKEFLOW_SCHEMA", "cdc_portfolio")),
            snapshot_base_path=get_conf(
                "lakeflow.snapshot_base_path",
                os.getenv("LAKEFLOW_SNAPSHOT_PATH", "/Volumes/main/cdc_portfolio/cdc_data/source_snapshot"),
            ),
            normalized_cdc_base_path=get_conf(
                "lakeflow.normalized_cdc_base_path",
                os.getenv("LAKEFLOW_NORMALIZED_CDC_PATH", "/Volumes/main/cdc_portfolio/cdc_data/normalized_cdc"),
            ),
            tombstone_gc_threshold_seconds=int(
                get_conf("lakeflow.tombstone_gc_seconds", os.getenv("LAKEFLOW_TOMBSTONE_GC_SECONDS", "604800"))
            ),
            target_prefix=get_conf("lakeflow.target_prefix", os.getenv("LAKEFLOW_TARGET_PREFIX", "")),
            ignore_null_updates=get_conf(
                "lakeflow.ignore_null_updates", os.getenv("LAKEFLOW_IGNORE_NULL_UPDATES", "false")
            ).lower() == "true",
        )
