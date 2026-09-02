"""Databricks Lakeflow Declarative Pipelines and AUTO CDC package."""

from databricks.lakeflow.config import LakeflowConfig
from databricks.lakeflow.contracts import TABLE_CDC_SPECS, TableCDCSpec

__all__ = [
    "LakeflowConfig",
    "TableCDCSpec",
    "TABLE_CDC_SPECS",
]
