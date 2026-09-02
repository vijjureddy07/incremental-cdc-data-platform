"""Adapter for converting accepted normalized CDC events into typed PySpark DataFrames."""

import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructType,
    TimestampType,
)

from src.merge.models import TARGET_METADATA_FIELDS
from src.normalization.models import NormalizedCDCEvent
from src.source.schemas import TABLE_SCHEMAS_MAP
from src.utils.helpers import parse_iso_date, parse_iso_timestamp


def extract_processing_id_from_path(path: str | Path) -> str | None:
    """Extract processing_id from directory path (e.g. .../processing_id=proc_123/accepted.jsonl)."""
    match = re.search(r"processing_id=([^/\\]+)", str(path))
    if match:
        return match.group(1)
    p = Path(path)
    if p.parent.name.startswith("processing_id="):
        return p.parent.name.split("=", 1)[1]
    return None


def load_accepted_events_from_file(
    file_path: str | Path,
) -> list[NormalizedCDCEvent]:
    """Read a normalized accepted.jsonl file into NormalizedCDCEvent objects.

    Args:
        file_path: Path to accepted.jsonl file.

    Returns:
        List of NormalizedCDCEvent instances.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Accepted events file not found at: {path}")

    events: list[NormalizedCDCEvent] = []

    with open(path, encoding="utf-8") as f:
        for line in f:
            clean = line.strip()
            if not clean:
                continue
            data = json.loads(clean)
            events.append(NormalizedCDCEvent.from_dict(data))

    return events


def convert_events_to_spark_df(
    table_name: str,
    events: list[NormalizedCDCEvent],
    spark: SparkSession,
    processing_id: str,
) -> DataFrame:
    """Convert a list of accepted NormalizedCDCEvents for a table into a typed Spark DataFrame."""
    if table_name not in TABLE_SCHEMAS_MAP:
        raise ValueError(f"Unknown table name: {table_name}")

    base_schema = TABLE_SCHEMAS_MAP[table_name]
    full_schema = StructType(list(base_schema.fields) + list(TARGET_METADATA_FIELDS))

    rows: list[dict[str, Any]] = []
    for ev in events:
        if ev.table_name != table_name:
            continue

        raw_data = ev.payload if ev.payload is not None else (ev.before_payload or {})
        row_dict: dict[str, Any] = {}

        for field in base_schema.fields:
            val = raw_data.get(field.name)
            if val is None:
                # If field missing in payload, check business_key
                val = ev.business_key.get(field.name)

            if val is None:
                row_dict[field.name] = None
            elif isinstance(field.dataType, DecimalType):
                row_dict[field.name] = Decimal(str(val))
            elif isinstance(field.dataType, DateType):
                row_dict[field.name] = parse_iso_date(str(val)) if isinstance(val, str) else val
            elif isinstance(field.dataType, TimestampType):
                row_dict[field.name] = (
                    parse_iso_timestamp(str(val)) if isinstance(val, str) else val
                )
            elif isinstance(field.dataType, StringType):
                row_dict[field.name] = str(val)
            else:
                row_dict[field.name] = val

        # Populate CDC target metadata fields
        is_del = ev.operation == "DELETE"
        row_dict["_last_sequence_number"] = ev.sequence_number
        row_dict["_last_event_id"] = ev.event_id
        row_dict["_last_operation"] = ev.operation
        row_dict["_last_event_fingerprint"] = ev.event_fingerprint
        row_dict["_last_source_commit_timestamp"] = ev.source_commit_timestamp
        row_dict["_last_processing_id"] = processing_id
        row_dict["_is_deleted"] = is_del
        row_dict["_deleted_at"] = ev.source_commit_timestamp if is_del else None

        rows.append(row_dict)

    return spark.createDataFrame(rows, schema=full_schema)
