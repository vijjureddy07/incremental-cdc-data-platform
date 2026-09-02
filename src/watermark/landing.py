"""Deterministic landing area manager for watermark incremental extraction batches."""

import hashlib
import json
import os
from pathlib import Path
from typing import Any

from src.utils.helpers import safe_json_default
from src.watermark.models import CompositeWatermark


def generate_deterministic_batch_id(
    table_name: str,
    low_watermark: CompositeWatermark,
    high_watermark: CompositeWatermark,
) -> str:
    """Generate a deterministic SHA-256 batch identity for a logical extraction window.

    A retry of the exact same (table, LOW, HIGH) extraction window always computes the same batch_id.
    """
    raw_payload = (
        f"{table_name}:"
        f"{low_watermark.timestamp or 'MIN'}:"
        f"{low_watermark.key or 'MIN'}:"
        f"{high_watermark.timestamp or 'MIN'}:"
        f"{high_watermark.key or 'MIN'}"
    )
    digest = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()[:16]
    return f"batch_{table_name}_{digest}"


def get_batch_landing_dir(
    table_name: str,
    batch_id: str,
    base_dir: str | Path = "data/watermark_landing",
) -> Path:
    """Get the target directory path for a partitioned watermark batch."""
    return Path(base_dir) / f"table={table_name}" / f"batch_id={batch_id}"


def write_watermark_batch_jsonl(
    table_name: str,
    batch_id: str,
    records: list[dict[str, Any]],
    base_dir: str | Path = "data/watermark_landing",
) -> Path:
    """Write extracted records atomically to a deterministic JSONL landing path."""
    batch_dir = get_batch_landing_dir(table_name, batch_id, base_dir)
    batch_dir.mkdir(parents=True, exist_ok=True)

    target_file = batch_dir / "data.jsonl"
    temp_file = batch_dir / f"data.jsonl.tmp.{os.getpid()}"

    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            for record in records:
                line = json.dumps(
                    record,
                    sort_keys=True,
                    default=safe_json_default,
                    ensure_ascii=False,
                )
                f.write(line + "\n")

        # Atomic replacement of target file
        temp_file.replace(target_file)
        return target_file
    except Exception:
        if temp_file.exists():
            temp_file.unlink()
        raise


def read_watermark_batch_jsonl(file_path: str | Path) -> list[dict[str, Any]]:
    """Read a landed JSON Lines batch back into a list of record dictionaries."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Landing batch file not found: {file_path}")

    records: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line_str = line.strip()
            if line_str:
                records.append(json.loads(line_str))
    return records
