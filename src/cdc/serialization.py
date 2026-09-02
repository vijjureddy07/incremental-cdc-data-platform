"""Deterministic JSON Lines Serialization and Landing for CDC Batches."""

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from src.cdc.models import CDCEvent
from src.utils.helpers import ensure_dir, safe_json_default


def serialize_event_to_json(event: CDCEvent | dict[str, Any]) -> str:
    """Serialize a CDCEvent or event dictionary to a deterministic single-line JSON string."""
    event_dict = event.to_dict() if isinstance(event, CDCEvent) else event
    return json.dumps(
        event_dict,
        sort_keys=True,
        default=safe_json_default,
        ensure_ascii=False,
    )


def deserialize_event_from_json(json_str: str) -> CDCEvent:
    """Deserialize a JSON string into a strongly typed CDCEvent object."""
    data = json.loads(json_str)
    return CDCEvent.from_dict(data)


def write_cdc_batch_jsonl(
    events: list[CDCEvent | dict[str, Any]],
    output_base_dir: Path | str = "data/cdc_landing",
) -> dict[str, Path]:
    """Write CDC events to local partitioned JSONL files.

    Files are written to:
    output_base_dir/batch_id=<batch_id>/<table_name>.jsonl

    Args:
        events: List of CDCEvent objects or raw event dictionaries.
        output_base_dir: Base directory for landing CDC files.

    Returns:
        Dictionary mapping "(batch_id, table_name)" to written file Paths.
    """
    base_path = Path(output_base_dir)

    # Group events by batch_id and table_name
    grouped_events: dict[tuple[str, str], list[CDCEvent | dict[str, Any]]] = defaultdict(list)
    for event in events:
        batch_id = (
            event.batch_id
            if isinstance(event, CDCEvent)
            else event.get("batch_id", "batch_unknown")
        )
        table_name = (
            event.table_name if isinstance(event, CDCEvent) else event.get("table_name", "unknown")
        )
        grouped_events[(batch_id, table_name)].append(event)

    written_paths: dict[str, Path] = {}

    for (batch_id, table_name), batch_events in grouped_events.items():
        batch_dir = ensure_dir(base_path / f"batch_id={batch_id}")
        file_path = batch_dir / f"{table_name}.jsonl"

        with open(file_path, "w", encoding="utf-8") as f:
            for ev in batch_events:
                json_line = serialize_event_to_json(ev)
                f.write(json_line + "\n")

        written_paths[f"{batch_id}/{table_name}"] = file_path

    return written_paths


def read_cdc_batch_jsonl(file_path: Path | str) -> list[CDCEvent]:
    """Read a JSONL file and parse into a list of CDCEvent instances."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"CDC batch file not found: {path}")

    events: list[CDCEvent] = []
    with open(path, encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            clean_line = line.strip()
            if not clean_line:
                continue
            try:
                events.append(deserialize_event_from_json(clean_line))
            except Exception as e:
                raise ValueError(f"Failed to parse CDC event at {path}:{line_num} - {e}") from e

    return events
