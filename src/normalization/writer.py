"""Atomic output writers and readers for normalized CDC change streams and quarantine stores."""

import json
import uuid
from pathlib import Path

from src.normalization.models import NormalizedCDCEvent, QuarantinedEvent


def write_normalized_accepted_jsonl(
    processing_id: str,
    events: list[NormalizedCDCEvent],
    base_dir: str | Path = "data/normalized_cdc",
) -> Path:
    """Atomically write accepted normalized CDC events into a deterministic partition directory."""
    target_dir = Path(base_dir) / f"processing_id={processing_id}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "accepted.jsonl"

    temp_file = target_dir / f".tmp_{uuid.uuid4().hex[:8]}.jsonl"
    with open(temp_file, "w", encoding="utf-8") as f:
        for ev in events:
            line = json.dumps(ev.to_dict(), sort_keys=True, separators=(",", ":"))
            f.write(line + "\n")

    temp_file.replace(target_file)
    return target_file


def write_quarantine_jsonl(
    processing_id: str,
    events: list[QuarantinedEvent],
    base_dir: str | Path = "data/quarantine",
) -> Path:
    """Atomically write quarantined dead-letter events into a deterministic partition directory."""
    target_dir = Path(base_dir) / f"processing_id={processing_id}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "quarantine.jsonl"

    temp_file = target_dir / f".tmp_{uuid.uuid4().hex[:8]}.jsonl"
    with open(temp_file, "w", encoding="utf-8") as f:
        for q in events:
            line = json.dumps(q.to_dict(), sort_keys=True, separators=(",", ":"))
            f.write(line + "\n")

    temp_file.replace(target_file)
    return target_file


def read_normalized_accepted_jsonl(file_path: str | Path) -> list[dict]:
    """Read a normalized accepted JSONL file back from disk."""
    path = Path(file_path)
    if not path.exists():
        return []
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            clean = line.strip()
            if clean:
                records.append(json.loads(clean))
    return records


def read_quarantine_jsonl(file_path: str | Path) -> list[dict]:
    """Read a quarantine JSONL file back from disk."""
    path = Path(file_path)
    if not path.exists():
        return []
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            clean = line.strip()
            if clean:
                records.append(json.loads(clean))
    return records
