"""Raw CDC file reader with fault-tolerant malformed JSON isolation."""

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.normalization.models import QuarantinedEvent, QuarantineReasonCode
from src.utils.helpers import format_iso_timestamp


def extract_batch_id_from_path(path: Path) -> str:
    """Extract batch_id from directory pattern batch_id=<id> or default to parent directory name."""
    match = re.search(r"batch_id=([^/\\]+)", str(path))
    if match:
        return match.group(1)
    return path.parent.name or "unknown_batch"


def read_raw_cdc_files(
    file_paths: list[str | Path],
) -> tuple[list[dict[str, Any]], list[QuarantinedEvent]]:
    """Read a collection of raw CDC JSONL files, isolating malformed lines into quarantine.

    Returns:
        tuple of (parsed_records, malformed_json_quarantined_events)
    """
    parsed_records: list[dict[str, Any]] = []
    quarantined_events: list[QuarantinedEvent] = []
    ingestion_order = 0

    now_str = format_iso_timestamp(datetime.now(UTC))

    for p in file_paths:
        path = Path(p)
        if not path.exists():
            continue

        batch_id_hint = extract_batch_id_from_path(path)
        source_file_str = str(path)

        with open(path, encoding="utf-8") as f:
            for line_no, raw_line in enumerate(f, start=1):
                clean_line = raw_line.strip()
                if not clean_line:
                    continue

                ingestion_order += 1

                try:
                    record = json.loads(clean_line)
                    if not isinstance(record, dict):
                        quarantined_events.append(
                            QuarantinedEvent(
                                quarantine_code=QuarantineReasonCode.MALFORMED_JSON,
                                quarantine_reason=(
                                    f"Line {line_no} in {path.name} is not a JSON object: "
                                    f"got {type(record).__name__}"
                                ),
                                raw_record=clean_line,
                                source_file=source_file_str,
                                batch_id=batch_id_hint,
                                quarantined_at=now_str,
                            )
                        )
                        continue

                    # Attach raw ingestion provenance metadata
                    record["source_file"] = source_file_str
                    record["ingestion_batch_id"] = record.get("batch_id") or batch_id_hint
                    record["ingestion_order"] = ingestion_order
                    parsed_records.append(record)

                except json.JSONDecodeError as err:
                    quarantined_events.append(
                        QuarantinedEvent(
                            quarantine_code=QuarantineReasonCode.MALFORMED_JSON,
                            quarantine_reason=f"JSON decode error at line {line_no} in {path.name}: {err.msg}",
                            raw_record=clean_line,
                            source_file=source_file_str,
                            batch_id=batch_id_hint,
                            quarantined_at=now_str,
                        )
                    )

    return parsed_records, quarantined_events
