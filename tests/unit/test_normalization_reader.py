"""Unit tests for raw CDC file reader and malformed JSON line isolation."""

import json
import tempfile
from pathlib import Path

from src.normalization.models import QuarantineReasonCode
from src.normalization.reader import read_raw_cdc_files


def test_read_valid_jsonl_files():
    """Verify reading valid JSONL files populates records and attaches ingestion metadata."""
    with tempfile.TemporaryDirectory() as tmpdir:
        batch_dir = Path(tmpdir) / "batch_id=batch_001"
        batch_dir.mkdir(parents=True)
        acc_file = batch_dir / "accounts.jsonl"

        event = {
            "event_id": "evt_001",
            "table_name": "accounts",
            "operation": "INSERT",
            "business_key": {"account_id": "ACC-0001"},
            "sequence_number": 1,
            "event_timestamp": "2026-05-11T01:00:00Z",
            "source_commit_timestamp": "2026-05-11T01:00:01Z",
            "batch_id": "batch_001",
            "payload": {"account_id": "ACC-0001"},
            "before_payload": None,
            "source_system": "b2b_saas_postgres",
        }

        with open(acc_file, "w", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")

        records, quarantine = read_raw_cdc_files([acc_file])

        assert len(records) == 1
        assert len(quarantine) == 0
        assert records[0]["event_id"] == "evt_001"
        assert records[0]["ingestion_batch_id"] == "batch_001"
        assert records[0]["source_file"] == str(acc_file)
        assert records[0]["ingestion_order"] == 1


def test_read_malformed_json_lines_isolated():
    """Verify malformed non-JSON lines are captured into quarantine without dropping valid rows."""
    with tempfile.TemporaryDirectory() as tmpdir:
        batch_dir = Path(tmpdir) / "batch_id=batch_corrupt"
        batch_dir.mkdir(parents=True)
        corrupt_file = batch_dir / "accounts.jsonl"

        valid_event = {
            "event_id": "evt_valid_001",
            "table_name": "accounts",
            "operation": "INSERT",
            "business_key": {"account_id": "ACC-0001"},
            "sequence_number": 1,
            "event_timestamp": "2026-05-11T01:00:00Z",
            "source_commit_timestamp": "2026-05-11T01:00:01Z",
            "batch_id": "batch_corrupt",
            "payload": {"account_id": "ACC-0001"},
            "before_payload": None,
            "source_system": "b2b_saas_postgres",
        }

        with open(corrupt_file, "w", encoding="utf-8") as f:
            f.write(json.dumps(valid_event) + "\n")
            f.write("{NOT_VALID_JSON_LINE\n")  # Corrupted line
            f.write('"not_a_json_object"\n')  # Non-dict JSON

        records, quarantine = read_raw_cdc_files([corrupt_file])

        assert len(records) == 1
        assert records[0]["event_id"] == "evt_valid_001"

        assert len(quarantine) == 2
        assert quarantine[0].quarantine_code == QuarantineReasonCode.MALFORMED_JSON
        assert "{NOT_VALID_JSON_LINE" in str(quarantine[0].raw_record)
        assert quarantine[1].quarantine_code == QuarantineReasonCode.MALFORMED_JSON
