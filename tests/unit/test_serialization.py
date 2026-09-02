"""Unit tests for deterministic JSONL serialization and file I/O."""

import json
import tempfile

from src.cdc.models import CDCEvent
from src.cdc.serialization import (
    read_cdc_batch_jsonl,
    serialize_event_to_json,
    write_cdc_batch_jsonl,
)


def test_deterministic_json_serialization():
    """Verify serialize_event_to_json produces sorted keys and identical output across runs."""
    ev = CDCEvent(
        event_id="evt_ser_001",
        table_name="invoices",
        operation="INSERT",
        business_key={"invoice_id": "INV-0001"},
        sequence_number=1,
        event_timestamp="2026-01-01T00:00:00Z",
        source_commit_timestamp="2026-01-01T00:00:01Z",
        batch_id="batch_001",
        payload={"invoice_id": "INV-0001", "invoice_amount": "199.00"},
        before_payload=None,
    )

    ser1 = serialize_event_to_json(ev)
    ser2 = serialize_event_to_json(ev)
    assert ser1 == ser2

    # Check key order is alphabetical
    parsed = json.loads(ser1)
    keys = list(parsed.keys())
    assert keys == sorted(keys)


def test_batch_write_and_read_jsonl():
    """Verify write_cdc_batch_jsonl writes partition files and read_cdc_batch_jsonl reads them back."""
    events = [
        CDCEvent(
            event_id="evt_io_001",
            table_name="accounts",
            operation="INSERT",
            business_key={"account_id": "ACC-0041"},
            sequence_number=1,
            event_timestamp="2026-04-01T10:00:00Z",
            source_commit_timestamp="2026-04-01T10:00:01Z",
            batch_id="batch_001",
            payload={"account_id": "ACC-0041", "status": "ACTIVE"},
            before_payload=None,
        ),
        CDCEvent(
            event_id="evt_io_002",
            table_name="subscriptions",
            operation="INSERT",
            business_key={"subscription_id": "SUB-0061"},
            sequence_number=1,
            event_timestamp="2026-04-01T10:05:00Z",
            source_commit_timestamp="2026-04-01T10:05:01Z",
            batch_id="batch_001",
            payload={"subscription_id": "SUB-0061", "plan_name": "GROWTH"},
            before_payload=None,
        ),
    ]

    with tempfile.TemporaryDirectory() as tmp_dir:
        written_map = write_cdc_batch_jsonl(events, output_base_dir=tmp_dir)

        assert "batch_001/accounts" in written_map
        assert "batch_001/subscriptions" in written_map

        acc_file = written_map["batch_001/accounts"]
        assert acc_file.exists()

        loaded_acc_events = read_cdc_batch_jsonl(acc_file)
        assert len(loaded_acc_events) == 1
        assert loaded_acc_events[0].event_id == "evt_io_001"
        assert loaded_acc_events[0].table_name == "accounts"
        assert loaded_acc_events[0].payload["account_id"] == "ACC-0041"
