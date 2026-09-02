"""Unit tests for deterministic landing area serialization and partition layout."""

import tempfile

from src.watermark.landing import (
    generate_deterministic_batch_id,
    get_batch_landing_dir,
    read_watermark_batch_jsonl,
    write_watermark_batch_jsonl,
)
from src.watermark.models import CompositeWatermark


def test_deterministic_batch_id_stability():
    """Verify that the same (table, LOW, HIGH) parameters always generate identical batch_id."""
    low = CompositeWatermark("2026-01-01T00:00:00Z", "ACC-0001")
    high = CompositeWatermark("2026-01-02T00:00:00Z", "ACC-0040")

    id_1 = generate_deterministic_batch_id("accounts", low, high)
    id_2 = generate_deterministic_batch_id("accounts", low, high)
    assert id_1 == id_2
    assert id_1.startswith("batch_accounts_")

    # Different table produces different batch_id
    id_sub = generate_deterministic_batch_id("subscriptions", low, high)
    assert id_sub != id_1

    # Different watermark produces different batch_id
    high_diff = CompositeWatermark("2026-01-02T00:00:00Z", "ACC-0041")
    id_diff = generate_deterministic_batch_id("accounts", low, high_diff)
    assert id_diff != id_1


def test_landing_directory_layout():
    """Verify partition directory naming convention."""
    path = get_batch_landing_dir("subscriptions", "batch_sub_123", "/base")
    assert str(path) == "/base/table=subscriptions/batch_id=batch_sub_123"


def test_atomic_write_and_read_roundtrip():
    """Verify writing records to JSONL and reading them back."""
    records = [
        {"account_id": "ACC-0001", "name": "Company A", "amount": "100.50"},
        {"account_id": "ACC-0002", "name": "Company B", "amount": "250.00"},
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        batch_id = "batch_acc_test"
        written_path = write_watermark_batch_jsonl("accounts", batch_id, records, base_dir=tmpdir)
        assert written_path.exists()
        assert written_path.name == "data.jsonl"

        read_records = read_watermark_batch_jsonl(written_path)
        assert len(read_records) == 2
        assert read_records[0]["account_id"] == "ACC-0001"
        assert read_records[1]["amount"] == "250.00"


def test_landing_idempotent_retry_overwrite():
    """Verify that retrying a write for the same batch replaces the content cleanly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        batch_id = "batch_acc_retry"
        records_attempt_1 = [{"account_id": "ACC-0001", "status": "PENDING"}]
        records_attempt_2 = [{"account_id": "ACC-0001", "status": "ACTIVE"}]

        # Write attempt 1
        p1 = write_watermark_batch_jsonl("accounts", batch_id, records_attempt_1, base_dir=tmpdir)
        read1 = read_watermark_batch_jsonl(p1)
        assert read1[0]["status"] == "PENDING"

        # Write attempt 2 (retry)
        p2 = write_watermark_batch_jsonl("accounts", batch_id, records_attempt_2, base_dir=tmpdir)
        assert p1 == p2
        read2 = read_watermark_batch_jsonl(p2)
        assert len(read2) == 1
        assert read2[0]["status"] == "ACTIVE"
