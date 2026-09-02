"""Unit tests for in-memory source adapter, composite cursors, and collision edge cases."""

from src.watermark.models import CompositeWatermark
from src.watermark.source_adapter import InMemorySourceAdapter


def test_source_adapter_capture_high_watermark():
    """Verify high watermark calculation finds the maximum (updated_at, PK) pair."""
    records = [
        {"account_id": "ACC-0001", "updated_at": "2026-01-01T10:00:00Z"},
        {"account_id": "ACC-0003", "updated_at": "2026-01-01T12:00:00Z"},
        {"account_id": "ACC-0002", "updated_at": "2026-01-01T12:00:00Z"},
    ]
    adapter = InMemorySourceAdapter({"accounts": records})
    high = adapter.capture_source_high_watermark("accounts")

    # High watermark should be the highest timestamp and tie-break key
    assert high.timestamp == "2026-01-01T12:00:00Z"
    assert high.key == "ACC-0003"


def test_source_adapter_equal_timestamp_collision_handling():
    """Verify that composite watermark (updated_at, PK) prevents data loss when multiple

    records share the exact same timestamp.

    Example scenario:
    Three accounts updated in the exact same second:
      - (2026-09-02T10:00:00Z, ACC-0100)
      - (2026-09-02T10:00:00Z, ACC-0101)
      - (2026-09-02T10:00:00Z, ACC-0102)

    If previous run committed LOW = (2026-09-02T10:00:00Z, ACC-0100),
    the next run must extract ACC-0101 and ACC-0102 without skipping them.
    """
    ts = "2026-09-02T10:00:00Z"
    records = [
        {"account_id": "ACC-0100", "account_name": "Co 100", "updated_at": ts},
        {"account_id": "ACC-0101", "account_name": "Co 101", "updated_at": ts},
        {"account_id": "ACC-0102", "account_name": "Co 102", "updated_at": ts},
    ]
    adapter = InMemorySourceAdapter({"accounts": records})

    low = CompositeWatermark(timestamp=ts, key="ACC-0100")
    high = CompositeWatermark(timestamp=ts, key="ACC-0102")

    extracted = adapter.extract_bounded_window("accounts", low, high)

    # Must extract exactly ACC-0101 and ACC-0102
    assert len(extracted) == 2
    assert [r["account_id"] for r in extracted] == ["ACC-0101", "ACC-0102"]


def test_source_adapter_bounded_high_isolation():
    """Verify that extraction only includes rows up to the frozen HIGH watermark."""
    records = [
        {"account_id": "ACC-0001", "updated_at": "2026-01-01T10:00:00Z"},
        {"account_id": "ACC-0002", "updated_at": "2026-01-01T11:00:00Z"},
        {"account_id": "ACC-0003", "updated_at": "2026-01-01T12:00:00Z"},
    ]
    adapter = InMemorySourceAdapter({"accounts": records})

    low = CompositeWatermark(timestamp="2026-01-01T10:00:00Z", key="ACC-0001")
    high = CompositeWatermark(timestamp="2026-01-01T11:00:00Z", key="ACC-0002")

    extracted = adapter.extract_bounded_window("accounts", low, high)
    assert len(extracted) == 1
    assert extracted[0]["account_id"] == "ACC-0002"


def test_source_adapter_backdated_updated_at_limitation():
    """Demonstrate architectural limitation: if a record is modified with a backdated

    updated_at timestamp that is less than or equal to the committed LOW watermark,
    watermark extraction will miss the modification.
    """
    records = [
        {"account_id": "ACC-0001", "account_name": "Normal", "updated_at": "2026-01-01T12:00:00Z"},
        {
            "account_id": "ACC-0002",
            "account_name": "Backdated Update",
            "updated_at": "2026-01-01T08:00:00Z",
        },
    ]
    adapter = InMemorySourceAdapter({"accounts": records})

    # Committed LOW watermark is 10:00:00Z
    low = CompositeWatermark(timestamp="2026-01-01T10:00:00Z", key="ACC-0001")
    high = CompositeWatermark(timestamp="2026-01-01T12:00:00Z", key="ACC-0001")

    extracted = adapter.extract_bounded_window("accounts", low, high)

    # ACC-0002 is missed because its backdated updated_at (08:00:00Z) <= LOW (10:00:00Z)
    extracted_keys = [r["account_id"] for r in extracted]
    assert "ACC-0001" in extracted_keys
    assert "ACC-0002" not in extracted_keys
