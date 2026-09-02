"""Unit tests for watermark domain models and composite cursor comparison semantics."""

from src.watermark.models import (
    CompositeWatermark,
    ExtractionResult,
    WatermarkRunAudit,
    WatermarkRunStatus,
    WatermarkState,
)


def test_composite_watermark_initial_state():
    """Verify initial uncommitted watermark properties."""
    cw = CompositeWatermark(None, None)
    assert cw.is_initial
    assert cw.timestamp is None
    assert cw.key is None
    assert "INITIAL" in str(cw)


def test_composite_watermark_comparisons():
    """Verify strict ordering rules:

    1. None (initial) is smaller than any populated timestamp
    2. Earlier timestamp is smaller than later timestamp
    3. Equal timestamps are broken by primary key tie-breaker
    """
    initial = CompositeWatermark(None, None)
    w1 = CompositeWatermark("2026-01-01T10:00:00Z", "ACC-0001")
    w2 = CompositeWatermark("2026-01-01T10:00:00Z", "ACC-0002")
    w3 = CompositeWatermark("2026-01-01T11:00:00Z", "ACC-0001")

    # Initial comparisons
    assert initial < w1
    assert initial <= w1
    assert w1 > initial
    assert w1 >= initial
    assert initial != w1

    # Same timestamp, different key tie-breaker
    assert w1 < w2
    assert w1 <= w2
    assert w2 > w1
    assert w2 >= w1
    assert w1 != w2

    # Different timestamps
    assert w1 < w3
    assert w2 < w3
    assert w3 > w2


def test_composite_watermark_equality_and_serialization():
    """Verify equality checks and dictionary serialization/deserialization."""
    w1 = CompositeWatermark("2026-01-01T10:00:00Z", "ACC-0001")
    w2 = CompositeWatermark("2026-01-01T10:00:00Z", "ACC-0001")
    assert w1 == w2

    d = w1.to_dict()
    assert d == {"timestamp": "2026-01-01T10:00:00Z", "key": "ACC-0001"}

    w_deser = CompositeWatermark.from_dict(d)
    assert w_deser == w1

    # Deserializing None or empty dict produces initial watermark
    assert CompositeWatermark.from_dict(None).is_initial
    assert CompositeWatermark.from_dict({}).is_initial


def test_watermark_state_serialization():
    """Verify WatermarkState dictionary conversion."""
    state = WatermarkState(
        table_name="accounts",
        watermark_column="updated_at",
        tie_breaker_column="account_id",
        last_watermark=CompositeWatermark("2026-01-16T09:00:00Z", "ACC-0040"),
        version=3,
        last_success_run_id="run_acc_123",
        updated_at="2026-01-16T09:05:00Z",
    )
    d = state.to_dict()
    assert d["table_name"] == "accounts"
    assert d["last_watermark_timestamp"] == "2026-01-16T09:00:00Z"
    assert d["last_watermark_key"] == "ACC-0040"
    assert d["version"] == 3


def test_watermark_run_audit_serialization():
    """Verify WatermarkRunAudit conversion."""
    audit = WatermarkRunAudit(
        run_id="run_001",
        table_name="subscriptions",
        batch_id="batch_sub_abc",
        low_watermark=CompositeWatermark(None, None),
        high_watermark=CompositeWatermark("2026-03-16T00:00:00Z", "SUB-0060"),
        status=WatermarkRunStatus.SUCCESS,
        rows_extracted=60,
        landing_path="/data/watermark_landing/table=subscriptions/batch_sub_abc/data.jsonl",
        started_at="2026-03-16T01:00:00Z",
        completed_at="2026-03-16T01:00:02Z",
    )
    d = audit.to_dict()
    assert d["status"] == "SUCCESS"
    assert d["rows_extracted"] == 60
    assert d["low_watermark_timestamp"] is None
    assert d["high_watermark_key"] == "SUB-0060"


def test_extraction_result_model():
    """Verify ExtractionResult attributes."""
    res = ExtractionResult(
        table_name="invoices",
        run_id="run_inv_1",
        batch_id="batch_inv_1",
        status=WatermarkRunStatus.NO_DATA,
        low_watermark=CompositeWatermark("2026-04-01T00:00:00Z", "INV-0120"),
        high_watermark=CompositeWatermark("2026-04-01T00:00:00Z", "INV-0120"),
        rows_extracted=0,
        landing_path=None,
        records=[],
    )
    assert res.status == WatermarkRunStatus.NO_DATA
    assert res.rows_extracted == 0
    assert len(res.records) == 0
