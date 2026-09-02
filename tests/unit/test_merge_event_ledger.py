"""Unit tests for Delta Lake applied event ledger operations."""

import tempfile

from pyspark.sql import SparkSession

from src.merge.event_ledger import EventApplyLedger
from src.merge.models import LedgerStatus
from src.normalization.models import NormalizedCDCEvent


def _make_sample_event(
    event_id: str = "evt_001",
    entity_key: str = 'accounts:{"account_id":"ACC-0001"}',
    sequence_number: int = 10,
    fingerprint: str = "fp_abc123",
) -> NormalizedCDCEvent:
    return NormalizedCDCEvent(
        event_id=event_id,
        table_name="accounts",
        operation="INSERT",
        business_key={"account_id": "ACC-0001"},
        sequence_number=sequence_number,
        event_timestamp="2026-05-11T01:00:00Z",
        source_commit_timestamp="2026-05-11T01:00:01Z",
        batch_id="batch_001",
        payload={"account_id": "ACC-0001", "account_name": "Test Co"},
        before_payload=None,
        source_system="b2b_saas_postgres",
        entity_sequence_key=entity_key,
        business_key_canonical='{"account_id":"ACC-0001"}',
        event_fingerprint=fingerprint,
        is_late_arrival=False,
        source_file="batch_id=batch_001/accounts.jsonl",
        ingestion_batch_id="batch_001",
    )


def test_event_ledger_initialization_and_schema(spark_session: SparkSession):
    """Verify ledger initializes with correct empty Delta table and schema."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ledger = EventApplyLedger(spark=spark_session, ledger_base_dir=tmpdir)
        ledger.initialize_ledger()

        assert ledger.ledger_exists()
        df = ledger.read_ledger()
        assert df.count() == 0
        assert "event_id" in df.columns
        assert "status" in df.columns
        assert "entity_sequence_key" in df.columns


def test_event_ledger_record_pending_events(spark_session: SparkSession):
    """Verify recording events in ledger assigns PENDING status."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ledger = EventApplyLedger(spark=spark_session, ledger_base_dir=tmpdir)
        ev1 = _make_sample_event("evt_001")
        ev2 = _make_sample_event("evt_002", sequence_number=11)

        ledger.record_pending_events([ev1, ev2])

        pending = ledger.get_pending_records()
        assert len(pending) == 2
        assert {p.event_id for p in pending} == {"evt_001", "evt_002"}
        assert all(p.status == LedgerStatus.PENDING.value for p in pending)


def test_event_ledger_mark_applied(spark_session: SparkSession):
    """Verify transitioning events from PENDING to APPLIED."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ledger = EventApplyLedger(spark=spark_session, ledger_base_dir=tmpdir)
        ev1 = _make_sample_event("evt_001")
        ev2 = _make_sample_event("evt_002", sequence_number=11)

        ledger.record_pending_events([ev1, ev2])
        ledger.mark_events_applied(["evt_001"])

        all_recs = ledger.get_all_ledger_records()
        rec_by_id = {r.event_id: r for r in all_recs}
        assert rec_by_id["evt_001"].status == LedgerStatus.APPLIED.value
        assert rec_by_id["evt_002"].status == LedgerStatus.PENDING.value

        # Only evt_002 is still pending
        pending = ledger.get_pending_records()
        assert len(pending) == 1
        assert pending[0].event_id == "evt_002"


def test_event_ledger_get_applied_max_sequences(spark_session: SparkSession):
    """Verify get_applied_max_sequences aggregates highest applied sequence per entity."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ledger = EventApplyLedger(spark=spark_session, ledger_base_dir=tmpdir)
        ev1 = _make_sample_event("evt_001", sequence_number=10)
        ev2 = _make_sample_event("evt_002", sequence_number=20)
        ev3 = _make_sample_event("evt_003", entity_key='accounts:{"account_id":"ACC-0002"}', sequence_number=5)

        ledger.record_pending_events([ev1, ev2, ev3])
        # Mark ev1 and ev2 applied
        ledger.mark_events_applied(["evt_001", "evt_002"])

        max_seqs = ledger.get_applied_max_sequences()
        assert max_seqs['accounts:{"account_id":"ACC-0001"}'] == 20
        # ACC-0002 is still PENDING so not in applied max seqs
        assert 'accounts:{"account_id":"ACC-0002"}' not in max_seqs
