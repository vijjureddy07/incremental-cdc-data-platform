"""Unit tests for PySpark normalization, deduplication, conflict resolution, and sequence ordering."""

from pyspark.sql import SparkSession

from src.normalization.models import QuarantineReasonCode
from src.normalization.processor import SparkCDCNormalizationProcessor


def test_processor_exact_duplicate_deduplication(spark_session: SparkSession):
    """Verify that exact duplicate deliveries (same event_id and same fingerprint) are deduplicated cleanly."""
    processor = SparkCDCNormalizationProcessor(spark_session)

    event1 = {
        "event_id": "evt_ins_acc_0041",
        "table_name": "accounts",
        "operation": "INSERT",
        "business_key": {"account_id": "ACC-0041"},
        "sequence_number": 1,
        "event_timestamp": "2026-05-11T01:05:00Z",
        "source_commit_timestamp": "2026-05-11T01:05:01Z",
        "batch_id": "batch_001",
        "payload": {"account_id": "ACC-0041", "status": "ACTIVE"},
        "before_payload": None,
        "source_system": "b2b_saas_postgres",
        "source_file": "batch_001/accounts.jsonl",
        "ingestion_batch_id": "batch_001",
        "ingestion_order": 1,
    }

    # Redelivered copy with identical semantic content
    event2 = dict(event1)
    event2["ingestion_order"] = 2
    event2["ingestion_batch_id"] = "batch_002"

    accepted, quarantined, exact_dups_dropped, dup_conflicts, seq_conflicts = processor.process([event1, event2])

    assert len(accepted) == 1
    assert len(quarantined) == 0
    assert exact_dups_dropped == 1
    assert dup_conflicts == 0
    assert seq_conflicts == 0
    assert accepted[0].event_id == "evt_ins_acc_0041"


def test_processor_conflicting_duplicate_event_id_quarantine(spark_session: SparkSession):
    """Verify that duplicate event_ids with differing payloads are quarantined under DUPLICATE_EVENT_CONFLICT."""
    processor = SparkCDCNormalizationProcessor(spark_session)

    event1 = {
        "event_id": "evt_conflict_001",
        "table_name": "accounts",
        "operation": "UPDATE",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 10,
        "event_timestamp": "2026-05-11T01:30:00Z",
        "source_commit_timestamp": "2026-05-11T01:30:02Z",
        "batch_id": "batch_001",
        "payload": {"account_id": "ACC-0001", "status": "ACTIVE"},
        "before_payload": {"account_id": "ACC-0001", "status": "SUSPENDED"},
        "source_system": "b2b_saas_postgres",
        "source_file": "batch_001/accounts.jsonl",
        "ingestion_batch_id": "batch_001",
        "ingestion_order": 1,
    }

    # Same event_id but DIFFERENT payload (e.g. status TRIAL instead of ACTIVE)
    event2 = dict(event1)
    event2["payload"] = {"account_id": "ACC-0001", "status": "TRIAL"}
    event2["ingestion_order"] = 2

    accepted, quarantined, exact_dups_dropped, dup_conflicts, seq_conflicts = processor.process([event1, event2])

    assert len(accepted) == 0
    assert len(quarantined) == 2
    assert exact_dups_dropped == 0
    assert dup_conflicts == 2
    assert all(q.quarantine_code == QuarantineReasonCode.DUPLICATE_EVENT_CONFLICT for q in quarantined)


def test_processor_out_of_order_sequence_normalization(spark_session: SparkSession):
    """Verify that arrival out-of-order events (seq 102 arriving before 101) are normalized into sequence order."""
    processor = SparkCDCNormalizationProcessor(spark_session)

    # Sequence 102 arriving first
    ev_102 = {
        "event_id": "evt_ooo_seq102",
        "table_name": "accounts",
        "operation": "UPDATE",
        "business_key": {"account_id": "ACC-0002"},
        "sequence_number": 102,
        "event_timestamp": "2026-05-13T01:20:00Z",
        "source_commit_timestamp": "2026-05-13T01:20:02Z",
        "batch_id": "batch_002",
        "payload": {"account_id": "ACC-0002", "status": "TRIAL"},
        "before_payload": {"account_id": "ACC-0002", "status": "ACTIVE"},
        "source_system": "b2b_saas_postgres",
        "source_file": "batch_002/accounts.jsonl",
        "ingestion_batch_id": "batch_002",
        "ingestion_order": 1,
    }

    # Sequence 101 arriving second
    ev_101 = {
        "event_id": "evt_ooo_seq101",
        "table_name": "accounts",
        "operation": "UPDATE",
        "business_key": {"account_id": "ACC-0002"},
        "sequence_number": 101,
        "event_timestamp": "2026-05-13T01:15:00Z",
        "source_commit_timestamp": "2026-05-13T01:15:01Z",
        "batch_id": "batch_002",
        "payload": {"account_id": "ACC-0002", "country": "GB"},
        "before_payload": {"account_id": "ACC-0002", "country": "US"},
        "source_system": "b2b_saas_postgres",
        "source_file": "batch_002/accounts.jsonl",
        "ingestion_batch_id": "batch_002",
        "ingestion_order": 2,
    }

    # Ingestion order is 102 then 101
    accepted, quarantined, exact_dups_dropped, dup_conflicts, seq_conflicts = processor.process([ev_102, ev_101])

    assert len(accepted) == 2
    assert len(quarantined) == 0

    # Normalized sequence must be 101 then 102
    assert accepted[0].sequence_number == 101
    assert accepted[0].event_id == "evt_ooo_seq101"
    assert accepted[1].sequence_number == 102
    assert accepted[1].event_id == "evt_ooo_seq102"


def test_processor_equal_sequence_conflict_quarantine(spark_session: SparkSession):
    """Verify that multiple distinct events for the same entity with equal sequence numbers are quarantined."""
    processor = SparkCDCNormalizationProcessor(spark_session)

    # Event A at sequence 10
    ev_a = {
        "event_id": "evt_acc_seq10_a",
        "table_name": "accounts",
        "operation": "UPDATE",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 10,
        "event_timestamp": "2026-05-11T01:30:00Z",
        "source_commit_timestamp": "2026-05-11T01:30:02Z",
        "batch_id": "batch_001",
        "payload": {"account_id": "ACC-0001", "status": "ACTIVE"},
        "before_payload": {"account_id": "ACC-0001", "status": "SUSPENDED"},
        "source_system": "b2b_saas_postgres",
        "source_file": "batch_001/accounts.jsonl",
        "ingestion_batch_id": "batch_001",
        "ingestion_order": 1,
    }

    # Event B also at sequence 10 for the same ACC-0001 entity with different event_id & payload
    ev_b = {
        "event_id": "evt_acc_seq10_b",
        "table_name": "accounts",
        "operation": "UPDATE",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 10,
        "event_timestamp": "2026-05-11T01:35:00Z",
        "source_commit_timestamp": "2026-05-11T01:35:01Z",
        "batch_id": "batch_001",
        "payload": {"account_id": "ACC-0001", "industry": "Fintech"},
        "before_payload": {"account_id": "ACC-0001", "industry": "Healthcare"},
        "source_system": "b2b_saas_postgres",
        "source_file": "batch_001/accounts.jsonl",
        "ingestion_batch_id": "batch_001",
        "ingestion_order": 2,
    }

    accepted, quarantined, exact_dups_dropped, dup_conflicts, seq_conflicts = processor.process([ev_a, ev_b])

    assert len(accepted) == 0
    assert len(quarantined) == 2
    assert seq_conflicts == 2
    assert all(q.quarantine_code == QuarantineReasonCode.SEQUENCE_CONFLICT for q in quarantined)


def test_processor_cross_entity_same_sequence_allowed(spark_session: SparkSession):
    """Verify that different entities (or different tables) sharing the same sequence number do NOT conflict."""
    processor = SparkCDCNormalizationProcessor(spark_session)

    # ACC-0001 at sequence 10
    ev_acc1 = {
        "event_id": "evt_acc1_seq10",
        "table_name": "accounts",
        "operation": "UPDATE",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 10,
        "event_timestamp": "2026-05-11T01:30:00Z",
        "source_commit_timestamp": "2026-05-11T01:30:02Z",
        "batch_id": "batch_001",
        "payload": {"account_id": "ACC-0001", "status": "ACTIVE"},
        "before_payload": {"account_id": "ACC-0001", "status": "SUSPENDED"},
        "source_system": "b2b_saas_postgres",
        "source_file": "batch_001/accounts.jsonl",
        "ingestion_batch_id": "batch_001",
        "ingestion_order": 1,
    }

    # ACC-0002 also at sequence 10 (different entity)
    ev_acc2 = {
        "event_id": "evt_acc2_seq10",
        "table_name": "accounts",
        "operation": "UPDATE",
        "business_key": {"account_id": "ACC-0002"},
        "sequence_number": 10,
        "event_timestamp": "2026-05-11T01:35:00Z",
        "source_commit_timestamp": "2026-05-11T01:35:01Z",
        "batch_id": "batch_001",
        "payload": {"account_id": "ACC-0002", "status": "ACTIVE"},
        "before_payload": {"account_id": "ACC-0002", "status": "SUSPENDED"},
        "source_system": "b2b_saas_postgres",
        "source_file": "batch_001/accounts.jsonl",
        "ingestion_batch_id": "batch_001",
        "ingestion_order": 2,
    }

    accepted, quarantined, exact_dups_dropped, dup_conflicts, seq_conflicts = processor.process([ev_acc1, ev_acc2])

    assert len(accepted) == 2
    assert len(quarantined) == 0
    assert exact_dups_dropped == 0
    assert dup_conflicts == 0
    assert seq_conflicts == 0


def test_processor_late_arriving_event_preserved(spark_session: SparkSession):
    """Verify that late-arriving events are accepted if valid and tagged appropriately."""
    processor = SparkCDCNormalizationProcessor(spark_session)

    late_event = {
        "event_id": "evt_late_sub_0002",
        "table_name": "subscriptions",
        "operation": "UPDATE",
        "business_key": {"subscription_id": "SUB-0002"},
        "sequence_number": 5,
        "event_timestamp": "2026-01-01T00:00:00Z",  # Older timestamp
        "source_commit_timestamp": "2026-01-01T00:00:05Z",
        "batch_id": "batch_002",
        "payload": {"subscription_id": "SUB-0002", "billing_cycle": "ANNUAL"},
        "before_payload": {"subscription_id": "SUB-0002", "billing_cycle": "MONTHLY"},
        "source_system": "b2b_saas_postgres",
        "source_file": "batch_002/subscriptions.jsonl",
        "ingestion_batch_id": "batch_002",
        "ingestion_order": 1,
    }

    accepted, quarantined, exact_dups_dropped, dup_conflicts, seq_conflicts = processor.process([late_event])

    assert len(accepted) == 1
    assert len(quarantined) == 0
    assert accepted[0].is_late_arrival is True
