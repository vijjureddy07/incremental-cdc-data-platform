"""Unit tests for normalized CDC event adapter and typed DataFrame conversion."""

import json
import tempfile
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession

from src.merge.event_adapter import convert_events_to_spark_df, load_accepted_events_from_file
from src.normalization.models import NormalizedCDCEvent


def test_adapter_load_from_jsonl():
    """Verify loading accepted.jsonl reconstructs NormalizedCDCEvent instances."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dir_path = Path(tmpdir) / "processing_id=proc_sample_123"
        dir_path.mkdir(parents=True)
        file_path = dir_path / "accepted.jsonl"

        event_data = {
            "event_id": "evt_acc_01",
            "table_name": "accounts",
            "operation": "INSERT",
            "business_key": {"account_id": "ACC-0041"},
            "sequence_number": 1,
            "event_timestamp": "2026-05-11T01:00:00Z",
            "source_commit_timestamp": "2026-05-11T01:00:01Z",
            "batch_id": "batch_001",
            "payload": {
                "account_id": "ACC-0041",
                "account_name": "Acme Inc",
                "industry": "Software",
                "country": "US",
                "status": "ACTIVE",
                "created_at": "2026-05-11T01:00:00Z",
                "updated_at": "2026-05-11T01:00:00Z",
            },
            "before_payload": None,
            "source_system": "b2b_saas_postgres",
            "entity_sequence_key": 'accounts:{"account_id":"ACC-0041"}',
            "business_key_canonical": '{"account_id":"ACC-0041"}',
            "event_fingerprint": "fp_acc_01",
            "is_late_arrival": False,
            "source_file": "batch_id=batch_001/accounts.jsonl",
            "ingestion_batch_id": "batch_001",
        }

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(event_data) + "\n")

        events = load_accepted_events_from_file(file_path)
        assert len(events) == 1
        assert events[0].event_id == "evt_acc_01"
        assert events[0].table_name == "accounts"


def test_adapter_convert_to_spark_df_with_types(spark_session: SparkSession):
    """Verify conversion of subscriptions payload into strongly typed Spark DataFrame."""
    ev = NormalizedCDCEvent(
        event_id="evt_sub_01",
        table_name="subscriptions",
        operation="INSERT",
        business_key={"subscription_id": "SUB-0061"},
        sequence_number=1,
        event_timestamp="2026-05-11T01:00:00Z",
        source_commit_timestamp="2026-05-11T01:00:01Z",
        batch_id="batch_001",
        payload={
            "subscription_id": "SUB-0061",
            "account_id": "ACC-0001",
            "plan_name": "ENTERPRISE",
            "billing_cycle": "ANNUAL",
            "monthly_amount": "4999.00",
            "status": "ACTIVE",
            "start_date": "2026-05-11",
            "end_date": None,
            "created_at": "2026-05-11T01:00:00Z",
            "updated_at": "2026-05-11T01:00:00Z",
        },
        before_payload=None,
        source_system="b2b_saas_postgres",
        entity_sequence_key='subscriptions:{"subscription_id":"SUB-0061"}',
        business_key_canonical='{"subscription_id":"SUB-0061"}',
        event_fingerprint="fp_sub_01",
        is_late_arrival=False,
        source_file="batch_id=batch_001/subscriptions.jsonl",
        ingestion_batch_id="batch_001",
    )

    df = convert_events_to_spark_df("subscriptions", [ev], spark_session, processing_id="proc_123")
    assert df.count() == 1
    row = df.first()

    assert isinstance(row["monthly_amount"], Decimal)
    assert row["monthly_amount"] == Decimal("4999.00")
    assert row["_last_sequence_number"] == 1
    assert row["_last_event_id"] == "evt_sub_01"
    assert row["_last_processing_id"] == "proc_123"
    assert row["_is_deleted"] is False
    assert row["_deleted_at"] is None
