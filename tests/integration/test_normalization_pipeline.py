"""Integration tests for the complete CDCNormalizationPipeline."""

import json
import tempfile
import time
from pathlib import Path

from pyspark.sql import SparkSession

from src.cdc.generator import CDCScenarioGenerator
from src.normalization.models import QuarantineReasonCode
from src.normalization.pipeline import CDCNormalizationPipeline
from src.normalization.writer import (
    read_normalized_accepted_jsonl,
    read_quarantine_jsonl,
)
from src.source.generator import SnapshotConfig, SourceGenerator


def write_cdc_events_to_landing_dir(
    events: list[dict],
    landing_dir: Path,
    batch_id: str,
) -> list[Path]:
    """Helper to write raw CDC events into partitioned landing directory layout."""
    batch_dir = landing_dir / f"batch_id={batch_id}"
    batch_dir.mkdir(parents=True, exist_ok=True)

    # Group by table
    by_table: dict[str, list[dict]] = {}
    for ev in events:
        tbl = ev.get("table_name") or "unknown"
        by_table.setdefault(tbl, []).append(ev)

    created_files: list[Path] = []
    for tbl, recs in by_table.items():
        file_path = batch_dir / f"{tbl}.jsonl"
        with open(file_path, "w", encoding="utf-8") as f:
            for r in recs:
                f.write(json.dumps(r) + "\n")
        created_files.append(file_path)

    return created_files


def test_normalization_pipeline_batch_1_clean_ingestion(spark_session: SparkSession):
    """Verify Batch 1 clean inserts and updates are 100% accepted and landed."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        landing_dir = base_dir / "cdc_landing"
        norm_dir = base_dir / "normalized_cdc"
        quarantine_dir = base_dir / "quarantine"

        cdc_gen = CDCScenarioGenerator(SourceGenerator(SnapshotConfig(seed=42)))
        batch_1_events = [e.to_dict() for e in cdc_gen.generate_batch_1_inserts_and_updates("batch_001")]

        files = write_cdc_events_to_landing_dir(batch_1_events, landing_dir, "batch_001")

        pipeline = CDCNormalizationPipeline(
            spark=spark_session,
            normalized_base_dir=norm_dir,
            quarantine_base_dir=quarantine_dir,
        )

        accepted, quarantined, metrics = pipeline.run_pipeline(files)

        assert len(accepted) == 8  # 4 inserts + 4 updates
        assert len(quarantined) == 0
        assert metrics.raw_records_seen == 8
        assert metrics.accepted_records == 8
        assert metrics.quarantined_records == 0
        assert metrics.status == "SUCCESS"

        # Verify output on disk
        out_file = norm_dir / f"processing_id={metrics.processing_id}" / "accepted.jsonl"
        assert out_file.exists()
        landed = read_normalized_accepted_jsonl(out_file)
        assert len(landed) == 8


def test_normalization_pipeline_batch_2_advanced_scenarios(spark_session: SparkSession):
    """Verify Batch 2: deletes, exact duplicate drop, out-of-order resolution, and late arrivals."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        landing_dir = base_dir / "cdc_landing"
        norm_dir = base_dir / "normalized_cdc"
        quarantine_dir = base_dir / "quarantine"

        cdc_gen = CDCScenarioGenerator(SourceGenerator(SnapshotConfig(seed=42)))
        batch_1_events = [e.to_dict() for e in cdc_gen.generate_batch_1_inserts_and_updates("batch_001")]
        batch_2_events = [e.to_dict() for e in cdc_gen.generate_batch_2_advanced_cdc_scenarios("batch_002")]

        files_b1 = write_cdc_events_to_landing_dir(batch_1_events, landing_dir, "batch_001")
        files_b2 = write_cdc_events_to_landing_dir(batch_2_events, landing_dir, "batch_002")
        files = files_b1 + files_b2

        pipeline = CDCNormalizationPipeline(
            spark=spark_session,
            normalized_base_dir=norm_dir,
            quarantine_base_dir=quarantine_dir,
        )

        accepted, quarantined, metrics = pipeline.run_pipeline(files)

        # 13 raw events: 8 from Batch 1 + 5 from Batch 2
        assert metrics.raw_records_seen == 13
        assert metrics.accepted_records == 12
        assert metrics.exact_duplicates_dropped == 1  # evt_ins_acc_0041 duplicate dropped
        assert metrics.quarantined_records == 0
        assert metrics.status == "SUCCESS"

        # Verify out-of-order events for ACC-0002 are sorted strictly into sequence order: 101 then 102
        acc_events = [e for e in accepted if e.entity_sequence_key == 'accounts:{"account_id":"ACC-0002"}']
        assert len(acc_events) == 2
        assert acc_events[0].sequence_number == 101
        assert acc_events[1].sequence_number == 102

        # Verify late event is accepted and tagged
        late_events = [e for e in accepted if e.event_id == "evt_late_sub_0002"]
        assert len(late_events) == 1
        assert late_events[0].is_late_arrival is True


def test_normalization_pipeline_batch_3_quarantine_fixtures(spark_session: SparkSession):
    """Verify Batch 3 invalid fixtures + malformed JSON lines are routed to quarantine."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        landing_dir = base_dir / "cdc_landing"
        norm_dir = base_dir / "normalized_cdc"
        quarantine_dir = base_dir / "quarantine"

        cdc_gen = CDCScenarioGenerator(SourceGenerator(SnapshotConfig(seed=42)))
        batch_3_events = cdc_gen.generate_batch_3_quarantine_fixtures("batch_003_quarantine")

        files = write_cdc_events_to_landing_dir(batch_3_events, landing_dir, "batch_003_quarantine")

        # Append one raw corrupted line to accounts.jsonl
        acc_file = next(f for f in files if "accounts" in f.name)
        with open(acc_file, "a", encoding="utf-8") as f:
            f.write("CORRUPT_NON_JSON_LINE\n")

        pipeline = CDCNormalizationPipeline(
            spark=spark_session,
            normalized_base_dir=norm_dir,
            quarantine_base_dir=quarantine_dir,
        )

        accepted, quarantined, metrics = pipeline.run_pipeline(files)

        # 7 fixture events + 1 malformed line = 8
        assert metrics.raw_records_seen == 8
        assert metrics.accepted_records == 0
        assert metrics.quarantined_records == 8
        assert metrics.malformed_json_records == 1
        assert metrics.status == "SUCCESS_WITH_QUARANTINE"

        codes = {q.quarantine_code for q in quarantined}
        assert QuarantineReasonCode.MALFORMED_JSON in codes
        assert QuarantineReasonCode.MISSING_BUSINESS_KEY in codes
        assert QuarantineReasonCode.UNSUPPORTED_OPERATION in codes
        assert QuarantineReasonCode.MISSING_PAYLOAD in codes
        assert QuarantineReasonCode.INVALID_SEQUENCE in codes
        assert QuarantineReasonCode.MISSING_SEQUENCE in codes
        assert QuarantineReasonCode.UNKNOWN_TABLE in codes
        assert QuarantineReasonCode.MISSING_BEFORE_IMAGE in codes

        # Verify quarantine on disk
        q_file = quarantine_dir / f"processing_id={metrics.processing_id}" / "quarantine.jsonl"
        assert q_file.exists()
        landed_q = read_quarantine_jsonl(q_file)
        assert len(landed_q) == 8


def test_normalization_pipeline_combined_lifecycle_reconciliation(spark_session: SparkSession):
    """Verify full end-to-end reconciliation: raw_records_seen == accepted + exact_dups + quarantined."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        landing_dir = base_dir / "cdc_landing"
        norm_dir = base_dir / "normalized_cdc"
        quarantine_dir = base_dir / "quarantine"

        cdc_gen = CDCScenarioGenerator(SourceGenerator(SnapshotConfig(seed=42)))
        all_batches = cdc_gen.generate_all_batches()

        all_files: list[Path] = []
        for b_id, events in all_batches.items():
            dict_events = [e.to_dict() if hasattr(e, "to_dict") else e for e in events]
            all_files.extend(write_cdc_events_to_landing_dir(dict_events, landing_dir, b_id))

        pipeline = CDCNormalizationPipeline(
            spark=spark_session,
            normalized_base_dir=norm_dir,
            quarantine_base_dir=quarantine_dir,
        )

        accepted, quarantined, metrics = pipeline.run_pipeline(all_files)

        # Total raw: Batch 1 (8) + Batch 2 (5) + Batch 3 (7) = 20
        assert metrics.raw_records_seen == 20
        assert metrics.accepted_records == 12  # Batch 1 (8) + Batch 2 accepted (4)
        assert metrics.exact_duplicates_dropped == 1  # Batch 2 dup
        assert metrics.quarantined_records == 7  # Batch 3 fixtures

        # Strict invariant reconciliation
        assert metrics.raw_records_seen == (
            metrics.accepted_records + metrics.exact_duplicates_dropped + metrics.quarantined_records
        )


def test_normalization_pipeline_replay_determinism(spark_session: SparkSession):
    """Verify executing pipeline twice against same input produces identical processing_id and fingerprints."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        landing_dir = base_dir / "cdc_landing"
        norm_dir = base_dir / "normalized_cdc"
        quarantine_dir = base_dir / "quarantine"

        cdc_gen = CDCScenarioGenerator(SourceGenerator(SnapshotConfig(seed=42)))
        batch_1_events = [e.to_dict() for e in cdc_gen.generate_batch_1_inserts_and_updates("batch_001")]
        files = write_cdc_events_to_landing_dir(batch_1_events, landing_dir, "batch_001")

        pipeline = CDCNormalizationPipeline(
            spark=spark_session,
            normalized_base_dir=norm_dir,
            quarantine_base_dir=quarantine_dir,
        )

        # Run 1
        acc1, q1, m1 = pipeline.run_pipeline(files)

        # Run 2
        acc2, q2, m2 = pipeline.run_pipeline(files)

        # Assert identical deterministic identity
        assert m1.processing_id == m2.processing_id
        assert [e.event_fingerprint for e in acc1] == [e.event_fingerprint for e in acc2]
        assert [e.event_id for e in acc1] == [e.event_id for e in acc2]


def test_normalization_pipeline_deterministic_output_ordering(spark_session: SparkSession):
    """Verify output records are deterministically ordered by table_name, canonical key, sequence_number, event_id."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        landing_dir = base_dir / "cdc_landing"
        norm_dir = base_dir / "normalized_cdc"
        quarantine_dir = base_dir / "quarantine"

        cdc_gen = CDCScenarioGenerator(SourceGenerator(SnapshotConfig(seed=42)))
        batch_1_events = [e.to_dict() for e in cdc_gen.generate_batch_1_inserts_and_updates("batch_001")]
        files = write_cdc_events_to_landing_dir(batch_1_events, landing_dir, "batch_001")

        pipeline = CDCNormalizationPipeline(
            spark=spark_session,
            normalized_base_dir=norm_dir,
            quarantine_base_dir=quarantine_dir,
        )

        accepted, _quarantined, metrics = pipeline.run_pipeline(files)

        # Table names must appear in alphabetical order (accounts, invoices, payments, subscriptions)
        table_names = [e.table_name for e in accepted]
        assert table_names == sorted(table_names)


def test_normalization_pipeline_reversed_input_replay(spark_session: SparkSession):
    """Verify passing files in forward order vs reverse order yields identical accepted/quarantine logical output."""
    with tempfile.TemporaryDirectory() as tmpdir1, tempfile.TemporaryDirectory() as tmpdir2:
        # Env 1: Forward order
        base1 = Path(tmpdir1)
        cdc_gen = CDCScenarioGenerator(SourceGenerator(SnapshotConfig(seed=42)))
        b1_events = [e.to_dict() for e in cdc_gen.generate_batch_1_inserts_and_updates("batch_001")]
        b2_events = [e.to_dict() for e in cdc_gen.generate_batch_2_advanced_cdc_scenarios("batch_002")]

        files1_b1 = write_cdc_events_to_landing_dir(b1_events, base1 / "cdc_landing", "batch_001")
        files1_b2 = write_cdc_events_to_landing_dir(b2_events, base1 / "cdc_landing", "batch_002")
        forward_files = files1_b1 + files1_b2

        p1 = CDCNormalizationPipeline(
            spark=spark_session,
            normalized_base_dir=base1 / "norm",
            quarantine_base_dir=base1 / "quar",
        )
        acc1, q1, m1 = p1.run_pipeline(forward_files)

        # Env 2: Reversed order
        base2 = Path(tmpdir2)
        files2_b1 = write_cdc_events_to_landing_dir(b1_events, base2 / "cdc_landing", "batch_001")
        files2_b2 = write_cdc_events_to_landing_dir(b2_events, base2 / "cdc_landing", "batch_002")
        reversed_files = list(reversed(files2_b2 + files2_b1))

        p2 = CDCNormalizationPipeline(
            spark=spark_session,
            normalized_base_dir=base2 / "norm",
            quarantine_base_dir=base2 / "quar",
        )
        acc2, q2, m2 = p2.run_pipeline(reversed_files)

        # Must have identical processing_id
        assert m1.processing_id == m2.processing_id

        # Must have identical accepted events
        assert [e.to_dict() for e in acc1] == [e.to_dict() for e in acc2]


def test_normalization_pipeline_reversed_duplicate_conflict_quarantine_order(spark_session: SparkSession):
    """Verify conflicting duplicate events produce identical quarantine ordering regardless of file/input order."""
    with tempfile.TemporaryDirectory() as tmpdir1, tempfile.TemporaryDirectory() as tmpdir2:
        # Create 2 conflicting events with same event_id but different payloads
        ev_conf_a = {
            "event_id": "evt_conf_same_id",
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
        }
        ev_conf_b = dict(ev_conf_a)
        ev_conf_b["payload"] = {"account_id": "ACC-0001", "status": "TRIAL"}
        ev_conf_b["batch_id"] = "batch_002"

        # Env 1: A then B
        base1 = Path(tmpdir1)
        f1_a = base1 / "cdc_landing" / "batch_id=batch_001" / "accounts.jsonl"
        f1_a.parent.mkdir(parents=True)
        f1_a.write_text(json.dumps(ev_conf_a) + "\n", encoding="utf-8")
        f1_b = base1 / "cdc_landing" / "batch_id=batch_002" / "accounts.jsonl"
        f1_b.parent.mkdir(parents=True)
        f1_b.write_text(json.dumps(ev_conf_b) + "\n", encoding="utf-8")

        p1 = CDCNormalizationPipeline(spark=spark_session, normalized_base_dir=base1 / "norm", quarantine_base_dir=base1 / "quar")
        _, q1, _ = p1.run_pipeline([f1_a, f1_b])

        # Env 2: B then A (reversed input order)
        base2 = Path(tmpdir2)
        f2_a = base2 / "cdc_landing" / "batch_id=batch_001" / "accounts.jsonl"
        f2_a.parent.mkdir(parents=True)
        f2_a.write_text(json.dumps(ev_conf_a) + "\n", encoding="utf-8")
        f2_b = base2 / "cdc_landing" / "batch_id=batch_002" / "accounts.jsonl"
        f2_b.parent.mkdir(parents=True)
        f2_b.write_text(json.dumps(ev_conf_b) + "\n", encoding="utf-8")

        p2 = CDCNormalizationPipeline(spark=spark_session, normalized_base_dir=base2 / "norm", quarantine_base_dir=base2 / "quar")
        _, q2, _ = p2.run_pipeline([f2_b, f2_a])

        assert len(q1) == 2
        assert len(q2) == 2
        assert [q.to_dict() for q in q1] == [q.to_dict() for q in q2]


def test_normalization_pipeline_byte_level_replay_determinism(spark_session: SparkSession):
    """Verify executing the pipeline twice against the same input produces byte-for-byte identical output files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        landing_dir = base_dir / "cdc_landing"
        norm_dir = base_dir / "normalized_cdc"
        quarantine_dir = base_dir / "quarantine"

        cdc_gen = CDCScenarioGenerator(SourceGenerator(SnapshotConfig(seed=42)))
        all_batches = cdc_gen.generate_all_batches()

        all_files: list[Path] = []
        for b_id, events in all_batches.items():
            dict_events = [e.to_dict() if hasattr(e, "to_dict") else e for e in events]
            all_files.extend(write_cdc_events_to_landing_dir(dict_events, landing_dir, b_id))

        pipeline = CDCNormalizationPipeline(
            spark=spark_session,
            normalized_base_dir=norm_dir,
            quarantine_base_dir=quarantine_dir,
        )

        # Run 1
        _, _, m1 = pipeline.run_pipeline(all_files)
        acc_file_1 = norm_dir / f"processing_id={m1.processing_id}" / "accepted.jsonl"
        quar_file_1 = quarantine_dir / f"processing_id={m1.processing_id}" / "quarantine.jsonl"
        acc_bytes_1 = acc_file_1.read_bytes()
        quar_bytes_1 = quar_file_1.read_bytes()

        # Small sleep to guarantee wall-clock advancement
        time.sleep(0.05)

        # Run 2
        _, _, m2 = pipeline.run_pipeline(all_files)
        acc_file_2 = norm_dir / f"processing_id={m2.processing_id}" / "accepted.jsonl"
        quar_file_2 = quarantine_dir / f"processing_id={m2.processing_id}" / "quarantine.jsonl"
        acc_bytes_2 = acc_file_2.read_bytes()
        quar_bytes_2 = quar_file_2.read_bytes()

        assert m1.processing_id == m2.processing_id
        # Byte-for-byte identical contents
        assert acc_bytes_1 == acc_bytes_2
        assert quar_bytes_1 == quar_bytes_2


def test_normalization_pipeline_reversed_validation_quarantine(spark_session: SparkSession):
    """Verify invalid validation events in different files produce identical quarantine outputs regardless of file input ordering."""
    with tempfile.TemporaryDirectory() as tmpdir1, tempfile.TemporaryDirectory() as tmpdir2:
        # Invalid accounts event (missing payload for INSERT)
        invalid_acc_event = {
            "event_id": "evt_inv_acc_001",
            "table_name": "accounts",
            "operation": "INSERT",
            "business_key": {"account_id": "ACC-9999"},
            "sequence_number": 1,
            "event_timestamp": "2026-05-11T01:00:00Z",
            "source_commit_timestamp": "2026-05-11T01:00:01Z",
            "batch_id": "batch_003_quarantine",
            "payload": None,
            "before_payload": None,
            "source_system": "b2b_saas_postgres",
        }
        # Invalid payments event (unsupported operation)
        invalid_pay_event = {
            "event_id": "evt_inv_pay_001",
            "table_name": "payments",
            "operation": "PURGE",
            "business_key": {"payment_id": "PAY-9999"},
            "sequence_number": 1,
            "event_timestamp": "2026-05-11T01:00:00Z",
            "source_commit_timestamp": "2026-05-11T01:00:01Z",
            "batch_id": "batch_003_quarantine",
            "payload": {"payment_id": "PAY-9999"},
            "before_payload": None,
            "source_system": "b2b_saas_postgres",
        }

        # Env 1: accounts then payments
        base1 = Path(tmpdir1)
        acc_file_1 = base1 / "cdc_landing" / "batch_id=batch_003" / "accounts.jsonl"
        acc_file_1.parent.mkdir(parents=True, exist_ok=True)
        acc_file_1.write_text(json.dumps(invalid_acc_event) + "\n", encoding="utf-8")

        pay_file_1 = base1 / "cdc_landing" / "batch_id=batch_003" / "payments.jsonl"
        pay_file_1.parent.mkdir(parents=True, exist_ok=True)
        pay_file_1.write_text(json.dumps(invalid_pay_event) + "\n", encoding="utf-8")

        p1 = CDCNormalizationPipeline(
            spark=spark_session,
            normalized_base_dir=base1 / "norm",
            quarantine_base_dir=base1 / "quar",
        )
        _, q1, m1 = p1.run_pipeline([acc_file_1, pay_file_1])
        quar_file_1 = base1 / "quar" / f"processing_id={m1.processing_id}" / "quarantine.jsonl"
        quar_bytes_1 = quar_file_1.read_bytes()

        # Env 2: payments then accounts (reversed input order in independent temp directory)
        base2 = Path(tmpdir2)
        acc_file_2 = base2 / "cdc_landing" / "batch_id=batch_003" / "accounts.jsonl"
        acc_file_2.parent.mkdir(parents=True, exist_ok=True)
        acc_file_2.write_text(json.dumps(invalid_acc_event) + "\n", encoding="utf-8")

        pay_file_2 = base2 / "cdc_landing" / "batch_id=batch_003" / "payments.jsonl"
        pay_file_2.parent.mkdir(parents=True, exist_ok=True)
        pay_file_2.write_text(json.dumps(invalid_pay_event) + "\n", encoding="utf-8")

        p2 = CDCNormalizationPipeline(
            spark=spark_session,
            normalized_base_dir=base2 / "norm",
            quarantine_base_dir=base2 / "quar",
        )
        _, q2, m2 = p2.run_pipeline([pay_file_2, acc_file_2])
        quar_file_2 = base2 / "quar" / f"processing_id={m2.processing_id}" / "quarantine.jsonl"
        quar_bytes_2 = quar_file_2.read_bytes()

        # 1. Processing ID must be identical across runs
        assert m1.processing_id == m2.processing_id

        # 2. Record count matches
        assert len(q1) == 2
        assert len(q2) == 2

        # 3. Serialized dictionary representation matches identically
        assert [q.to_dict() for q in q1] == [q.to_dict() for q in q2]

        # 4. Byte-for-byte identical persisted quarantine files
        assert quar_bytes_1 == quar_bytes_2

        # 5. Verify raw_record does NOT contain ingestion_order or transient metadata
        for q in q1 + q2:
            assert isinstance(q.raw_record, dict)
            assert "ingestion_order" not in q.raw_record
            assert "ingestion_batch_id" not in q.raw_record
            assert "source_file" not in q.raw_record

        # 6. Verify source_file is logical and portable (no absolute /tmp/ or /var/ paths)
        source_files = {q.source_file for q in q1}
        assert source_files == {
            "batch_id=batch_003/accounts.jsonl",
            "batch_id=batch_003/payments.jsonl",
        }
