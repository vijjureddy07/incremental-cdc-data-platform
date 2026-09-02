"""Integration tests for the complete CDCNormalizationPipeline."""

import json
import tempfile
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
    """Verify that executing the pipeline twice against the exact same input produces identical deterministic outputs."""
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
