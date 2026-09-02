"""Orchestrator pipeline for end-to-end PySpark CDC normalization, deduplication, and quarantine routing."""

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from src.normalization.fingerprint import generate_processing_id
from src.normalization.models import (
    NormalizationAuditMetrics,
    NormalizedCDCEvent,
    QuarantinedEvent,
)
from src.normalization.processor import SparkCDCNormalizationProcessor
from src.normalization.reader import read_raw_cdc_files
from src.normalization.validator import validate_raw_cdc_record
from src.normalization.writer import (
    write_normalized_accepted_jsonl,
    write_quarantine_jsonl,
)
from src.utils.helpers import format_iso_timestamp


class CDCNormalizationPipeline:
    """Orchestrates ingestion, structural validation, PySpark normalization, and deterministic partitioning."""

    def __init__(
        self,
        spark: SparkSession,
        normalized_base_dir: str | Path = "data/normalized_cdc",
        quarantine_base_dir: str | Path = "data/quarantine",
    ) -> None:
        self.spark = spark
        self.processor = SparkCDCNormalizationProcessor(spark)
        self.normalized_base_dir = Path(normalized_base_dir)
        self.quarantine_base_dir = Path(quarantine_base_dir)

    def run_pipeline(
        self,
        file_paths: list[str | Path],
    ) -> tuple[list[NormalizedCDCEvent], list[QuarantinedEvent], NormalizationAuditMetrics]:
        """Execute full CDC normalization across raw input landing files."""
        started_at = format_iso_timestamp(datetime.now(UTC))
        run_id = f"run_norm_{uuid.uuid4().hex[:12]}"

        # Resolve deterministic processing_id from stable input file names/paths
        clean_file_paths = [str(Path(p).resolve()) for p in file_paths if Path(p).exists()]
        processing_id = generate_processing_id(clean_file_paths)

        # Step 1: Read raw files and isolate malformed JSON lines
        parsed_records, malformed_quarantine = read_raw_cdc_files(clean_file_paths)

        # Step 2: Validate parsed records against structural and semantic rules
        valid_records: list[dict[str, Any]] = []
        validation_quarantine: list[QuarantinedEvent] = []

        now_str = format_iso_timestamp(datetime.now(UTC))
        for r in parsed_records:
            is_valid, q_code, q_reason = validate_raw_cdc_record(r)
            if is_valid:
                valid_records.append(r)
            else:
                validation_quarantine.append(
                    QuarantinedEvent(
                        quarantine_code=q_code or "VALIDATION_FAILED",
                        quarantine_reason=q_reason or "Unknown validation error.",
                        raw_record=r,
                        event_id=r.get("event_id"),
                        table_name=r.get("table_name"),
                        source_file=r.get("source_file"),
                        batch_id=r.get("batch_id"),
                        quarantined_at=now_str,
                    )
                )

        # Step 3: Run PySpark transformation engine (dedupe, conflict detection, entity sequence ordering)
        accepted_events, spark_quarantine, exact_dups_dropped, dup_conflicts, seq_conflicts = (
            self.processor.process(valid_records)
        )

        # Step 4: Collate all quarantined records
        all_quarantine: list[QuarantinedEvent] = (
            malformed_quarantine + validation_quarantine + spark_quarantine
        )

        # Sort quarantined events deterministically
        all_quarantine.sort(
            key=lambda q: (
                str(q.table_name or ""),
                str(q.event_id or ""),
                str(q.quarantine_code),
            )
        )

        # Step 5: Write outputs to deterministic partition directories
        if accepted_events:
            write_normalized_accepted_jsonl(
                processing_id=processing_id,
                events=accepted_events,
                base_dir=self.normalized_base_dir,
            )

        if all_quarantine:
            write_quarantine_jsonl(
                processing_id=processing_id,
                events=all_quarantine,
                base_dir=self.quarantine_base_dir,
            )

        # Step 6: Compile audit metrics
        tables_seen = sorted(
            {
                str(r.get("table_name"))
                for r in parsed_records
                if r.get("table_name")
            }
        )

        min_seq_by_table: dict[str, int] = {}
        max_seq_by_table: dict[str, int] = {}
        for ev in accepted_events:
            tbl = ev.table_name
            if tbl not in min_seq_by_table or ev.sequence_number < min_seq_by_table[tbl]:
                min_seq_by_table[tbl] = ev.sequence_number
            if tbl not in max_seq_by_table or ev.sequence_number > max_seq_by_table[tbl]:
                max_seq_by_table[tbl] = ev.sequence_number

        raw_seen = len(parsed_records) + len(malformed_quarantine)
        status = "SUCCESS_WITH_QUARANTINE" if all_quarantine else "SUCCESS"
        completed_at = format_iso_timestamp(datetime.now(UTC))

        metrics = NormalizationAuditMetrics(
            run_id=run_id,
            processing_id=processing_id,
            files_read=clean_file_paths,
            raw_records_seen=raw_seen,
            parsed_records=len(parsed_records),
            accepted_records=len(accepted_events),
            exact_duplicates_dropped=exact_dups_dropped,
            quarantined_records=len(all_quarantine),
            malformed_json_records=len(malformed_quarantine),
            duplicate_event_conflicts=dup_conflicts,
            sequence_conflicts=seq_conflicts,
            tables_seen=tables_seen,
            min_sequence_by_table=min_seq_by_table,
            max_sequence_by_table=max_seq_by_table,
            started_at=started_at,
            completed_at=completed_at,
            status=status,
        )

        return accepted_events, all_quarantine, metrics
