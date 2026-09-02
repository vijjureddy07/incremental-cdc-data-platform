"""Transactional pipeline orchestrator for watermark incremental ingestion."""

import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from src.utils.helpers import format_iso_timestamp
from src.watermark.control_store import SQLiteWatermarkControlStore
from src.watermark.landing import (
    generate_deterministic_batch_id,
    write_watermark_batch_jsonl,
)
from src.watermark.models import (
    ExtractionResult,
    WatermarkError,
    WatermarkRunAudit,
    WatermarkRunStatus,
)
from src.watermark.source_adapter import InMemorySourceAdapter


class WatermarkPipeline:
    """Orchestrates transactional watermark extraction cycles across source tables."""

    def __init__(
        self,
        control_store: SQLiteWatermarkControlStore,
        source_adapter: InMemorySourceAdapter,
        landing_base_dir: str | Path = "data/watermark_landing",
    ) -> None:
        self.control_store = control_store
        self.source_adapter = source_adapter
        self.landing_base_dir = Path(landing_base_dir)

    def run_table_extraction(
        self,
        table_name: str,
        watermark_column: str = "updated_at",
        post_capture_hook: Callable[[], None] | None = None,
        fail_before_commit: bool = False,
    ) -> ExtractionResult:
        """Execute a single transactional watermark extraction run for a table.

        Strict Order of Operations:
        1. Read current LOW watermark state & expected version from control store
        2. Capture source HIGH watermark
        3. [Optional hook for simulating source mutations after HIGH is captured]
        4. If HIGH <= LOW: return NO_DATA without advancing watermark
        5. Begin run audit in RUNNING status
        6. Extract bounded rows: LOW < cursor <= HIGH
        7. Write and verify landing output
        8. [Optional failure injection point]
        9. Commit watermark checkpoint (optimistic concurrency version check)
        10. Mark run audit SUCCESS
        """
        pk_col = self.source_adapter.get_primary_key_column(table_name)
        now_str = format_iso_timestamp(datetime.now(UTC))
        run_id = f"run_{table_name}_{uuid.uuid4().hex[:12]}"

        # Step 1: Read current persisted LOW watermark state
        state = self.control_store.get_or_create_watermark_state(
            table_name=table_name,
            watermark_column=watermark_column,
            tie_breaker_column=pk_col,
        )
        low_watermark = state.last_watermark
        expected_version = state.version

        # Step 2: Capture source HIGH watermark
        high_watermark = self.source_adapter.capture_source_high_watermark(
            table_name=table_name,
            watermark_column=watermark_column,
        )

        # Step 3: Optional hook (for bounded-high testing)
        if post_capture_hook is not None:
            post_capture_hook()

        # Step 4: Evaluate NO_DATA condition
        if high_watermark.is_initial or high_watermark <= low_watermark:
            batch_id = generate_deterministic_batch_id(table_name, low_watermark, high_watermark)
            audit = WatermarkRunAudit(
                run_id=run_id,
                table_name=table_name,
                batch_id=batch_id,
                low_watermark=low_watermark,
                high_watermark=high_watermark,
                status=WatermarkRunStatus.NO_DATA,
                rows_extracted=0,
                landing_path=None,
                started_at=now_str,
                completed_at=format_iso_timestamp(datetime.now(UTC)),
            )
            self.control_store.start_run_audit(audit)
            return ExtractionResult(
                table_name=table_name,
                run_id=run_id,
                batch_id=batch_id,
                status=WatermarkRunStatus.NO_DATA,
                low_watermark=low_watermark,
                high_watermark=high_watermark,
                rows_extracted=0,
                landing_path=None,
                records=[],
            )

        # Step 5: Begin run audit
        batch_id = generate_deterministic_batch_id(table_name, low_watermark, high_watermark)
        audit = WatermarkRunAudit(
            run_id=run_id,
            table_name=table_name,
            batch_id=batch_id,
            low_watermark=low_watermark,
            high_watermark=high_watermark,
            status=WatermarkRunStatus.RUNNING,
            rows_extracted=0,
            landing_path=None,
            started_at=now_str,
        )
        self.control_store.start_run_audit(audit)

        landing_path_str: str | None = None
        try:
            # Step 6: Extract bounded rows
            records = self.source_adapter.extract_bounded_window(
                table_name=table_name,
                low_watermark=low_watermark,
                high_watermark=high_watermark,
                watermark_column=watermark_column,
            )

            if not records:
                # No rows found in window despite HIGH > LOW
                self.control_store.complete_run_audit(
                    run_id=run_id,
                    status=WatermarkRunStatus.NO_DATA,
                    rows_extracted=0,
                )
                return ExtractionResult(
                    table_name=table_name,
                    run_id=run_id,
                    batch_id=batch_id,
                    status=WatermarkRunStatus.NO_DATA,
                    low_watermark=low_watermark,
                    high_watermark=high_watermark,
                    rows_extracted=0,
                    landing_path=None,
                    records=[],
                )

            # Step 7: Write landing output atomically & verify
            landing_file = write_watermark_batch_jsonl(
                table_name=table_name,
                batch_id=batch_id,
                records=records,
                base_dir=self.landing_base_dir,
            )
            landing_path_str = str(landing_file)

            if not landing_file.exists():
                raise WatermarkError(f"Landing file verification failed: {landing_file} not found.")

            # Step 8: Optional simulated failure point before watermark commit
            if fail_before_commit:
                raise RuntimeError("Simulated failure after landing but before checkpoint commit.")

            # Step 9: Commit watermark checkpoint (optimistic concurrency version check)
            self.control_store.commit_watermark_checkpoint(
                table_name=table_name,
                expected_version=expected_version,
                new_watermark=high_watermark,
                run_id=run_id,
            )

            # Step 10: Mark run audit SUCCESS
            self.control_store.complete_run_audit(
                run_id=run_id,
                status=WatermarkRunStatus.SUCCESS,
                rows_extracted=len(records),
                landing_path=landing_path_str,
            )

            return ExtractionResult(
                table_name=table_name,
                run_id=run_id,
                batch_id=batch_id,
                status=WatermarkRunStatus.SUCCESS,
                low_watermark=low_watermark,
                high_watermark=high_watermark,
                rows_extracted=len(records),
                landing_path=landing_path_str,
                records=records,
            )

        except Exception as e:
            self.control_store.complete_run_audit(
                run_id=run_id,
                status=WatermarkRunStatus.FAILED,
                rows_extracted=0,
                landing_path=landing_path_str,
                error_message=str(e),
            )
            raise

    def run_all_tables(
        self,
        table_names: list[str] | None = None,
        watermark_column: str = "updated_at",
    ) -> dict[str, ExtractionResult]:
        """Execute watermark extraction across all configured source tables."""
        target_tables = table_names or ["accounts", "subscriptions", "invoices", "payments"]
        results: dict[str, ExtractionResult] = {}

        for table in target_tables:
            results[table] = self.run_table_extraction(
                table_name=table,
                watermark_column=watermark_column,
            )

        return results
