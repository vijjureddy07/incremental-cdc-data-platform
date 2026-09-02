"""Transactional pipeline orchestrator for watermark incremental ingestion with durable recovery."""

import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from src.utils.helpers import format_iso_timestamp
from src.watermark.control_store import SQLiteWatermarkControlStore
from src.watermark.landing import (
    generate_deterministic_batch_id,
    read_watermark_batch_jsonl,
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
        fail_before_landing: bool = False,
        fail_before_commit: bool = False,
    ) -> ExtractionResult:
        """Execute a single transactional watermark extraction run for a table.

        Strict Order of Operations:
        1. Register RUNNING audit record before source work begins
        2. Read current LOW watermark & expected version from control store
        3. Check for recoverable window from prior failed/abandoned attempts
           - If recoverable: REUSE prior HIGH and deterministic batch_id
           - If none: capture fresh HIGH watermark from source and compute batch_id
        4. Update audit record with resolved extraction window boundaries
        5. If HIGH <= LOW: complete audit as NO_DATA and return without advancing watermark
        6. Extract bounded rows: LOW < cursor <= HIGH
        7. [Optional fail_before_landing hook]
        8. Write landing JSONL file atomically
        9. Read back and verify landed file existence AND exact row count
        10. [Optional fail_before_commit hook]
        11. Atomically commit watermark checkpoint (CAS version check) AND mark audit SUCCESS
        """
        now_str = format_iso_timestamp(datetime.now(UTC))
        run_id = f"run_{table_name}_{uuid.uuid4().hex[:12]}"
        landing_path_str: str | None = None

        # Step 1: Start audit record immediately in RUNNING status
        init_audit = WatermarkRunAudit(
            run_id=run_id,
            table_name=table_name,
            status=WatermarkRunStatus.RUNNING,
            started_at=now_str,
        )
        self.control_store.start_run_audit(init_audit)

        try:
            # Step 2: Read current persisted LOW watermark state
            pk_col = self.source_adapter.get_primary_key_column(table_name)
            state = self.control_store.get_or_create_watermark_state(
                table_name=table_name,
                watermark_column=watermark_column,
                tie_breaker_column=pk_col,
            )
            low_watermark = state.last_watermark
            expected_version = state.version

            # Step 3: Check for recoverable uncommitted window
            recoverable = self.control_store.get_recoverable_window(table_name)

            if recoverable is not None and recoverable.batch_id is not None:
                # Reuse prior HIGH and batch_id for deterministic recovery
                high_watermark = recoverable.high_watermark
                batch_id = recoverable.batch_id

                # If the recoverable attempt was abandoned in RUNNING status, mark it superseded
                if recoverable.status == WatermarkRunStatus.RUNNING:
                    self.control_store.mark_superseded(recoverable.run_id, run_id)
            else:
                # Capture fresh source HIGH watermark
                high_watermark = self.source_adapter.capture_source_high_watermark(
                    table_name=table_name,
                    watermark_column=watermark_column,
                )

                # Optional hook (for bounded-high testing)
                if post_capture_hook is not None:
                    post_capture_hook()

                batch_id = generate_deterministic_batch_id(
                    table_name, low_watermark, high_watermark
                )

            # Step 4: Update run audit record with window boundaries and batch_id
            self.control_store.update_run_audit_window(
                run_id=run_id,
                expected_version=expected_version,
                low_watermark=low_watermark,
                high_watermark=high_watermark,
                batch_id=batch_id,
            )

            # Step 5: Evaluate NO_DATA condition
            if high_watermark.is_initial or high_watermark <= low_watermark:
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

            # Step 6: Extract bounded rows
            records = self.source_adapter.extract_bounded_window(
                table_name=table_name,
                low_watermark=low_watermark,
                high_watermark=high_watermark,
                watermark_column=watermark_column,
            )

            if not records:
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

            # Step 7: Optional failure before landing hook
            if fail_before_landing:
                raise RuntimeError("Simulated failure before landing succeeds.")

            # Step 8: Write landing output atomically
            landing_file = write_watermark_batch_jsonl(
                table_name=table_name,
                batch_id=batch_id,
                records=records,
                base_dir=self.landing_base_dir,
            )
            landing_path_str = str(landing_file)

            # Step 9: Verify landing file existence and row count
            if not landing_file.exists():
                raise WatermarkError(f"Landing file verification failed: {landing_file} not found.")

            landed_records = read_watermark_batch_jsonl(landing_file)
            if len(landed_records) != len(records):
                raise WatermarkError(
                    f"Landing row count mismatch for table '{table_name}': "
                    f"extracted {len(records)} rows, but found {len(landed_records)} rows on disk."
                )

            # Step 10: Optional simulated failure point before watermark commit
            if fail_before_commit:
                raise RuntimeError("Simulated failure after landing but before checkpoint commit.")

            # Step 11: Atomically commit watermark checkpoint and mark audit SUCCESS
            self.control_store.commit_successful_run(
                table_name=table_name,
                expected_version=expected_version,
                new_watermark=high_watermark,
                run_id=run_id,
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
