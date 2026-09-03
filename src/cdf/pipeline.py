"""Orchestrator for downstream Change Data Feed consumption, archive write, and checkpoint recovery."""

from collections.abc import Callable
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.cdf.archive import CDFArchiveStore
from src.cdf.models import (
    CDFConsumptionResult,
    CDFSourceNotFoundError,
    CDFSourceRegistration,
)
from src.cdf.reader import CDFReader
from src.cdf.state_store import CDFStateStore
from src.merge.target_store import DeltaTargetStore


class CDFDownstreamPipeline:
    """Downstream pipeline consuming Delta Change Data Feed into permanent archives."""

    def __init__(
        self,
        spark: SparkSession,
        target_store: DeltaTargetStore | None = None,
        state_store: CDFStateStore | None = None,
        reader: CDFReader | None = None,
        archive_store: CDFArchiveStore | None = None,
    ) -> None:
        self.spark = spark
        self.target_store = target_store or DeltaTargetStore(spark)
        self.state_store = state_store or CDFStateStore()
        self.reader = reader or CDFReader(spark)
        self.archive_store = archive_store or CDFArchiveStore(spark)

    def register_table(
        self,
        table_name: str,
        if_exists: str = "ignore",
    ) -> CDFSourceRegistration:
        """Register a Delta target table for downstream CDF tracking.

        Enables CDF on the target table if not already enabled, records the enabling commit
        version as cdf_start_version, and initializes last_processed_version = cdf_start_version - 1.

        Args:
            table_name: Domain table name (e.g. 'accounts').
            if_exists: 'ignore' to return existing registration, 'error' to raise.

        Returns:
            CDFSourceRegistration instance.
        """
        source_path = self.target_store.get_table_path(table_name)
        if not self.target_store.table_exists(table_name):
            raise CDFSourceNotFoundError(
                f"Cannot register '{table_name}': Delta table does not exist at {source_path}"
            )

        if not self.reader.is_cdf_enabled(source_path):
            start_version = self.reader.enable_cdf(source_path)
        else:
            start_version = self.target_store.get_table_version(table_name)

        return self.state_store.register_source(
            source_table=table_name,
            source_path=source_path,
            cdf_start_version=start_version,
            if_exists=if_exists,
        )

    def consume_table(
        self,
        table_name: str,
        target_version: int | None = None,
        failure_hook: Callable[[], None] | None = None,
    ) -> CDFConsumptionResult:
        """Process an incremental window of changes from a source table into the archive.

        Args:
            table_name: Source table name.
            target_version: Optional maximum commit version to consume up to.
            failure_hook: Optional hook executed after archive write and before checkpoint commit.

        Returns:
            CDFConsumptionResult containing run metrics.
        """
        reg = self.state_store.get_source(table_name)
        if reg is None:
            raise CDFSourceNotFoundError(
                f"Source table '{table_name}' has not been registered in the CDF state store."
            )

        current_source_version = self.target_store.get_table_version(table_name)
        end_version = (
            min(target_version, current_source_version)
            if target_version is not None
            else current_source_version
        )
        start_version = reg.last_processed_version + 1

        # No new commits since last checkpoint
        if start_version > end_version:
            return CDFConsumptionResult(
                source_table=table_name,
                start_version=start_version,
                end_version=end_version,
                input_change_rows=0,
                archive_rows_inserted=0,
                checkpoint_before=reg.last_processed_version,
                checkpoint_after=reg.last_processed_version,
                no_op=True,
            )

        # Bounded read of changes in [start_version, end_version]
        change_df = self.reader.read_changes(
            reg.source_path,
            start_version=start_version,
            end_version=end_version,
        )
        input_rows = change_df.count()

        # Handle empty version window (e.g. metadata commits without data changes)
        if input_rows == 0:
            self.state_store.advance_checkpoint(table_name, end_version)
            return CDFConsumptionResult(
                source_table=table_name,
                start_version=start_version,
                end_version=end_version,
                input_change_rows=0,
                archive_rows_inserted=0,
                checkpoint_before=reg.last_processed_version,
                checkpoint_after=end_version,
                no_op=False,
            )

        # Write to downstream archive idempotently
        primary_key = self.target_store.get_primary_key(table_name)
        inserted_rows = self.archive_store.write_changes(
            source_table=table_name,
            df=change_df,
            primary_key=primary_key,
        )

        # Controlled failure hook between archive write and checkpoint commit
        if failure_hook is not None:
            failure_hook()

        # Advance checkpoint in control store
        self.state_store.advance_checkpoint(table_name, end_version)

        return CDFConsumptionResult(
            source_table=table_name,
            start_version=start_version,
            end_version=end_version,
            input_change_rows=input_rows,
            archive_rows_inserted=inserted_rows,
            checkpoint_before=reg.last_processed_version,
            checkpoint_after=end_version,
            no_op=False,
        )

    def consume_all(
        self,
        target_version: int | None = None,
    ) -> list[CDFConsumptionResult]:
        """Consume available changes for all currently registered source tables."""
        results = []
        for reg in self.state_store.list_sources():
            res = self.consume_table(reg.source_table, target_version=target_version)
            results.append(res)
        return results

    def replay_range(
        self,
        table_name: str,
        start_version: int,
        end_version: int,
    ) -> DataFrame:
        """Observational read of a bounded CDF range without updating consumer checkpoint state."""
        reg = self.state_store.get_source(table_name)
        source_path = (
            reg.source_path
            if reg is not None
            else str(Path(self.target_store.get_table_path(table_name)).resolve())
        )
        return self.reader.read_changes(
            source_path=source_path,
            start_version=start_version,
            end_version=end_version,
        )
