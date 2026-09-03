"""Unit tests for CDFDownstreamPipeline orchestrator, failure recovery, and no-op handling."""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.cdf.archive import CDFArchiveStore
from src.cdf.pipeline import CDFDownstreamPipeline
from src.cdf.reader import CDFReader
from src.cdf.state_store import CDFStateStore
from src.merge.target_store import DeltaTargetStore


def test_pipeline_registration_and_noop(spark_session: SparkSession, tmp_path: Path):
    """Verify table registration and subsequent no-op when no new commits exist."""
    current_dir = tmp_path / "current"
    control_db = tmp_path / "control.db"
    archive_dir = tmp_path / "archive"

    # Create dummy accounts table
    acc_path = current_dir / "accounts"
    df = spark_session.createDataFrame([("ACC-1", "Alpha")], ["account_id", "name"])
    df.write.format("delta").save(str(acc_path))

    target_store = DeltaTargetStore(spark_session, target_base_dir=current_dir)
    state_store = CDFStateStore(db_path=control_db)
    reader = CDFReader(spark_session)
    archive_store = CDFArchiveStore(spark_session, archive_base_dir=archive_dir)

    pipeline = CDFDownstreamPipeline(
        spark=spark_session,
        target_store=target_store,
        state_store=state_store,
        reader=reader,
        archive_store=archive_store,
    )

    reg = pipeline.register_table("accounts")
    # Enabling CDF bumped version from 0 to 1
    assert reg.cdf_start_version == 1
    assert reg.last_processed_version == 0

    # First consume from version 1 to 1: version 1 was ALTER TABLE, so 0 CDF rows
    res = pipeline.consume_table("accounts")
    assert res.no_op is False
    assert res.start_version == 1
    assert res.end_version == 1
    assert res.checkpoint_after == 1

    # Second consume without new commits: should be a clean no-op
    noop_res = pipeline.consume_table("accounts")
    assert noop_res.no_op is True
    assert noop_res.archive_rows_inserted == 0
    assert noop_res.checkpoint_after == 1


def test_pipeline_failure_recovery_before_checkpoint(spark_session: SparkSession, tmp_path: Path):
    """Verify recovery when a failure occurs after archive write but before checkpoint commit.

    Expected lifecycle:
    1. New changes exist at V2.
    2. Archive write succeeds.
    3. Failure hook simulates crash before checkpoint commit.
    4. State store checkpoint remains at V1.
    5. Retry run rereads V2.
    6. Archive MERGE inserts 0 duplicates.
    7. Checkpoint advances to V2.
    """
    current_dir = tmp_path / "current"
    control_db = tmp_path / "control.db"
    archive_dir = tmp_path / "archive"

    acc_path = current_dir / "accounts"
    df = spark_session.createDataFrame([("ACC-1", "Alpha")], ["account_id", "name"])
    df.write.format("delta").save(str(acc_path))

    target_store = DeltaTargetStore(spark_session, target_base_dir=current_dir)
    state_store = CDFStateStore(db_path=control_db)
    reader = CDFReader(spark_session)
    archive_store = CDFArchiveStore(spark_session, archive_base_dir=archive_dir)

    pipeline = CDFDownstreamPipeline(
        spark=spark_session,
        target_store=target_store,
        state_store=state_store,
        reader=reader,
        archive_store=archive_store,
    )

    pipeline.register_table("accounts")
    # Advance to V1 (the ALTER TABLE commit)
    pipeline.consume_table("accounts")
    assert state_store.get_source("accounts").last_processed_version == 1

    # Add commit V2 with new data
    df_v2 = spark_session.createDataFrame([("ACC-2", "Beta")], ["account_id", "name"])
    df_v2.write.format("delta").mode("append").save(str(acc_path))

    # Define failure hook simulating crash
    def crash_hook():
        raise RuntimeError("Simulated crash after archive write before checkpoint")

    with pytest.raises(RuntimeError, match="Simulated crash"):
        pipeline.consume_table("accounts", failure_hook=crash_hook)

    # Verify state store was NOT advanced (still at V1)
    state_before_retry = state_store.get_source("accounts")
    assert state_before_retry.last_processed_version == 1

    # Verify archive DID receive the rows
    archive_df_before = archive_store.read_archive("accounts")
    assert archive_df_before.count() == 1

    # Retry recovery run: should reprocess V2, insert 0 duplicates, and advance checkpoint to V2
    retry_res = pipeline.consume_table("accounts")
    assert retry_res.start_version == 2
    assert retry_res.end_version == 2
    assert retry_res.input_change_rows == 1
    assert retry_res.archive_rows_inserted == 0  # 0 duplicates via MERGE
    assert retry_res.checkpoint_after == 2

    # Verify final state store is V2 and archive row count is still exactly 1
    assert state_store.get_source("accounts").last_processed_version == 2
    assert archive_store.read_archive("accounts").count() == 1


def test_pipeline_observational_replay_does_not_mutate_checkpoint(
    spark_session: SparkSession, tmp_path: Path
):
    """Verify replay_range returns CDF change records without modifying the consumer checkpoint."""
    current_dir = tmp_path / "current"
    control_db = tmp_path / "control.db"
    archive_dir = tmp_path / "archive"

    acc_path = current_dir / "accounts"
    df = spark_session.createDataFrame([("ACC-1", "Alpha")], ["account_id", "name"])
    df.write.format("delta").save(str(acc_path))

    target_store = DeltaTargetStore(spark_session, target_base_dir=current_dir)
    state_store = CDFStateStore(db_path=control_db)
    reader = CDFReader(spark_session)
    archive_store = CDFArchiveStore(spark_session, archive_base_dir=archive_dir)

    pipeline = CDFDownstreamPipeline(
        spark=spark_session,
        target_store=target_store,
        state_store=state_store,
        reader=reader,
        archive_store=archive_store,
    )

    pipeline.register_table("accounts")
    pipeline.consume_table("accounts")

    df_v2 = spark_session.createDataFrame([("ACC-2", "Beta")], ["account_id", "name"])
    df_v2.write.format("delta").mode("append").save(str(acc_path))

    # Checkpoint is at 1. Observational replay of V2:
    replay_df = pipeline.replay_range("accounts", start_version=2, end_version=2)
    assert replay_df.count() == 1
    assert replay_df.first()["account_id"] == "ACC-2"

    # Checkpoint must still be at 1
    assert state_store.get_source("accounts").last_processed_version == 1
