"""Unit tests for Databricks Lakeflow configuration, table specifications, and API compliance."""

from pathlib import Path

import pytest

from databricks.lakeflow.config import LakeflowConfig
from databricks.lakeflow.contracts import TABLE_CDC_SPECS


def test_lakeflow_config_defaults():
    """Verify default Lakeflow configuration parameters."""
    cfg = LakeflowConfig()
    assert cfg.catalog == "main"
    assert cfg.schema == "cdc_portfolio"
    assert cfg.snapshot_base_path == "/Volumes/main/cdc_portfolio/cdc_data/source_snapshot"
    assert cfg.normalized_cdc_base_path == "/Volumes/main/cdc_portfolio/cdc_data/normalized_cdc"
    assert cfg.tombstone_gc_threshold_seconds == 604800
    assert cfg.target_prefix == ""
    assert cfg.ignore_null_updates is False


def test_lakeflow_config_from_env(monkeypatch: pytest.MonkeyPatch):
    """Verify LakeflowConfig parses environment variables correctly."""
    monkeypatch.setenv("LAKEFLOW_CATALOG", "prod_catalog")
    monkeypatch.setenv("LAKEFLOW_SCHEMA", "finance_cdc")
    monkeypatch.setenv("LAKEFLOW_SNAPSHOT_PATH", "/Volumes/prod/finance/snapshots")
    monkeypatch.setenv("LAKEFLOW_NORMALIZED_CDC_PATH", "/Volumes/prod/finance/cdc")
    monkeypatch.setenv("LAKEFLOW_TOMBSTONE_GC_SECONDS", "1209600")
    monkeypatch.setenv("LAKEFLOW_TARGET_PREFIX", "stg_")
    monkeypatch.setenv("LAKEFLOW_IGNORE_NULL_UPDATES", "true")

    cfg = LakeflowConfig.from_env()
    assert cfg.catalog == "prod_catalog"
    assert cfg.schema == "finance_cdc"
    assert cfg.snapshot_base_path == "/Volumes/prod/finance/snapshots"
    assert cfg.normalized_cdc_base_path == "/Volumes/prod/finance/cdc"
    assert cfg.tombstone_gc_threshold_seconds == 1209600
    assert cfg.target_prefix == "stg_"
    assert cfg.ignore_null_updates is True


def test_table_cdc_specs_completeness():
    """Verify all four domain tables have explicit TableCDCSpec definitions."""
    assert set(TABLE_CDC_SPECS.keys()) == {"accounts", "subscriptions", "invoices", "payments"}


def test_table_cdc_specs_primary_keys():
    """Verify each table defines its authoritative primary key."""
    assert TABLE_CDC_SPECS["accounts"].primary_key == "account_id"
    assert TABLE_CDC_SPECS["subscriptions"].primary_key == "subscription_id"
    assert TABLE_CDC_SPECS["invoices"].primary_key == "invoice_id"
    assert TABLE_CDC_SPECS["payments"].primary_key == "payment_id"


def test_table_cdc_specs_unique_flow_names():
    """Verify all 10 AUTO CDC flow names (8 Type 1 + 2 Type 2) are globally unique."""
    flow_names = []
    for spec in TABLE_CDC_SPECS.values():
        flow_names.append(spec.hydration_flow_name)
        flow_names.append(spec.continuous_flow_name)
        if spec.history_hydration_flow_name:
            flow_names.append(spec.history_hydration_flow_name)
        if spec.history_continuous_flow_name:
            flow_names.append(spec.history_continuous_flow_name)

    assert len(flow_names) == 10
    assert len(set(flow_names)) == 10


def test_table_cdc_specs_unique_target_names():
    """Verify all target streaming table names are distinct."""
    target_names = []
    for spec in TABLE_CDC_SPECS.values():
        target_names.append(spec.target_table_current)
        if spec.target_table_history:
            target_names.append(spec.target_table_history)

    assert len(target_names) == 5
    assert len(set(target_names)) == 5
    assert set(target_names) == {
        "accounts_current",
        "subscriptions_current",
        "invoices_current",
        "payments_current",
        "subscriptions_history",
    }


def test_subscriptions_scd2_history_tracking_columns():
    """Verify subscriptions SCD Type 2 defines historical tracking across all business columns."""
    sub_spec = TABLE_CDC_SPECS["subscriptions"]
    assert sub_spec.target_table_history == "subscriptions_history"
    assert sub_spec.history_track_columns == [
        "account_id",
        "plan_name",
        "billing_cycle",
        "monthly_amount",
        "status",
        "start_date",
        "end_date",
    ]


def test_excluded_columns_contains_control_fields():
    """Verify that all table specs exclude operation and sequence_number from target tables."""
    for spec in TABLE_CDC_SPECS.values():
        assert "operation" in spec.excluded_columns
        assert "sequence_number" in spec.excluded_columns


def test_no_deprecated_api_in_module5_files():
    """Verify that Module 5 files do not use deprecated apply_changes APIs."""
    module5_root = Path("databricks/lakeflow")
    banned_tokens = [
        "dlt.apply_changes",
        "apply_changes(",
        "APPLY CHANGES INTO",
    ]

    files_to_check = [
        module5_root / "config.py",
        module5_root / "contracts.py",
        module5_root / "pipeline.py",
        module5_root / "sql" / "auto_cdc_reference.sql",
    ]

    for file_path in files_to_check:
        assert file_path.exists(), f"File {file_path} missing"
        content = file_path.read_text(encoding="utf-8")
        for token in banned_tokens:
            assert token not in content, f"Found banned deprecated token '{token}' in {file_path}"


def test_sql_reference_contains_auto_cdc_and_scd2():
    """Verify SQL reference implementation contains modern AUTO CDC ONCE and SCD Type 2 statements."""
    sql_path = Path("databricks/lakeflow/sql/auto_cdc_reference.sql")
    content = sql_path.read_text(encoding="utf-8")

    assert "CREATE FLOW accounts_initial_hydration" in content
    assert "AS AUTO CDC ONCE" in content
    assert "CREATE FLOW accounts_continuous_cdc" in content
    assert "AS AUTO CDC" in content
    assert "APPLY AS DELETES (operation = 'DELETE')" in content
    assert "STORED AS SCD TYPE 1" in content
    assert "CREATE OR REFRESH STREAMING TABLE subscriptions_history" in content
    assert "STORED AS SCD TYPE 2" in content
    assert "TRACK (account_id, plan_name, billing_cycle, monthly_amount, status, start_date, end_date)" in content


def test_normal_package_import_isolation():
    """Verify that standard local packages import cleanly without requiring Databricks Lakeflow runtime."""
    import src
    import src.cdc
    import src.merge
    import src.normalization
    import src.source
    import src.watermark

    assert src is not None
    assert src.source is not None
    assert src.cdc is not None
    assert src.watermark is not None
    assert src.normalization is not None
    assert src.merge is not None
