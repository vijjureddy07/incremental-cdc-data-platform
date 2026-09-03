"""Unit tests for Databricks Lakeflow declarative pipeline AST, registration harness, and flows."""

import ast
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from databricks.lakeflow.config import LakeflowConfig
from databricks.lakeflow.contracts import TABLE_CDC_SPECS


@dataclass
class RecordedTable:
    name: str
    comment: str | None
    table_properties: dict[str, str] | None


@dataclass
class RecordedView:
    name: str
    comment: str | None


@dataclass
class RecordedFlow:
    name: str
    target: str
    source: str
    keys: list[str]
    sequence_by: str
    stored_as_scd_type: int
    once: bool
    apply_as_deletes: Any
    except_column_list: list[str]
    track_history_column_list: list[str] | None
    ignore_null_updates: bool


class FakePipelines:
    """Lightweight test harness capturing declarative registrations."""

    def __init__(self) -> None:
        self.tables: list[RecordedTable] = []
        self.views: list[RecordedView] = []
        self.flows: list[RecordedFlow] = []
        self.view_functions: dict[str, Callable] = {}

    def create_streaming_table(
        self,
        name: str,
        comment: str | None = None,
        table_properties: dict[str, str] | None = None,
    ) -> None:
        self.tables.append(
            RecordedTable(name=name, comment=comment, table_properties=table_properties)
        )

    def create_auto_cdc_flow(
        self,
        name: str,
        target: str,
        source: str,
        keys: list[str],
        sequence_by: str,
        stored_as_scd_type: int = 1,
        once: bool = False,
        apply_as_deletes: Any = None,
        except_column_list: list[str] | None = None,
        track_history_column_list: list[str] | None = None,
        ignore_null_updates: bool = False,
        **kwargs: Any,
    ) -> None:
        self.flows.append(
            RecordedFlow(
                name=name,
                target=target,
                source=source,
                keys=keys,
                sequence_by=sequence_by,
                stored_as_scd_type=stored_as_scd_type,
                once=once,
                apply_as_deletes=apply_as_deletes,
                except_column_list=except_column_list or [],
                track_history_column_list=track_history_column_list,
                ignore_null_updates=ignore_null_updates,
            )
        )

    def temporary_view(self, name: str, comment: str | None = None) -> Callable:
        def decorator(fn: Callable) -> Callable:
            self.views.append(RecordedView(name=name, comment=comment))
            self.view_functions[name] = fn
            return fn

        return decorator


def test_lakeflow_pipeline_ast_compilation():
    """Verify Lakeflow pipeline source code is syntactically valid and compiles cleanly under Python AST."""
    pipeline_path = Path("databricks/lakeflow/pipeline.py")
    assert pipeline_path.exists()

    code = pipeline_path.read_text(encoding="utf-8")
    tree = ast.parse(code, filename=str(pipeline_path))
    compiled = compile(tree, filename=str(pipeline_path), mode="exec")
    assert compiled is not None


def test_lakeflow_pipeline_top_level_automatic_registration(
    spark_session: SparkSession, monkeypatch: pytest.MonkeyPatch
):
    """Verify evaluating/importing pipeline.py automatically registers streaming tables, AUTO CDC flows, and temporary views."""
    fake_dp = FakePipelines()

    mock_pipelines_module = MagicMock()
    mock_pipelines_module.create_streaming_table = fake_dp.create_streaming_table
    mock_pipelines_module.create_auto_cdc_flow = fake_dp.create_auto_cdc_flow
    mock_pipelines_module.temporary_view = fake_dp.temporary_view

    monkeypatch.setitem(sys.modules, "pyspark.pipelines", mock_pipelines_module)

    # Remove existing pipeline module from sys.modules so fresh import runs top-level registration once
    sys.modules.pop("databricks.lakeflow.pipeline", None)
    import databricks.lakeflow.pipeline as pipeline_mod  # noqa: F401

    # 1. Target Streaming Tables Verification (4 Type 1 + 1 Type 2 = 5 tables)
    assert len(fake_dp.tables) == 5
    registered_table_names = {t.name for t in fake_dp.tables}
    expected_tables = {
        "accounts_current",
        "subscriptions_current",
        "invoices_current",
        "payments_current",
        "subscriptions_history",
    }
    assert registered_table_names == expected_tables

    # Verify all tables have tombstone GC threshold property configured
    for t in fake_dp.tables:
        assert t.table_properties is not None
        assert "pipelines.cdc.tombstoneGCThresholdInSeconds" in t.table_properties
        assert t.table_properties["pipelines.cdc.tombstoneGCThresholdInSeconds"] == "604800"

    # 2. Flows Count Verification (8 Type 1 + 2 Type 2 = 10 flows)
    assert len(fake_dp.flows) == 10

    # 3. Hydration Flows Verification (5 flows with once=True)
    hydration_flows = [f for f in fake_dp.flows if f.once is True]
    assert len(hydration_flows) == 5
    for hf in hydration_flows:
        assert hf.sequence_by == "sequence_number"
        assert "operation" in hf.except_column_list
        assert "sequence_number" in hf.except_column_list

    # 4. Continuous Flows Verification (5 continuous flows)
    continuous_flows = [f for f in fake_dp.flows if f.once is False]
    assert len(continuous_flows) == 5
    for cf in continuous_flows:
        assert cf.sequence_by == "sequence_number"
        assert cf.apply_as_deletes is not None  # DELETE condition expression present
        assert "operation" in cf.except_column_list
        assert "sequence_number" in cf.except_column_list

    # 5. Type 1 Current Targets Verification (4 tables, each having 1 hydration + 1 continuous flow)
    for spec in TABLE_CDC_SPECS.values():
        tbl_flows = [f for f in fake_dp.flows if f.target == spec.target_table_current]
        assert len(tbl_flows) == 2
        f_names = {f.name for f in tbl_flows}
        assert f_names == {spec.hydration_flow_name, spec.continuous_flow_name}
        for f in tbl_flows:
            assert f.keys == [spec.primary_key]
            assert f.stored_as_scd_type == 1

    # 6. Type 2 History Target Verification (subscriptions_history)
    sub_spec = TABLE_CDC_SPECS["subscriptions"]
    history_flows = [f for f in fake_dp.flows if f.target == sub_spec.target_table_history]
    assert len(history_flows) == 2
    h_names = {f.name for f in history_flows}
    assert h_names == {
        sub_spec.history_hydration_flow_name,
        sub_spec.history_continuous_flow_name,
    }

    for hf in history_flows:
        assert hf.stored_as_scd_type == 2
        assert hf.keys == ["subscription_id"]
        assert hf.track_history_column_list == sub_spec.history_track_columns
        assert len(hf.track_history_column_list) == 7

    # 7. Source Streaming Temporary Views Verification (4 snapshot + 4 cdc = 8 source views)
    assert len(fake_dp.views) == 8
    assert len(fake_dp.view_functions) == 8
    expected_views = {
        "accounts_snapshot_source",
        "accounts_cdc_source",
        "subscriptions_snapshot_source",
        "subscriptions_cdc_source",
        "invoices_snapshot_source",
        "invoices_cdc_source",
        "payments_snapshot_source",
        "payments_cdc_source",
    }
    assert {v.name for v in fake_dp.views} == expected_views


def test_lakeflow_pipeline_custom_tombstone_config_registration(
    spark_session: SparkSession, monkeypatch: pytest.MonkeyPatch
):
    """Verify passing custom LakeflowConfig sets custom tombstone GC property on all streaming tables."""
    fake_dp = FakePipelines()

    mock_pipelines_module = MagicMock()
    mock_pipelines_module.create_streaming_table = fake_dp.create_streaming_table
    mock_pipelines_module.create_auto_cdc_flow = fake_dp.create_auto_cdc_flow
    mock_pipelines_module.temporary_view = fake_dp.temporary_view
    monkeypatch.setitem(sys.modules, "pyspark.pipelines", mock_pipelines_module)

    sys.modules.pop("databricks.lakeflow.pipeline", None)
    import databricks.lakeflow.pipeline as pipeline_mod

    # Clear initial registrations from module import
    fake_dp.tables.clear()
    fake_dp.flows.clear()
    fake_dp.views.clear()
    fake_dp.view_functions.clear()

    custom_cfg = LakeflowConfig(tombstone_gc_threshold_seconds=1209600)
    pipeline_mod.register_lakeflow_pipeline(custom_cfg)

    # 5 tables created during custom call
    assert len(fake_dp.tables) == 5
    for t in fake_dp.tables:
        assert t.table_properties == {"pipelines.cdc.tombstoneGCThresholdInSeconds": "1209600"}
