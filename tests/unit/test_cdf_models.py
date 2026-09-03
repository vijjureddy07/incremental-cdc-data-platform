"""Unit tests for Delta Change Data Feed models, results, and exception hierarchies."""

from dataclasses import FrozenInstanceError

import pytest

from src.cdf.models import (
    CDF_METADATA_COLUMNS,
    SUPPORTED_CHANGE_TYPES,
    CDFConsumptionResult,
    CDFError,
    CDFInvalidRangeError,
    CDFNotEnabledError,
    CDFSourceAlreadyRegisteredError,
    CDFSourceNotFoundError,
    CDFSourceRegistration,
)


def test_supported_change_types_and_metadata_columns():
    """Verify standard supported change types and metadata columns match Delta CDF specifications."""
    assert SUPPORTED_CHANGE_TYPES == {"insert", "update_preimage", "update_postimage", "delete"}
    assert CDF_METADATA_COLUMNS == ["_change_type", "_commit_version", "_commit_timestamp"]


def test_cdf_exception_hierarchy():
    """Verify that domain exceptions inherit cleanly from base CDFError."""
    assert issubclass(CDFSourceNotFoundError, CDFError)
    assert issubclass(CDFSourceAlreadyRegisteredError, CDFError)
    assert issubclass(CDFNotEnabledError, CDFError)
    assert issubclass(CDFInvalidRangeError, CDFError)


def test_cdf_source_registration_immutability():
    """Verify CDFSourceRegistration is a frozen dataclass with correct attributes."""
    reg = CDFSourceRegistration(
        source_table="accounts",
        source_path="/path/to/accounts",
        cdf_start_version=1,
        last_processed_version=0,
        registered_at="2026-05-11T00:00:00Z",
        last_updated_at="2026-05-11T00:00:00Z",
    )
    assert reg.source_table == "accounts"
    assert reg.cdf_start_version == 1
    assert reg.last_processed_version == 0

    with pytest.raises(FrozenInstanceError):
        reg.last_processed_version = 2  # type: ignore[misc]


def test_cdf_consumption_result_defaults():
    """Verify CDFConsumptionResult fields and default no_op flag."""
    res = CDFConsumptionResult(
        source_table="subscriptions",
        start_version=2,
        end_version=4,
        input_change_rows=10,
        archive_rows_inserted=10,
        checkpoint_before=1,
        checkpoint_after=4,
    )
    assert res.source_table == "subscriptions"
    assert res.start_version == 2
    assert res.end_version == 4
    assert res.input_change_rows == 10
    assert res.archive_rows_inserted == 10
    assert res.no_op is False

    res_noop = CDFConsumptionResult(
        source_table="subscriptions",
        start_version=5,
        end_version=4,
        input_change_rows=0,
        archive_rows_inserted=0,
        checkpoint_before=4,
        checkpoint_after=4,
        no_op=True,
    )
    assert res_noop.no_op is True
