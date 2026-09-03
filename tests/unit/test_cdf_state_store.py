"""Unit tests for SQLite downstream Change Data Feed state store and checkpointing."""

from pathlib import Path

import pytest

from src.cdf.models import (
    CDFInvalidRangeError,
    CDFSourceAlreadyRegisteredError,
    CDFSourceNotFoundError,
)
from src.cdf.state_store import CDFStateStore


def test_state_store_registration_and_initial_checkpoint(tmp_path: Path):
    """Verify registration records start version and initializes checkpoint to start_version - 1."""
    db_path = tmp_path / "test_cdf.db"
    store = CDFStateStore(db_path=db_path)

    reg = store.register_source(
        source_table="accounts",
        source_path="/data/delta/accounts",
        cdf_start_version=5,
    )
    assert reg.source_table == "accounts"
    assert reg.cdf_start_version == 5
    assert reg.last_processed_version == 4  # 5 - 1

    fetched = store.get_source("accounts")
    assert fetched is not None
    assert fetched.source_table == "accounts"
    assert fetched.cdf_start_version == 5
    assert fetched.last_processed_version == 4


def test_state_store_idempotent_registration(tmp_path: Path):
    """Verify if_exists='ignore' returns existing registration and 'error' raises exception."""
    store = CDFStateStore(db_path=tmp_path / "test_cdf.db")

    reg1 = store.register_source("subscriptions", "/data/delta/subscriptions", cdf_start_version=2)
    reg2 = store.register_source(
        "subscriptions", "/other/path", cdf_start_version=10, if_exists="ignore"
    )

    assert reg1 == reg2

    with pytest.raises(CDFSourceAlreadyRegisteredError):
        store.register_source(
            "subscriptions", "/other/path", cdf_start_version=10, if_exists="error"
        )


def test_state_store_advance_checkpoint(tmp_path: Path):
    """Verify advancing checkpoint moves version forward monotonically."""
    store = CDFStateStore(db_path=tmp_path / "test_cdf.db")
    store.register_source("invoices", "/data/delta/invoices", cdf_start_version=1)

    updated = store.advance_checkpoint("invoices", 3)
    assert updated.last_processed_version == 3

    # Backwards checkpoint attempt is rejected
    with pytest.raises(CDFInvalidRangeError):
        store.advance_checkpoint("invoices", 2)


def test_state_store_non_existent_source(tmp_path: Path):
    """Verify querying or advancing un-registered source returns None or raises error."""
    store = CDFStateStore(db_path=tmp_path / "test_cdf.db")
    assert store.get_source("unknown_table") is None

    with pytest.raises(CDFSourceNotFoundError):
        store.advance_checkpoint("unknown_table", 5)


def test_state_store_multi_table_isolation(tmp_path: Path):
    """Verify that multiple tables maintain strictly isolated checkpoints."""
    store = CDFStateStore(db_path=tmp_path / "test_cdf.db")

    store.register_source("accounts", "/data/accounts", cdf_start_version=1)
    store.register_source("payments", "/data/payments", cdf_start_version=10)

    # Advance accounts only
    store.advance_checkpoint("accounts", 8)

    acc = store.get_source("accounts")
    pay = store.get_source("payments")

    assert acc is not None and acc.last_processed_version == 8
    assert pay is not None and pay.last_processed_version == 9  # 10 - 1 unchanged


def test_state_store_persistence_across_instances(tmp_path: Path):
    """Verify registered state and checkpoints persist cleanly across new store instances."""
    db_path = tmp_path / "test_persistent.db"
    store1 = CDFStateStore(db_path=db_path)
    store1.register_source("accounts", "/data/accounts", cdf_start_version=3)
    store1.advance_checkpoint("accounts", 6)

    store2 = CDFStateStore(db_path=db_path)
    fetched = store2.get_source("accounts")
    assert fetched is not None
    assert fetched.last_processed_version == 6
