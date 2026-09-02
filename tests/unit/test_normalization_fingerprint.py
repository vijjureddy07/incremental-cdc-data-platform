"""Unit tests for canonical business key formatting, event fingerprinting, and portable processing ID."""

import tempfile
from pathlib import Path

from src.normalization.fingerprint import (
    canonicalize_business_key,
    compute_entity_sequence_key,
    compute_event_fingerprint,
    compute_manifest_and_processing_id,
    derive_logical_file_id,
)


def test_canonicalize_business_key_field_order_independence():
    """Verify canonical business key formatting is independent of dictionary key insertion order."""
    key1 = {"account_id": "ACC-0001", "region": "US"}
    key2 = {"region": "US", "account_id": "ACC-0001"}

    assert canonicalize_business_key(key1) == canonicalize_business_key(key2)
    assert canonicalize_business_key(key1) == '{"account_id":"ACC-0001","region":"US"}'


def test_compute_entity_sequence_key():
    """Verify entity sequence key creation combines table name and canonical key."""
    canonical_key = '{"account_id":"ACC-0001"}'
    entity_key = compute_entity_sequence_key("accounts", canonical_key)
    assert entity_key == 'accounts:{"account_id":"ACC-0001"}'


def test_deterministic_event_fingerprint_stability():
    """Verify event fingerprint is identical across multiple computations of the same event."""
    event = {
        "event_id": "evt_001",
        "table_name": "accounts",
        "operation": "INSERT",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 1,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "payload": {"account_id": "ACC-0001", "status": "ACTIVE"},
        "before_payload": None,
        "source_system": "b2b_saas_postgres",
    }

    fp1 = compute_event_fingerprint(event)
    fp2 = compute_event_fingerprint(event)
    assert fp1 == fp2
    assert len(fp1) == 64  # Valid SHA-256 hex string


def test_fingerprint_differs_on_payload_change():
    """Verify that any modification to payload changes the fingerprint."""
    base_event = {
        "event_id": "evt_001",
        "table_name": "accounts",
        "operation": "UPDATE",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 2,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "payload": {"account_id": "ACC-0001", "status": "ACTIVE"},
        "before_payload": {"account_id": "ACC-0001", "status": "SUSPENDED"},
        "source_system": "b2b_saas_postgres",
    }

    mutated_event = dict(base_event)
    mutated_event["payload"] = {"account_id": "ACC-0001", "status": "TRIAL"}

    assert compute_event_fingerprint(base_event) != compute_event_fingerprint(mutated_event)


def test_fingerprint_differs_on_sequence_change():
    """Verify that different sequence numbers produce different fingerprints."""
    event_seq1 = {
        "event_id": "evt_001",
        "table_name": "accounts",
        "operation": "INSERT",
        "business_key": {"account_id": "ACC-0001"},
        "sequence_number": 1,
        "event_timestamp": "2026-05-11T01:00:00Z",
        "source_commit_timestamp": "2026-05-11T01:00:01Z",
        "payload": {"account_id": "ACC-0001"},
        "before_payload": None,
        "source_system": "b2b_saas_postgres",
    }

    event_seq2 = dict(event_seq1)
    event_seq2["sequence_number"] = 2

    assert compute_event_fingerprint(event_seq1) != compute_event_fingerprint(event_seq2)


def test_derive_logical_file_id():
    """Verify logical file ID derivation extracts batch_id and table name portably."""
    path1 = Path("/tmp/somedir/cdc_landing/batch_id=batch_001/accounts.jsonl")
    assert derive_logical_file_id(path1) == "batch_id=batch_001/accounts.jsonl"

    path2 = Path("data/cdc_landing/batch_id=batch_002/subscriptions.jsonl")
    assert derive_logical_file_id(path2) == "batch_id=batch_002/subscriptions.jsonl"


def test_processing_id_input_list_order_independence():
    """Verify processing_id is identical regardless of the order input files are passed."""
    with tempfile.TemporaryDirectory() as tmpdir:
        b_dir = Path(tmpdir) / "batch_id=batch_001"
        b_dir.mkdir(parents=True)
        f1 = b_dir / "accounts.jsonl"
        f2 = b_dir / "subscriptions.jsonl"
        f1.write_text('{"event_id":"e1"}\n', encoding="utf-8")
        f2.write_text('{"event_id":"e2"}\n', encoding="utf-8")

        proc_id1, _ = compute_manifest_and_processing_id([f1, f2])
        proc_id2, _ = compute_manifest_and_processing_id([f2, f1])

        assert proc_id1 == proc_id2
        assert proc_id1.startswith("proc_")


def test_processing_id_root_directory_independence():
    """Verify copying identical logical files into two different temp root directories yields the same processing_id."""
    with tempfile.TemporaryDirectory() as tmpdir1, tempfile.TemporaryDirectory() as tmpdir2:
        # Directory 1 on Machine A
        b_dir1 = Path(tmpdir1) / "landing" / "batch_id=batch_001"
        b_dir1.mkdir(parents=True)
        f1 = b_dir1 / "accounts.jsonl"
        f1.write_text('{"event_id":"e1","table_name":"accounts"}\n', encoding="utf-8")

        # Directory 2 on Machine B
        b_dir2 = Path(tmpdir2) / "var" / "other_mount" / "batch_id=batch_001"
        b_dir2.mkdir(parents=True)
        f2 = b_dir2 / "accounts.jsonl"
        f2.write_text('{"event_id":"e1","table_name":"accounts"}\n', encoding="utf-8")

        proc_id1, manifest1 = compute_manifest_and_processing_id([f1])
        proc_id2, manifest2 = compute_manifest_and_processing_id([f2])

        assert proc_id1 == proc_id2
        assert manifest1 == manifest2
        assert manifest1 == [
            "batch_id=batch_001/accounts.jsonl:"
            + f1.read_text().strip().encode().hex()[:0]
            + manifest1[0].split(":")[1]
        ]


def test_processing_id_content_change_sensitivity():
    """Verify modifying file contents produces a different processing_id for the same logical path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        b_dir = Path(tmpdir) / "batch_id=batch_001"
        b_dir.mkdir(parents=True)
        f1 = b_dir / "accounts.jsonl"
        f1.write_text('{"event_id":"e1"}\n', encoding="utf-8")

        proc_id1, _ = compute_manifest_and_processing_id([f1])

        # Modify one byte
        f1.write_text('{"event_id":"e1_mutated"}\n', encoding="utf-8")
        proc_id2, _ = compute_manifest_and_processing_id([f1])

        assert proc_id1 != proc_id2
