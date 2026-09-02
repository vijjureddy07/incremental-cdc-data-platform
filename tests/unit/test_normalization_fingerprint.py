"""Unit tests for canonical business key formatting and deterministic SHA-256 event fingerprinting."""

from src.normalization.fingerprint import (
    canonicalize_business_key,
    compute_entity_sequence_key,
    compute_event_fingerprint,
    generate_processing_id,
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


def test_generate_processing_id_stability():
    """Verify generate_processing_id produces identical deterministic hashes regardless of input path ordering."""
    paths1 = ["/data/batch_001/accounts.jsonl", "/data/batch_001/subscriptions.jsonl"]
    paths2 = ["/data/batch_001/subscriptions.jsonl", "/data/batch_001/accounts.jsonl"]

    proc_id1 = generate_processing_id(paths1)
    proc_id2 = generate_processing_id(paths2)

    assert proc_id1 == proc_id2
    assert proc_id1.startswith("proc_")
