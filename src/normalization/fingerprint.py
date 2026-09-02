"""Deterministic canonical key formatting and SHA-256 event fingerprinting utilities."""

import hashlib
import json
from typing import Any


def canonicalize_business_key(business_key: dict[str, Any] | str | None) -> str:
    """Produce a deterministic, whitespace-free canonical JSON string for a business key dictionary."""
    if business_key is None:
        return "{}"
    if isinstance(business_key, str):
        try:
            parsed = json.loads(business_key)
            if isinstance(parsed, dict):
                sorted_dict = {str(k): str(v) for k, v in sorted(parsed.items())}
                return json.dumps(sorted_dict, sort_keys=True, separators=(",", ":"))
        except Exception:
            return business_key
        return business_key

    if isinstance(business_key, dict):
        sorted_dict = {str(k): str(v) for k, v in sorted(business_key.items())}
        return json.dumps(sorted_dict, sort_keys=True, separators=(",", ":"))

    return str(business_key)


def canonicalize_payload(payload: dict[str, Any] | str | None) -> str:
    """Produce a deterministic canonical string representation for payload dictionaries."""
    if payload is None:
        return ""
    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
            if isinstance(parsed, dict):
                return json.dumps(parsed, sort_keys=True, separators=(",", ":"))
        except Exception:
            return payload
        return payload

    if isinstance(payload, dict):
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    return str(payload)


def compute_entity_sequence_key(table_name: str, business_key_canonical: str) -> str:
    """Compute the scoped entity namespace key combining table name and canonical business key."""
    return f"{table_name}:{business_key_canonical}"


def compute_event_fingerprint(event_data: dict[str, Any]) -> str:
    """Compute a deterministic SHA-256 fingerprint over semantic, replay-stable event fields.

    Excluded metadata fields:
    - ingestion_batch_id, source_file, ingestion_order, normalized_at, Spark partition IDs.
    """
    stable_parts = [
        str(event_data.get("event_id", "")),
        str(event_data.get("table_name", "")),
        str(event_data.get("operation", "")),
        canonicalize_business_key(event_data.get("business_key")),
        str(event_data.get("sequence_number", 0)),
        str(event_data.get("event_timestamp", "")),
        str(event_data.get("source_commit_timestamp", "")),
        canonicalize_payload(event_data.get("payload")),
        canonicalize_payload(event_data.get("before_payload")),
        str(event_data.get("source_system", "")),
    ]
    raw_str = "|".join(stable_parts)
    return hashlib.sha256(raw_str.encode("utf-8")).hexdigest()


def generate_processing_id(input_identifiers: list[str]) -> str:
    """Generate deterministic processing_id from a stable, sorted set of input file paths or hashes."""
    sorted_inputs = sorted({str(x) for x in input_identifiers})
    combined = "\n".join(sorted_inputs)
    digest = hashlib.sha256(combined.encode("utf-8")).hexdigest()[:16]
    return f"proc_{digest}"
