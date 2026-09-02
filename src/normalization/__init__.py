"""CDC normalization, authoritative sequence ordering, deduplication, and quarantine package."""

from src.normalization.fingerprint import (
    canonicalize_business_key,
    canonicalize_payload,
    compute_entity_sequence_key,
    compute_event_fingerprint,
    compute_manifest_and_processing_id,
    derive_logical_file_id,
    generate_processing_id,
)
from src.normalization.models import (
    NormalizationAuditMetrics,
    NormalizedCDCEvent,
    QuarantinedEvent,
    QuarantineReasonCode,
)
from src.normalization.pipeline import CDCNormalizationPipeline
from src.normalization.processor import SparkCDCNormalizationProcessor
from src.normalization.reader import read_raw_cdc_files
from src.normalization.schema import NORMALIZED_CDC_SPARK_SCHEMA, RAW_CDC_SPARK_SCHEMA
from src.normalization.validator import TABLE_PRIMARY_KEYS, validate_raw_cdc_record
from src.normalization.writer import (
    read_normalized_accepted_jsonl,
    read_quarantine_jsonl,
    write_normalized_accepted_jsonl,
    write_quarantine_jsonl,
)

__all__ = [
    "CDCNormalizationPipeline",
    "NORMALIZED_CDC_SPARK_SCHEMA",
    "NormalizationAuditMetrics",
    "NormalizedCDCEvent",
    "QuarantineReasonCode",
    "QuarantinedEvent",
    "RAW_CDC_SPARK_SCHEMA",
    "SparkCDCNormalizationProcessor",
    "TABLE_PRIMARY_KEYS",
    "canonicalize_business_key",
    "canonicalize_payload",
    "compute_entity_sequence_key",
    "compute_event_fingerprint",
    "compute_manifest_and_processing_id",
    "derive_logical_file_id",
    "generate_processing_id",
    "read_normalized_accepted_jsonl",
    "read_quarantine_jsonl",
    "read_raw_cdc_files",
    "validate_raw_cdc_record",
    "write_normalized_accepted_jsonl",
    "write_quarantine_jsonl",
]
