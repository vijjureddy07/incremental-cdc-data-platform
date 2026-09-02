"""Watermark Incremental Ingestion Module."""

from src.watermark.control_store import SQLiteWatermarkControlStore
from src.watermark.landing import (
    generate_deterministic_batch_id,
    get_batch_landing_dir,
    read_watermark_batch_jsonl,
    write_watermark_batch_jsonl,
)
from src.watermark.models import (
    CompositeWatermark,
    ExtractionResult,
    WatermarkCommitError,
    WatermarkConcurrencyError,
    WatermarkError,
    WatermarkExtractionError,
    WatermarkRunAudit,
    WatermarkRunStatus,
    WatermarkState,
)
from src.watermark.pipeline import WatermarkPipeline
from src.watermark.source_adapter import InMemorySourceAdapter

__all__ = [
    "CompositeWatermark",
    "ExtractionResult",
    "InMemorySourceAdapter",
    "SQLiteWatermarkControlStore",
    "WatermarkCommitError",
    "WatermarkConcurrencyError",
    "WatermarkError",
    "WatermarkExtractionError",
    "WatermarkPipeline",
    "WatermarkRunAudit",
    "WatermarkRunStatus",
    "WatermarkState",
    "generate_deterministic_batch_id",
    "get_batch_landing_dir",
    "read_watermark_batch_jsonl",
    "write_watermark_batch_jsonl",
]
