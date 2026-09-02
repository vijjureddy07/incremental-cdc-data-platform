"""CDC simulation and change event processing package."""

from src.cdc.generator import CDCScenarioGenerator
from src.cdc.models import CDCEvent, CDCOperation
from src.cdc.serialization import (
    deserialize_event_from_json,
    read_cdc_batch_jsonl,
    serialize_event_to_json,
    write_cdc_batch_jsonl,
)
from src.cdc.validator import CDCValidator, ValidationResult

__all__ = [
    "CDCEvent",
    "CDCOperation",
    "CDCScenarioGenerator",
    "CDCValidator",
    "ValidationResult",
    "deserialize_event_from_json",
    "read_cdc_batch_jsonl",
    "serialize_event_to_json",
    "write_cdc_batch_jsonl",
]
