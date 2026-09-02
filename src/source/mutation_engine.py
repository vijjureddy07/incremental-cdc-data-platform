"""Snapshot Mutation Engine.

Maintains an authoritative local/in-memory representation of the source database state.
Applies valid CDC events (INSERT, UPDATE, DELETE) using sequence-aware reconciliation
to produce golden expected states after any batch of changes.
"""

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

from src.cdc.models import CDCEvent, CDCOperation
from src.cdc.validator import CDCValidator
from src.source.schemas import TABLE_PRIMARY_KEYS, TABLE_SCHEMAS_MAP


@dataclass
class MutationResult:
    """Summary of operations performed during CDC batch application."""

    applied_count: int = 0
    duplicate_count: int = 0
    stale_sequence_count: int = 0
    invalid_count: int = 0
    errors: list[str] = field(default_factory=list)


class SourceMutationEngine:
    """In-memory transactional state engine and golden reconciliation oracle."""

    def __init__(self, initial_state: dict[str, list[dict[str, Any]]] | None = None) -> None:
        # Structure: {table_name: {primary_key_value: record_dict}}
        self._tables: dict[str, dict[str, dict[str, Any]]] = {
            table: {} for table in TABLE_SCHEMAS_MAP.keys()
        }
        # Tracks the highest seen sequence_number per entity: {table_name: {pk_val: max_sequence}}
        self._entity_sequences: dict[str, dict[str, int]] = {
            table: {} for table in TABLE_SCHEMAS_MAP.keys()
        }
        # Tracks seen event_ids for deduplication
        self._seen_event_ids: set[str] = set()

        if initial_state:
            self.load_snapshot(initial_state)

    def load_snapshot(self, tables_dict: dict[str, list[dict[str, Any]]]) -> None:
        """Initialize or replace current state from a snapshot dictionary."""
        for table_name, records in tables_dict.items():
            if table_name not in self._tables:
                continue
            pk_col = TABLE_PRIMARY_KEYS[table_name]
            self._tables[table_name] = {}
            self._entity_sequences[table_name] = {}
            for rec in records:
                pk_val = str(rec[pk_col])
                self._tables[table_name][pk_val] = deepcopy(rec)
                self._entity_sequences[table_name][pk_val] = 0

    def apply_event(self, event: CDCEvent | dict[str, Any], enforce_sequence: bool = True) -> bool:
        """Apply a single CDC event to the state.

        Returns True if the state was mutated, False otherwise.
        """
        cdc_event = event if isinstance(event, CDCEvent) else CDCEvent.from_dict(event)

        # 1. Structural Validation
        val_res = CDCValidator.validate(cdc_event)
        if not val_res.is_valid:
            return False

        # 2. Duplicate event check (exact event_id deduplication)
        if cdc_event.event_id in self._seen_event_ids:
            return False

        table_name = cdc_event.table_name
        pk_col = TABLE_PRIMARY_KEYS[table_name]
        pk_val = str(cdc_event.business_key[pk_col])
        current_max_seq = self._entity_sequences[table_name].get(pk_val, 0)

        # 3. Strict Sequence Monotonicity enforcement
        # If sequence_number <= current_max_seq, event is non-monotonic / stale
        if enforce_sequence and cdc_event.sequence_number <= current_max_seq:
            self._seen_event_ids.add(cdc_event.event_id)
            return False

        # 4. Mutation Application
        if cdc_event.operation == CDCOperation.INSERT.value:
            record_payload = deepcopy(cdc_event.payload or {})
            self._tables[table_name][pk_val] = record_payload

        elif cdc_event.operation == CDCOperation.UPDATE.value:
            record_payload = deepcopy(cdc_event.payload or {})
            # Ensure updated_at is properly recorded
            if "updated_at" not in record_payload:
                record_payload["updated_at"] = cdc_event.event_timestamp
            self._tables[table_name][pk_val] = record_payload

        elif cdc_event.operation == CDCOperation.DELETE.value:
            if pk_val in self._tables[table_name]:
                del self._tables[table_name][pk_val]

        # Update sequence watermark and mark event_id seen
        self._entity_sequences[table_name][pk_val] = max(current_max_seq, cdc_event.sequence_number)
        self._seen_event_ids.add(cdc_event.event_id)
        return True

    def apply_batch(
        self,
        events: list[CDCEvent | dict[str, Any]],
        sort_by_sequence: bool = True,
    ) -> MutationResult:
        """Apply a batch of events with optional pre-sorting by authoritative sequence number.

        Sorting guarantees deterministic state convergence even when files arrive out of order.
        When sort_by_sequence is False, events are applied in exact arrival order.
        """
        result = MutationResult()

        event_objs = [ev if isinstance(ev, CDCEvent) else CDCEvent.from_dict(ev) for ev in events]

        if sort_by_sequence:
            # Sort authoritatively by table_name, business_key string, sequence_number
            # Do NOT sort by event_timestamp as tiebreaker
            event_objs.sort(
                key=lambda ev: (
                    ev.table_name,
                    str(sorted(ev.business_key.items())),
                    ev.sequence_number,
                )
            )

        for ev in event_objs:
            # Validate first
            val = CDCValidator.validate(ev)
            if not val.is_valid:
                result.invalid_count += 1
                result.errors.extend(val.errors)
                continue

            if ev.event_id in self._seen_event_ids:
                result.duplicate_count += 1
                continue

            table_name = ev.table_name
            pk_col = TABLE_PRIMARY_KEYS[table_name]
            pk_val = str(ev.business_key[pk_col])
            current_max_seq = self._entity_sequences[table_name].get(pk_val, 0)

            # Strict Monotonicity: <= is rejected as stale
            if ev.sequence_number <= current_max_seq:
                result.stale_sequence_count += 1
                self._seen_event_ids.add(ev.event_id)
                continue

            # Apply
            applied = self.apply_event(ev, enforce_sequence=True)
            if applied:
                result.applied_count += 1

        return result

    def get_table_records(self, table_name: str) -> list[dict[str, Any]]:
        """Return deep copy list of records currently in the specified table."""
        if table_name not in self._tables:
            raise KeyError(f"Table '{table_name}' does not exist.")
        return [deepcopy(r) for r in self._tables[table_name].values()]

    def get_record(self, table_name: str, pk_val: str) -> dict[str, Any] | None:
        """Retrieve a deep copy of a specific record by primary key."""
        rec = self._tables.get(table_name, {}).get(str(pk_val))
        return deepcopy(rec) if rec is not None else None

    def get_snapshot_state(self) -> dict[str, list[dict[str, Any]]]:
        """Return an actual deep copy of the current state of all tables."""
        return {
            table: [deepcopy(r) for r in records.values()]
            for table, records in self._tables.items()
        }

    def get_table_row_count(self, table_name: str) -> int:
        """Return count of active records in a table."""
        return len(self._tables.get(table_name, {}))
