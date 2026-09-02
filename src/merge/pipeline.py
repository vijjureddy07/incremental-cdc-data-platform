"""Two-phase transactional Delta Lake MERGE, sequence protection, and recovery pipeline."""

import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from src.merge.event_adapter import (
    extract_processing_id_from_path,
    load_accepted_events_from_file,
)
from src.merge.event_ledger import EventApplyLedger
from src.merge.merge_engine import DeltaMergeEngine
from src.merge.models import (
    AppliedEventConflictError,
    AppliedSequenceConflictError,
    DeletePolicy,
    LedgerStatus,
    MergeAmbiguityError,
    MergeError,
    MergePipelineResult,
    PendingRecoveryError,
)
from src.merge.target_store import DeltaTargetStore
from src.normalization.models import NormalizedCDCEvent
from src.source.schemas import TABLE_PRIMARY_KEYS


class DeltaMergePipeline:
    """Orchestrates two-phase transactional Delta MERGE, replay idempotency, and crash recovery."""

    def __init__(
        self,
        spark: SparkSession,
        target_store: DeltaTargetStore | None = None,
        event_ledger: EventApplyLedger | None = None,
        target_base_dir: str | Path = "data/delta/current",
        ledger_base_dir: str | Path = "data/delta/control/event_apply_ledger",
        delete_policy: DeletePolicy = DeletePolicy.HARD,
    ) -> None:
        self.spark = spark
        self.target_store = target_store or DeltaTargetStore(spark, target_base_dir)
        self.event_ledger = event_ledger or EventApplyLedger(spark, ledger_base_dir)
        self.merge_engine = DeltaMergeEngine(spark, self.target_store)
        self.delete_policy = delete_policy

    def run_from_file(
        self,
        accepted_jsonl_path: str | Path,
        processing_id: str | None = None,
        fail_after_pending_group: int | None = None,
        fail_after_target_group: int | None = None,
    ) -> MergePipelineResult:
        """Execute the pipeline from a Module 3 accepted.jsonl file.

        Derives processing_id from the path (processing_id=<id>/accepted.jsonl) if not explicitly provided.

        Raises:
            FileNotFoundError: If accepted_jsonl_path does not exist.
            ValueError: If processing_id is omitted and cannot be derived from path.
        """
        path = Path(accepted_jsonl_path)
        if not path.exists():
            raise FileNotFoundError(f"Accepted events file not found at: {path}")

        if processing_id and processing_id.strip():
            proc_id = processing_id.strip()
        else:
            derived_id = extract_processing_id_from_path(path)
            if not derived_id:
                raise ValueError(
                    f"Could not derive processing_id from path '{path}'. "
                    f"Provide an explicit processing_id or use a 'processing_id=<id>' directory structure."
                )
            proc_id = derived_id

        events = load_accepted_events_from_file(path)
        return self.run(
            events=events,
            processing_id=proc_id,
            fail_after_pending_group=fail_after_pending_group,
            fail_after_target_group=fail_after_target_group,
        )

    def run(
        self,
        events: list[NormalizedCDCEvent],
        processing_id: str,
        fail_after_pending_group: int | None = None,
        fail_after_target_group: int | None = None,
    ) -> MergePipelineResult:
        """Execute two-phase Delta MERGE across a list of accepted NormalizedCDCEvents.

        Args:
            events: List of canonical accepted normalized CDC events.
            processing_id: Required deterministic identifier of the input processing set.
            fail_after_pending_group: Test hook to inject failure after writing PENDING.
            fail_after_target_group: Test hook to inject failure after target mutation.

        Raises:
            ValueError: If processing_id is empty or not provided.
            PendingRecoveryError: If unresolved PENDING records exist that do not match current processing set.
            AppliedEventConflictError: If an event ID has conflicting fingerprints across runs.
            AppliedSequenceConflictError: If an entity has duplicate equal-sequence numbers across runs.
            MergeAmbiguityError: If multiple events target the same primary key in a single wave.
        """
        if not processing_id or not isinstance(processing_id, str) or not processing_id.strip():
            raise ValueError("A non-empty processing_id is required for direct run() calls.")

        proc_id = processing_id.strip()
        run_id = f"run_merge_{uuid.uuid4().hex[:12]}"

        # Step 1: Check existing ledger for PENDING records (recovery & ownership verification)
        existing_pending = self.event_ledger.get_pending_records()
        incoming_events_by_id = {e.event_id: e for e in events}

        if existing_pending:
            # All existing PENDING records must belong to the current processing_id
            foreign_pending = [p for p in existing_pending if p.processing_id != proc_id]
            if foreign_pending:
                foreign_p = foreign_pending[0]
                raise PendingRecoveryError(
                    f"Cannot adopt PENDING recovery: ledger contains {len(foreign_pending)} PENDING event(s) "
                    f"belonging to processing_id '{foreign_p.processing_id}' (e.g. event_id='{foreign_p.event_id}'), "
                    f"which cannot be adopted by current processing_id '{proc_id}'."
                )

            # Every existing PENDING event_id must exist in the retry input
            pending_ids = {p.event_id for p in existing_pending}
            unresolved = pending_ids - set(incoming_events_by_id.keys())
            if unresolved:
                raise PendingRecoveryError(
                    f"Interrupted processing set '{proc_id}' cannot be recovered: {len(unresolved)} "
                    f"PENDING event(s) missing from retry input (e.g. IDs: {sorted(unresolved)[:3]})."
                )

            # Fingerprints must match for all pending events in retry input
            for p in existing_pending:
                incoming_ev = incoming_events_by_id[p.event_id]
                if p.event_fingerprint != incoming_ev.event_fingerprint:
                    raise AppliedEventConflictError(
                        f"PENDING event '{p.event_id}' has conflicting fingerprint in retry input: "
                        f"ledger='{p.event_fingerprint}' vs incoming='{incoming_ev.event_fingerprint}'."
                    )

        # Step 2: Classify incoming events against ledger history
        applied_max_seqs = self.event_ledger.get_applied_max_sequences()

        fresh_events: list[NormalizedCDCEvent] = []
        recovery_events: list[NormalizedCDCEvent] = []
        replay_events_skipped: list[NormalizedCDCEvent] = []
        stale_events_skipped: list[NormalizedCDCEvent] = []

        for ev in events:
            existing_rec = self.event_ledger.get_ledger_record_by_event_id(ev.event_id)

            if existing_rec:
                if existing_rec.status == LedgerStatus.APPLIED.value:
                    if existing_rec.event_fingerprint == ev.event_fingerprint:
                        replay_events_skipped.append(ev)
                    else:
                        raise AppliedEventConflictError(
                            f"Event ID '{ev.event_id}' has already been APPLIED with fingerprint "
                            f"'{existing_rec.event_fingerprint}', conflicting with incoming fingerprint '{ev.event_fingerprint}'."
                        )
                elif existing_rec.status == LedgerStatus.PENDING.value:
                    if existing_rec.event_fingerprint != ev.event_fingerprint:
                        raise AppliedEventConflictError(
                            f"Event ID '{ev.event_id}' is PENDING with fingerprint "
                            f"'{existing_rec.event_fingerprint}', conflicting with incoming fingerprint '{ev.event_fingerprint}'."
                        )
                    if existing_rec.processing_id != proc_id:
                        raise PendingRecoveryError(
                            f"Event ID '{ev.event_id}' is PENDING under processing_id '{existing_rec.processing_id}', "
                            f"and cannot be recovered by processing_id '{proc_id}'."
                        )
                    recovery_events.append(ev)
            else:
                # Not in ledger: check entity sequence history
                entity_max = applied_max_seqs.get(ev.entity_sequence_key)
                if entity_max is not None:
                    if ev.sequence_number < entity_max:
                        stale_events_skipped.append(ev)
                    elif ev.sequence_number == entity_max:
                        raise AppliedSequenceConflictError(
                            f"Entity '{ev.entity_sequence_key}' already has APPLIED sequence {entity_max}. "
                            f"Incoming event '{ev.event_id}' has equal sequence with a new event ID."
                        )
                    else:
                        fresh_events.append(ev)
                else:
                    fresh_events.append(ev)

        actionable_events = fresh_events + recovery_events

        # If no actionable events, return early with no Delta MERGE mutations
        if not actionable_events:
            pending_count = len(self.event_ledger.get_pending_records())
            status = (
                "SUCCESS_WITH_SKIPS"
                if (replay_events_skipped or stale_events_skipped)
                else "SUCCESS"
            )
            return MergePipelineResult(
                run_id=run_id,
                processing_id=proc_id,
                events_received=len(events),
                fresh_events=len(fresh_events),
                recovered_pending_events=len(recovery_events),
                events_applied=0,
                replay_events_skipped=len(replay_events_skipped),
                stale_events_skipped=len(stale_events_skipped),
                insert_events_applied=0,
                update_events_applied=0,
                delete_events_applied=0,
                groups_completed=0,
                pending_events_remaining=pending_count,
                status=status,
            )

        # Step 3: Group actionable events into deterministic sequence waves
        # Key: (table_name, sequence_number)
        waves_by_key: dict[tuple[str, int], list[NormalizedCDCEvent]] = defaultdict(list)
        for ev in actionable_events:
            waves_by_key[(ev.table_name, ev.sequence_number)].append(ev)

        # Deterministic wave ordering: table_name ASC, sequence_number ASC
        sorted_wave_keys = sorted(waves_by_key.keys(), key=lambda k: (k[0], k[1]))

        # Ambiguity check across each wave
        for (tbl, seq), wave_evs in waves_by_key.items():
            pk = TABLE_PRIMARY_KEYS[tbl]
            seen_pks: set[Any] = set()
            for ev in wave_evs:
                val = str(ev.business_key.get(pk))
                if val in seen_pks:
                    raise MergeAmbiguityError(
                        f"Ambiguous wave: Multiple events target primary key {pk}={val} on table {tbl} at sequence {seq}."
                    )
                seen_pks.add(val)

        # Step 4: Execute Two-Phase group mutations
        total_inserts = 0
        total_updates = 0
        total_deletes = 0
        groups_completed = 0

        for group_idx, key in enumerate(sorted_wave_keys, start=1):
            table_name, _ = key
            group_events = waves_by_key[key]

            # Phase A: Persist PENDING intent in ledger (immutable if already exists)
            self.event_ledger.record_pending_events(group_events, processing_id=proc_id)

            # Failure hook 1: fail after writing PENDING
            if fail_after_pending_group is not None and fail_after_pending_group == group_idx:
                raise MergeError(
                    f"Test failure injected: Simulated crash after writing PENDING for group {group_idx}."
                )

            # Phase B: Apply Target Mutation(s) via Delta MERGE
            ins, upd, dels = self.merge_engine.merge_wave(
                table_name=table_name,
                events=group_events,
                delete_policy=self.delete_policy,
                processing_id=proc_id,
            )
            total_inserts += ins
            total_updates += upd
            total_deletes += dels

            # Failure hook 2: fail after target mutation before APPLIED
            if fail_after_target_group is not None and fail_after_target_group == group_idx:
                raise MergeError(
                    f"Test failure injected: Simulated crash after target mutation for group {group_idx}."
                )

            # Phase C: Mark events as APPLIED in ledger
            event_ids = [e.event_id for e in group_events]
            self.event_ledger.mark_events_applied(event_ids)
            groups_completed += 1

        pending_remaining = len(self.event_ledger.get_pending_records())
        status = (
            "SUCCESS_WITH_SKIPS" if (replay_events_skipped or stale_events_skipped) else "SUCCESS"
        )

        return MergePipelineResult(
            run_id=run_id,
            processing_id=proc_id,
            events_received=len(events),
            fresh_events=len(fresh_events),
            recovered_pending_events=len(recovery_events),
            events_applied=len(actionable_events),
            replay_events_skipped=len(replay_events_skipped),
            stale_events_skipped=len(stale_events_skipped),
            insert_events_applied=total_inserts,
            update_events_applied=total_updates,
            delete_events_applied=total_deletes,
            groups_completed=groups_completed,
            pending_events_remaining=pending_remaining,
            status=status,
        )
