"""Two-phase transactional Delta Lake MERGE, sequence protection, and recovery pipeline."""

import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from src.merge.event_adapter import load_accepted_events_from_file
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
        """Execute the pipeline from a Module 3 accepted.jsonl file."""
        events = load_accepted_events_from_file(accepted_jsonl_path)
        return self.run(
            events=events,
            processing_id=processing_id,
            fail_after_pending_group=fail_after_pending_group,
            fail_after_target_group=fail_after_target_group,
        )

    def run(
        self,
        events: list[NormalizedCDCEvent],
        processing_id: str | None = None,
        fail_after_pending_group: int | None = None,
        fail_after_target_group: int | None = None,
    ) -> MergePipelineResult:
        """Execute two-phase Delta MERGE across a list of accepted NormalizedCDCEvents."""
        run_id = f"run_merge_{uuid.uuid4().hex[:12]}"
        proc_id = processing_id or "proc_delta"

        # Step 1: Check existing ledger for PENDING records (recovery verification)
        existing_pending = self.event_ledger.get_pending_records()
        incoming_event_ids = {e.event_id for e in events}
        incoming_events_by_id = {e.event_id: e for e in events}

        if existing_pending:
            # If there are pending events in the ledger, verify that incoming events cover them
            pending_ids = {p.event_id for p in existing_pending}
            unresolved = pending_ids - incoming_event_ids
            if unresolved:
                raise PendingRecoveryError(
                    f"Cannot process new events: ledger contains {len(unresolved)} unresolved PENDING events "
                    f"(IDs: {sorted(unresolved)[:3]}...) from prior interrupted runs."
                )

            # Also verify fingerprints match for the pending events
            for p in existing_pending:
                if p.event_id in incoming_events_by_id:
                    incoming_fp = incoming_events_by_id[p.event_id].event_fingerprint
                    if p.event_fingerprint != incoming_fp:
                        raise AppliedEventConflictError(
                            f"PENDING event {p.event_id} has conflicting fingerprint in retry input: "
                            f"ledger='{p.event_fingerprint}' vs incoming='{incoming_fp}'"
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
                    if existing_rec.event_fingerprint == ev.event_fingerprint:
                        recovery_events.append(ev)
                    else:
                        raise AppliedEventConflictError(
                            f"Event ID '{ev.event_id}' is PENDING with fingerprint "
                            f"'{existing_rec.event_fingerprint}', conflicting with incoming fingerprint '{ev.event_fingerprint}'."
                        )
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
            status = "SUCCESS_WITH_SKIPS" if (replay_events_skipped or stale_events_skipped) else "SUCCESS"
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

            # Phase A: Persist PENDING intent in ledger
            self.event_ledger.record_pending_events(group_events, processing_id=proc_id)

            # Failure hook 1: fail after writing PENDING
            if fail_after_pending_group is not None and fail_after_pending_group == group_idx:
                raise MergeError(
                    f"Test failure injected: Simulated crash after writing PENDING for group {group_idx}."
                )

            # Phase C: Apply Target Mutation(s) via Delta MERGE
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

            # Phase E: Mark events as APPLIED in ledger
            event_ids = [e.event_id for e in group_events]
            self.event_ledger.mark_events_applied(event_ids)
            groups_completed += 1

        pending_remaining = len(self.event_ledger.get_pending_records())
        status = "SUCCESS_WITH_SKIPS" if (replay_events_skipped or stale_events_skipped) else "SUCCESS"

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
