# Project Progress Tracker

## Incremental & CDC Data Platform

| Module | Title | Status | Learning Status | Tests | Artifacts / Output |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Module 1** | **Source System + Deterministic CDC Event Simulator** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 38 passed | Synthetic snapshot (Parquet), Source-derived CDC stream generator, Validator, Mutation Oracle |
| **Module 2** | **Transactional Watermark Incremental Ingestion + Control Tables** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 30 passed | SQLite control store, Composite cursor extractor, Deterministic landing writer, Pipeline orchestrator |
| **Module 3** | **CDC Normalization, Ordering, Deduplication & Quarantine** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 49 passed | PySpark normalization engine, Dead-letter quarantine store, Deterministic partition writer, Pipeline orchestrator |
| **Module 4** | **Delta MERGE, Delete Propagation, Idempotent Replay & Recovery** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 39 passed (156 total) | Delta current-state tables, 2-phase event application ledger, ACID Delta MERGE, Stale resurrection protection, Disaster recovery, Replay idempotency |
| **Module 5** | Databricks Lakeflow AUTO CDC | *PLANNED* | `NOT STUDIED / PENDING` | — | Lakeflow declarative pipelines, AUTO CDC ingestion |
| **Module 6** | Delta Change Data Feed, CI/CD & Final Hardening | *PLANNED* | `NOT STUDIED / PENDING` | — | CDF downstream consumers, end-to-end reconciliation |

---

## Module 4 Verified Capabilities & Test Summary
- **Python Version**: 3.11+ (Validated on Python 3.13)
- **PySpark & Delta Lake**: `pyspark>=3.5.0,<3.6.0`, `delta-spark>=3.3.0,<3.4.0` (Pinned to PySpark 3.5.3 + Delta Lake 3.3.3)
- **Test Framework**: Pytest (**156 passed unit & integration tests**; 38 Module 1 + 30 Module 2 + 49 Module 3 + 39 Module 4)
- **Linter & Formatter**: Ruff (100% clean, 0 warnings/errors)
- **Current-State Target Tables**:
  - `accounts` (PK: `account_id`, initial 40 rows)
  - `subscriptions` (PK: `subscription_id`, initial 60 rows)
  - `invoices` (PK: `invoice_id`, initial 120 rows)
  - `payments` (PK: `payment_id`, initial 90 rows)
- **Operational Target Metadata**: `_last_sequence_number`, `_last_event_id`, `_last_operation`, `_last_event_fingerprint`, `_last_source_commit_timestamp`, `_last_processing_id`, `_is_deleted`, `_deleted_at`.
- **Processing Provenance vs Run Identity**:
  - `processing_id`: Deterministic logical Module 3 input-set identifier (persisted in Delta targets and ledger).
  - `run_id`: Ephemeral execution attempt identifier.
- **Two-Phase Delta Applied Ledger & Recovery Ownership**:
  - Located at `data/delta/control/event_apply_ledger`.
  - Enforces `PENDING` $\rightarrow$ `APPLIED` two-phase transactional groups across separate ACID Delta table operations.
  - PENDING recovery ownership: Interrupted `PENDING` events require matching `(event_id, event_fingerprint, processing_id)`; foreign processing sets cannot hijack unfinished intent.
  - Cross-processing event classification (`FRESH`, `RECOVERY_PENDING`, `EXACT_REPLAY_APPLIED`, `STALE_SKIPPED`).
  - Strict conflict detection (`AppliedEventConflictError`, `AppliedSequenceConflictError`, `PendingRecoveryError`).
- **Mutation & Deletion Policies**:
  - **HARD Delete**: `whenMatchedDelete()` physically removes the record; ledger retains applied sequence history forever to prevent stale resurrection.
  - **SOFT Delete**: `whenMatchedUpdate` sets `_is_deleted = True` and tombstone timestamp; subsequent updates reset `_is_deleted = False`.
  - **Sequence Waves**: Actionable mutations grouped by `(table_name, sequence_number)` and executed in deterministic ascending order with intra-wave primary key ambiguity protection.
- **Crash Recovery & Replay Invariants**:
  - Crash after writing `PENDING` $\rightarrow$ retried safely under original `processing_id` with idempotent target mutation and ledger transition to `APPLIED`.
  - Crash after target mutation before marking `APPLIED` $\rightarrow$ retried idempotently with zero duplicate rows and ledger transition to `APPLIED`.
  - Unresolved `PENDING` events block foreign processing runs.
  - Replaying exact applied events results in zero target mutations and zero Delta version increments.
- **Delta Lake Time-Travel**:
  - Point-in-time historical query via `versionAsOf` preserves exact initial snapshot state at version 0 even after subsequent `UPDATE` and `DELETE` MERGE commits.
- **End-to-End Oracle Reconciliation**:
  - Full field-by-field, row-by-row reconciliation against `SourceMutationEngine`:
    - `accounts`: 41 rows (100% matched)
    - `subscriptions`: 61 rows (100% matched)
    - `invoices`: 121 rows (100% matched)
    - `payments`: 90 rows (100% matched)
