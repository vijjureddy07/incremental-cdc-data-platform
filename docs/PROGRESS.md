# Project Progress Tracker

## Incremental & CDC Data Platform

| Module | Title | Status | Learning Status | Tests | Artifacts / Output |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Module 1** | **Source System + Deterministic CDC Event Simulator** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 38 passed | Synthetic snapshot (Parquet), Source-derived CDC stream generator, Validator, Mutation Oracle |
| **Module 2** | **Transactional Watermark Incremental Ingestion + Control Tables** | **COMPLETED** | `NOT STUDIED / PENDING` | 30 passed (68 total) | SQLite control store, Composite cursor extractor, Deterministic landing writer, Pipeline orchestrator |
| **Module 3** | CDC Normalization, Ordering, Dedupe & Quarantine | *PLANNED* | `PENDING` | — | Bronze ingestion, sequence ordering, quarantine routing |
| **Module 4** | Delta MERGE, Deletes, Replay & Recovery | *PLANNED* | `PENDING` | — | Silver merge, hard & soft delete propagation, disaster recovery |
| **Module 5** | Databricks Lakeflow AUTO CDC | *PLANNED* | `PENDING` | — | Lakeflow declarative pipelines, AUTO CDC ingestion |
| **Module 6** | Delta Change Data Feed, CI/CD & Final Hardening | *PLANNED* | `PENDING` | — | CDF downstream consumers, end-to-end reconciliation |

---

## Module 2 Hardened Verification Summary
- **Python Version**: 3.11+ (Validated on Python 3.13)
- **PySpark Constraint**: `pyspark>=3.5.0,<3.6.0` (Pinned)
- **Test Framework**: Pytest (**68 passed unit & integration tests**; 38 Module 1 + 30 Module 2)
- **Linter & Formatter**: Ruff (100% clean, 0 warnings/errors)
- **Control Store**: Durable SQLite store (`watermark_state` and `watermark_run_audit`) with explicit SQL transactions (`BEGIN IMMEDIATE` / `COMMIT` / `ROLLBACK`) and optimistic concurrency control (compare-and-swap versioning).
- **Atomic Completion**: `commit_successful_run` executes CAS watermark update AND marks audit `SUCCESS` in a single SQLite transaction with automatic rollback if either fails.
- **Recoverable Window Contract**: Uncompleted / failed extraction attempts persist `expected_version`, `low_watermark`, `high_watermark`, and `batch_id`. Retries reuse the exact frozen HIGH boundary and deterministic `batch_id`, isolating subsequent source mutations to the next cycle.
- **Landed File & Row Count Verification**: Reads back landed `data.jsonl` from disk and validates existence and row count before committing checkpoints.
- **Composite Cursor**: `(updated_at, primary_key)` eliminating timestamp collision data loss across identical timestamp boundaries.
- **Initial Full Load Counts**:
  - Accounts: 40 rows
  - Subscriptions: 60 rows
  - Invoices: 120 rows
  - Payments: 90 rows
- **Incremental Extraction (Batch 1 Mutations)**:
  - Accounts: 2 rows (`ACC-0041`, `ACC-0001`)
  - Subscriptions: 2 rows (`SUB-0061`, `SUB-0001`)
  - Invoices: 2 rows (`INV-0121`, `INV-0001`)
  - Payments: 2 rows (`PAY-0091`, `PAY-0001`)
- **Verified Architectural Edge Cases**:
  - Failed HIGH survives source mutations before retry (retry reuses exact HIGH and `batch_id`).
  - Newer records above recovered HIGH wait for the next cycle.
  - Failure before landing keeps watermark unchanged and recovers window.
  - Abandoned `RUNNING` process-death attempts recovered and superseded cleanly.
  - Explicit control-store transaction rollback on audit failures.
  - Multi-connection optimistic concurrency CAS conflicts rejected (`WatermarkConcurrencyError`).
  - Landing row-count verification failure detects corruption and blocks commit.
  - `NO_DATA` zero-change reruns (0 rows extracted, uncommitted watermarks).
  - Equal timestamp collisions (composite tie-breaker prevents skipping rows).
  - Bounded HIGH isolation (rows added mid-run wait for subsequent cycle).
  - Hard physical delete blind spot (demonstrated that hard deletes return 0 rows).
  - Backdated `updated_at` limitation (demonstrated missed modifications).
  - Restart persistence across database connections.
