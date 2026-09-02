# Project Progress Tracker

## Incremental & CDC Data Platform

| Module | Title | Status | Learning Status | Tests | Artifacts / Output |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Module 1** | **Source System + Deterministic CDC Event Simulator** | **COMPLETED** | `NOT STUDIED / PENDING` | 38 passed | Synthetic snapshot (Parquet), Source-derived CDC stream generator, Validator, Mutation Oracle |
| **Module 2** | Watermark Incremental Ingestion + Control Tables | *PLANNED* | `PENDING` | — | Control tables, high-watermark extraction, incremental staging |
| **Module 3** | CDC Normalization, Ordering, Dedupe & Quarantine | *PLANNED* | `PENDING` | — | Bronze ingestion, sequence ordering, quarantine routing |
| **Module 4** | Delta MERGE, Deletes, Replay & Recovery | *PLANNED* | `PENDING` | — | Silver merge, hard & soft delete propagation, disaster recovery |
| **Module 5** | Databricks Lakeflow AUTO CDC | *PLANNED* | `PENDING` | — | Lakeflow declarative pipelines, AUTO CDC ingestion |
| **Module 6** | Delta Change Data Feed, CI/CD & Final Hardening | *PLANNED* | `PENDING` | — | CDF downstream consumers, end-to-end reconciliation |

---

## Module 1 Verification Summary
- **Python Version**: 3.11+ (Validated on Python 3.13)
- **PySpark**: `pyspark>=3.5.0,<3.6.0` (StructType schema contracts pinned to PySpark 3.5.x)
- **PyArrow**: `pyarrow>=14.0.0` (Deterministic local Parquet generation without JVM requirement)
- **Test Framework**: Pytest (**38 passed unit & integration tests**)
- **Linter & Formatter**: Ruff (100% clean, 0 warnings/errors)
- **Packaging**: Setuptools `src.*` discovery verified with isolated wheel build and external environment import smoke test.
- **Source Snapshot Counts**:
  - Accounts: 40
  - Subscriptions: 60
  - Invoices: 120
  - Payments: 90
- **CDC Change Scenarios**:
  - Batch 1: 4 Inserts + 4 Updates across all tables (`before_payload` derived from actual source snapshot; untouched fields preserved; `updated_at` advanced)
  - Batch 2: Delete (`PAY-0002` before-image derived from actual source snapshot), Exact Duplicate, Out-of-Order arrival (Seq 102 before 101 with consistent source history, tested with and without pre-sorting), Late arrival (past timestamp)
  - Batch 3: Quarantine fixtures (missing PK, missing sequence number, negative sequence, invalid operation, missing delete before-image)
- **Reconciliation Oracle**: Golden in-memory `SourceMutationEngine` with strict sequence monotonicity (`sequence <= current_max` rejected as stale), deep-copy state protection, and zero reliance on `event_timestamp` for tiebreaking.
