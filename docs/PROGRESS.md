# Project Progress Tracker

## Incremental & CDC Data Platform

| Module | Title | Status | Learning Status | Tests | Artifacts / Output |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Module 1** | **Source System + Deterministic CDC Event Simulator** | **COMPLETED** | `NOT STUDIED / PENDING` | 30 passed | Synthetic snapshot (Parquet), CDC change stream generator, Validator, Mutation Engine |
| **Module 2** | Watermark Incremental Ingestion + Control Tables | *PLANNED* | `PENDING` | — | Control tables, high-watermark extraction, incremental staging |
| **Module 3** | CDC Normalization, Ordering, Dedupe & Quarantine | *PLANNED* | `PENDING` | — | Bronze ingestion, sequence ordering, quarantine routing |
| **Module 4** | Delta MERGE, Deletes, Replay & Recovery | *PLANNED* | `PENDING` | — | Silver merge, hard & soft delete propagation, disaster recovery |
| **Module 5** | Databricks Lakeflow AUTO CDC | *PLANNED* | `PENDING` | — | Lakeflow declarative pipelines, AUTO CDC ingestion |
| **Module 6** | Delta Change Data Feed, CI/CD & Final Hardening | *PLANNED* | `PENDING` | — | CDF downstream consumers, end-to-end reconciliation |

---

## Module 1 Verification Summary
- **Python Version**: 3.11+ (Validated on Python 3.13)
- **PySpark**: 3.5.3 (StructType schema definitions & contracts)
- **PyArrow**: 25.0.1 (Deterministic local Parquet generation without JVM requirement)
- **Test Framework**: Pytest (30 comprehensive unit & integration tests)
- **Linter & Formatter**: Ruff (100% clean, 0 warnings/errors)
- **Source Snapshot Counts**:
  - Accounts: 40
  - Subscriptions: 60
  - Invoices: 120
  - Payments: 90
- **CDC Change Scenarios**:
  - Batch 1: 4 Inserts + 4 Updates across all tables (`updated_at` advancement)
  - Batch 2: Delete, Exact Duplicate, Out-of-Order arrival (Seq 102 before 101), Late arrival (past timestamp)
  - Batch 3: Quarantine fixtures (missing PK, invalid operation, malformed payload, negative sequence)
- **Reconciliation Oracle**: Golden in-memory `SourceMutationEngine` with sequence-aware state convergence.
