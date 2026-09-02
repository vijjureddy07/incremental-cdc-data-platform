# Project Progress Tracker

## Incremental & CDC Data Platform

| Module | Title | Status | Learning Status | Tests | Artifacts / Output |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Module 1** | **Source System + Deterministic CDC Event Simulator** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 38 passed | Synthetic snapshot (Parquet), Source-derived CDC stream generator, Validator, Mutation Oracle |
| **Module 2** | **Transactional Watermark Incremental Ingestion + Control Tables** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 30 passed | SQLite control store, Composite cursor extractor, Deterministic landing writer, Pipeline orchestrator |
| **Module 3** | **CDC Normalization, Ordering, Deduplication & Quarantine** | **COMPLETED** | `NOT STUDIED / PENDING` | 32 passed (100 total) | PySpark normalization engine, Dead-letter quarantine store, Deterministic partition writer, Pipeline orchestrator |
| **Module 4** | Delta MERGE, Deletes, Replay & Recovery | *PLANNED* | `PENDING` | — | Silver merge, hard & soft delete propagation, disaster recovery |
| **Module 5** | Databricks Lakeflow AUTO CDC | *PLANNED* | `PENDING` | — | Lakeflow declarative pipelines, AUTO CDC ingestion |
| **Module 6** | Delta Change Data Feed, CI/CD & Final Hardening | *PLANNED* | `PENDING` | — | CDF downstream consumers, end-to-end reconciliation |

---

## Module 3 Verification Summary
- **Python Version**: 3.11+ (Validated on Python 3.13)
- **PySpark Constraint**: `pyspark>=3.5.0,<3.6.0` (Pinned to PySpark 3.5.3)
- **Test Framework**: Pytest (**100 passed unit & integration tests**; 38 Module 1 + 30 Module 2 + 32 Module 3)
- **Linter & Formatter**: Ruff (100% clean, 0 warnings/errors)
- **Core Processing Engine**: PySpark DataFrames with window functions for exact deduplication, conflicting duplicate event_id quarantine, equal-sequence conflict quarantine, and authoritative entity sequence ordering.
- **Durable Local Storage**:
  - `data/normalized_cdc/processing_id=<id>/accepted.jsonl`
  - `data/quarantine/processing_id=<id>/quarantine.jsonl`
- **Verified Ingestion Batches**:
  - **Batch 1** (8 raw events: 4 inserts, 4 updates) -> **8 accepted, 0 quarantined**.
  - **Batch 2** (5 raw events: 1 delete, 1 duplicate, 2 out-of-order, 1 late) -> **4 accepted, 1 exact duplicate dropped, 0 quarantined**.
  - **Batch 3** (7 fixture events + 1 malformed non-JSON line) -> **0 accepted, 8 quarantined**.
  - **Combined Lifecycle Reconciliation** (20 raw events total) -> **12 accepted + 1 duplicate dropped + 7 quarantined = 20 raw events seen**.
- **Verified Architectural Edge Cases**:
  - Exact duplicate deliveries dropped cleanly (`exact_duplicates_dropped` metric tracked).
  - Conflicting duplicate `event_id` records quarantined under `DUPLICATE_EVENT_CONFLICT`.
  - Out-of-order sequence arrival (seq 102 then 101) normalized strictly to seq 101 then 102.
  - Equal-sequence collisions on the same entity quarantined under `SEQUENCE_CONFLICT`.
  - Independent entities / tables sharing the same sequence number accepted without conflict.
  - Late-arriving historical events preserved and tagged `is_late_arrival = True`.
  - Malformed non-JSON lines isolated into quarantine without failing batch execution.
  - Deterministic `processing_id` and SHA-256 event fingerprints guaranteeing 100% replay determinism.
