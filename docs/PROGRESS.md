# Project Progress Tracker

## Incremental & CDC Data Platform

| Module | Title | Status | Learning Status | Tests | Artifacts / Output |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Module 1** | **Source System + Deterministic CDC Event Simulator** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 38 passed | Synthetic snapshot (Parquet), Source-derived CDC stream generator, Validator, Mutation Oracle |
| **Module 2** | **Transactional Watermark Incremental Ingestion + Control Tables** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 30 passed | SQLite control store, Composite cursor extractor, Deterministic landing writer, Pipeline orchestrator |
| **Module 3** | **CDC Normalization, Ordering, Deduplication & Quarantine** | **COMPLETED / HARDENED** | `NOT STUDIED / PENDING` | 47 passed (115 total) | PySpark normalization engine, Dead-letter quarantine store, Deterministic partition writer, Pipeline orchestrator |
| **Module 4** | Delta MERGE, Deletes, Replay & Recovery | *PLANNED* | `PENDING` | — | Silver merge, hard & soft delete propagation, disaster recovery |
| **Module 5** | Databricks Lakeflow AUTO CDC | *PLANNED* | `PENDING` | — | Lakeflow declarative pipelines, AUTO CDC ingestion |
| **Module 6** | Delta Change Data Feed, CI/CD & Final Hardening | *PLANNED* | `PENDING` | — | CDF downstream consumers, end-to-end reconciliation |

---

## Module 3 Hardened Verification Summary
- **Python Version**: 3.11+ (Validated on Python 3.13)
- **PySpark Constraint**: `pyspark>=3.5.0,<3.6.0` (Pinned to PySpark 3.5.3)
- **Test Framework**: Pytest (**115 passed unit & integration tests**; 38 Module 1 + 30 Module 2 + 47 Module 3)
- **Linter & Formatter**: Ruff (100% clean, 0 warnings/errors)
- **Portable Logical Input Manifest**: `processing_id` computed from content-addressed SHA-256 byte digests and logical file IDs (`batch_id=<batch-id>/<filename>`), guaranteeing identical IDs across different machines and directory roots.
- **Ingestion-Context Late Arrivals**: Removed name heuristics; classified strictly via timestamp comparisons across deterministically ordered ingestion batches (`event_timestamp < prior_batches_max_event_timestamp`).
- **Deterministic Exact-Duplicate Winner**: Exact duplicate deliveries select a deterministic representative via `batch_id ASC`, `ingestion_batch_id ASC`, `source_file ASC`, and `ingestion_order ASC`.
- **Decoupled Execution Timestamps**: Volatile run timestamps excluded from logical `accepted.jsonl` and `quarantine.jsonl`, achieving byte-for-byte identical replay files.
- **Strict Primary Key Presence**: Validates required primary key column presence in present payload and before-image dictionaries.
- **Verified Ingestion Batches**:
  - **Batch 1** (8 raw events: 4 inserts, 4 updates) -> **8 accepted, 0 quarantined**.
  - **Batch 2** (5 raw events: 1 delete, 1 duplicate, 2 out-of-order, 1 late) -> **4 accepted, 1 exact duplicate dropped, 0 quarantined**.
  - **Batch 3** (7 fixture events + 1 malformed non-JSON line) -> **0 accepted, 8 quarantined**.
  - **Combined Lifecycle Reconciliation** (20 raw events total) -> **12 accepted + 1 duplicate dropped + 7 quarantined = 20 raw events seen**.
- **Verified Hardening Proofs**:
  - Legitimately late events without "late" in name are correctly tagged late.
  - Non-late events containing "late" in name are not marked late.
  - Frozen Module 1 `evt_late_sub_0002` correctly tagged late via ingestion context.
  - Identical `processing_id` across distinct temporary directory roots.
  - Content modification sensitivity (changed bytes alter `processing_id`).
  - Reversed file-list order produces identical accepted and quarantine logical records.
  - Reversed duplicate-conflict input produces identical quarantine ordering.
  - Byte-level identical output file replay proof.
