# Project Progress Tracker

## Incremental & CDC Data Platform

| Module | Title | Status | Learning Status | Tests | Artifacts / Output |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Module 1** | **Source System + Deterministic CDC Event Simulator** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 38 passed | Synthetic snapshot (Parquet), Source-derived CDC stream generator, Validator, Mutation Oracle |
| **Module 2** | **Transactional Watermark Incremental Ingestion + Control Tables** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 30 passed | SQLite control store, Composite cursor extractor, Deterministic landing writer, Pipeline orchestrator |
| **Module 3** | **CDC Normalization, Ordering, Deduplication & Quarantine** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 49 passed | PySpark normalization engine, Dead-letter quarantine store, Deterministic partition writer, Pipeline orchestrator |
| **Module 4** | **Delta MERGE, Delete Propagation, Idempotent Replay & Recovery** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 39 passed | Delta current-state tables, 2-phase event application ledger, ACID Delta MERGE, Stale resurrection protection, Disaster recovery, Replay idempotency |
| **Module 5** | **Databricks Lakeflow AUTO CDC — Managed CDC, Initial Hydration & SCD Type 2** | **COMPLETE / CLOUD VALIDATION PENDING** | `NOT STUDIED / PENDING` | 17 passed (173 total) | Declarative streaming tables, Initial hydration flows (`once=True`), Continuous AUTO CDC flows, SCD Type 2 history tracking, SQL reference |
| **Module 6** | Delta Change Data Feed, CI/CD & Final Hardening | *PLANNED* | `NOT STUDIED / PENDING` | — | CDF downstream consumers, end-to-end reconciliation |

---

## Module 5 Verified Capabilities & Test Summary
- **Lakeflow API**: Modern PySpark Declarative Pipeline API (`from pyspark import pipelines as dp`, `dp.create_streaming_table`, `dp.create_auto_cdc_flow`).
- **Test Framework**: Pytest (**173 passed unit & integration tests**; 38 Module 1 + 30 Module 2 + 49 Module 3 + 39 Module 4 + 17 Module 5).
- **Linter & Formatter**: Ruff (100% clean, 0 warnings/errors).
- **Target Streaming Tables**:
  - `accounts_current` (SCD Type 1)
  - `subscriptions_current` (SCD Type 1)
  - `subscriptions_history` (SCD Type 2 Historical Audit)
  - `invoices_current` (SCD Type 1)
  - `payments_current` (SCD Type 1)
- **Multi-Flow Architecture (10 flows across 5 targets)**:
  - 5 initial snapshot hydration flows (`once=True`, deterministic `sequence_number = 0`).
  - 5 continuous CDC ingestion flows with native delete application (`apply_as_deletes = expr("operation = 'DELETE'")`).
- **SCD Type 2 History Tracking**:
  - `subscriptions_history` tracks business column modifications across `account_id`, `plan_name`, `billing_cycle`, `monthly_amount`, `status`, `start_date`, `end_date`.
  - Lakeflow-managed interval columns `__START_AT` and `__END_AT`.
- **Delete Propagation & Tombstones**:
  - Time-bounded managed tombstones (`pipelines.cdc.tombstoneGCThresholdInSeconds = 604800`) protect against out-of-order delete resurrection.
- **API Guard**:
  - Zero deprecated `apply_changes` or `APPLY CHANGES INTO` syntax across all Module 5 source and SQL reference files.
- **Validation Status**:
  - **Local Validation**: 100% validated via Python AST parsing, lightweight registration harness, projection tests, and configuration contracts.
  - **Cloud Validation**: `NOT CLOUD EXECUTED / DEPLOYMENT READY` (Live execution requires active Databricks workspace).
