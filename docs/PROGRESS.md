# Project Progress Tracker

## Incremental & CDC Data Platform

| Module | Title | Status | Learning Status | Tests | Artifacts / Output |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Module 1** | **Source System + Deterministic CDC Event Simulator** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 38 passed | Synthetic snapshot (Parquet), Source-derived CDC stream generator, Validator, Mutation Oracle |
| **Module 2** | **Transactional Watermark Incremental Ingestion + Control Tables** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 30 passed | SQLite control store, Composite cursor extractor, Deterministic landing writer, Pipeline orchestrator |
| **Module 3** | **CDC Normalization, Ordering, Deduplication & Quarantine** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 49 passed | PySpark normalization engine, Dead-letter quarantine store, Deterministic partition writer, Pipeline orchestrator |
| **Module 4** | **Delta MERGE, Delete Propagation, Idempotent Replay & Recovery** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 39 passed | Delta current-state tables, 2-phase event application ledger, ACID Delta MERGE, Stale resurrection protection, Disaster recovery, Replay idempotency |
| **Module 5** | **Databricks Lakeflow AUTO CDC — Managed CDC, Initial Hydration & SCD Type 2** | **COMPLETE / CLOUD VALIDATION PENDING** | `NOT STUDIED / PENDING` | 24 passed (180 total) | Declarative streaming tables, Streaming temporary views, Aligned source schemas, Initial hydration flows (`once=True`), Continuous AUTO CDC flows, SCD Type 2 history tracking, SQL reference |
| **Module 6** | Delta Change Data Feed, CI/CD & Final Hardening | *PLANNED* | `NOT STUDIED / PENDING` | — | CDF downstream consumers, end-to-end reconciliation |

---

## Module 5 Verified Capabilities & Test Summary
- **Lakeflow API**: Modern PySpark Declarative Pipeline API (`from pyspark import pipelines as dp`, `dp.create_streaming_table`, `dp.create_auto_cdc_flow`, `dp.temporary_view`).
- **Test Framework**: Pytest (**180 passed unit & integration tests**; 38 Module 1 + 30 Module 2 + 49 Module 3 + 39 Module 4 + 24 Module 5).
- **Linter & Formatter**: Ruff (100% clean, 0 warnings/errors).
- **Target Streaming Tables (5)**:
  - `accounts_current` (SCD Type 1)
  - `subscriptions_current` (SCD Type 1)
  - `subscriptions_history` (SCD Type 2 Historical Audit)
  - `invoices_current` (SCD Type 1)
  - `payments_current` (SCD Type 1)
- **Source Streaming Temporary Views (8)**:
  - 4 snapshot hydration views (`@dp.temporary_view`, `cloudFiles` Parquet, `includeExistingFiles=true`, `sequence_number=0`, typed NULL lineage placeholders).
  - 4 continuous CDC views (`@dp.temporary_view`, `cloudFiles` JSON, `inferColumnTypes=true`, `includeExistingFiles=true`, explicit domain type casts).
- **Unified Source Schema Contract**:
  - Exact schema and data type equality between snapshot and CDC views across all 4 tables, guaranteeing compliance with Databricks AUTO CDC multi-flow target constraints.
- **Multi-Flow Architecture (10 flows across 5 targets)**:
  - 5 initial snapshot hydration flows (`once=True`, deterministic `sequence_number = 0`).
  - 5 continuous CDC ingestion flows with native delete application (`apply_as_deletes = expr("operation = 'DELETE'")`).
- **SCD Type 2 History Tracking**:
  - `subscriptions_history` tracks business column modifications across `account_id`, `plan_name`, `billing_cycle`, `monthly_amount`, `status`, `start_date`, `end_date`.
  - Lakeflow-managed interval columns `__START_AT` and `__END_AT`.
- **Delete Propagation & Tombstones**:
  - Time-bounded managed tombstones (`pipelines.cdc.tombstoneGCThresholdInSeconds = 604800`) set on target streaming tables protect against out-of-order delete resurrection.
- **Top-Level Graph Registration**:
  - Automatic pipeline declaration on script evaluation with zero dependency on broken global inspections.
- **SQL Reference Implementation**:
  - Declarative SQL reference ([auto_cdc_reference.sql](../databricks/lakeflow/sql/auto_cdc_reference.sql)) using modern syntax (`FROM stream(...)`, `APPLY AS DELETE WHEN`, `COLUMNS * EXCEPT`, `TRACK HISTORY ON`).
- **API Guard**:
  - Zero deprecated `apply_changes`, `APPLY CHANGES INTO`, or `@dlt.view` syntax across all Module 5 source and SQL reference files.
- **Validation Status**:
  - **Local Contract Validation**: `PASSED` (AST syntax compilation, lightweight registration harness, projection tests, schema alignment, configuration contracts).
  - **Cloud Validation**: `NOT EXECUTED / PENDING` (Live execution requires active Databricks workspace).
