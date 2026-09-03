# Project Progress Tracker

## Incremental & CDC Data Platform

| Module | Title | Status | Learning Status | Tests | Artifacts / Output |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Module 1** | **Source System + Deterministic CDC Event Simulator** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 38 passed | Synthetic snapshot (Parquet), Source-derived CDC stream generator, Validator, Mutation Oracle |
| **Module 2** | **Transactional Watermark Incremental Ingestion + Control Tables** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 30 passed | SQLite control store, Composite cursor extractor, Deterministic landing writer, Pipeline orchestrator |
| **Module 3** | **CDC Normalization, Ordering, Deduplication & Quarantine** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 49 passed | PySpark normalization engine, Dead-letter quarantine store, Deterministic partition writer, Pipeline orchestrator |
| **Module 4** | **Delta MERGE, Delete Propagation, Idempotent Replay & Recovery** | **FROZEN / COMPLETE** | `NOT STUDIED / PENDING` | 39 passed | Delta current-state tables, 2-phase event application ledger, ACID Delta MERGE, Stale resurrection protection, Disaster recovery, Replay idempotency |
| **Module 5** | **Databricks Lakeflow AUTO CDC — Managed CDC, Initial Hydration & SCD Type 2** | **FROZEN / COMPLETE / CLOUD VALIDATION PENDING** | `NOT STUDIED / PENDING` | 24 passed | Declarative streaming tables, Streaming temporary views, Aligned source schemas, Initial hydration flows (`once=True`), Continuous AUTO CDC flows, SCD Type 2 history tracking, SQL reference |
| **Module 6** | **Delta Change Data Feed, Downstream Recovery, CI/CD & Final Hardening** | **COMPLETE / CLOUD DEPLOYMENT PENDING** | `NOT STUDIED / PENDING` | 26 passed (206 total) | Bounded Delta CDF reader, SQLite downstream consumer state store, Permanent Delta archive, Deterministic SHA-256 `_change_id`, Idempotent MERGE, GitHub Actions CI, Declarative Automation Bundles, Secretless OIDC deploy workflow |

**Project Status**: `IMPLEMENTATION COMPLETE`

---

## Module 6 Verified Capabilities & Test Summary
- **Delta CDF Reader**:
  - Legacy Delta CDF enabled via `ALTER TABLE delta.<path> SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`.
  - Captures exact enabling commit version as `cdf_start_version`.
  - Bounded version range reads `[start_version, end_version]` with canonical metadata validation (`_change_type`, `_commit_version`, `_commit_timestamp`).
- **Downstream Consumer State Store (`data/control/cdf_consumer.db`)**:
  - Dedicated SQLite store tracking `source_table`, `source_path`, `cdf_start_version`, and `last_processed_version`.
  - Initial `last_processed_version = cdf_start_version - 1`.
  - Strict multi-table checkpoint isolation (accounts, subscriptions, invoices, payments).
- **Permanent Downstream Delta Archive (`data/delta/downstream/cdf_archive/<table_name>`)**:
  - Stores all business fields, Module 4 operational metadata, and canonical CDF columns.
  - Adds `_source_table` and deterministic SHA-256 `_change_id` derived from table name, commit version, change type, primary key, and sorted row values.
  - Distinct IDs for update preimages and postimages.
  - Idempotent Delta MERGE guarantees zero duplicate rows on replay.
- **Robust Recovery & Checkpointing Semantics**:
  - Checkpoint commits strictly after archive MERGE completes.
  - Verified crash recovery: crash between archive write and checkpoint commit reprocesses without duplicates and recovers checkpoint cleanly.
  - Empty version windows (e.g. metadata commits with 0 CDF data rows) advance checkpoints cleanly to prevent indefinite polling.
  - No-new-data detected cleanly (`no_op=True`).
  - Observational `replay_range` reads changes without mutating checkpoints.
- **Mutation Semantics Proven in Integration**:
  - `insert`: Newly inserted rows recorded with lineage.
  - `update_preimage` & `update_postimage`: Both states preserved with identical commit version.
  - `delete`: Final row image preserved for hard deletes.
  - Soft delete verified represented as update pre/postimages rather than physical delete.
- **Modern Databricks Alternative**:
  - Automatic CDF documented as modern Databricks alternative (status: `DOCUMENTED / NOT LOCALLY EXECUTED`).
- **Real Local CI (`.github/workflows/ci.yml`)**:
  - GitHub Actions running on Python 3.11, Java 17 Temurin, executing pytest, Ruff, wheel build, and isolated wheel smoke verification.
  - 100% credential-free local quality gate.
- **Declarative Automation Bundle (`databricks.yml`, `resources/lakeflow.pipeline.yml`)**:
  - Deploys frozen Module 5 Lakeflow AUTO CDC pipeline with `dev` and `prod` targets.
  - Serverless configuration with modern `schema:` property and synchronized `src/**` project package.
- **Secretless GitHub OIDC Deployment (`.github/workflows/databricks-deploy.yml`)**:
  - Manual `workflow_dispatch` trigger only with `id-token: write` and `DATABRICKS_AUTH_TYPE: github-oidc`.
  - Zero committed secrets or tokens.
  - Deployment status: `CONFIGURED / CLOUD DEPLOYMENT NOT EXECUTED`.
- **Test Framework**: Pytest (**206 passed unit & integration tests**; 38 M1 + 30 M2 + 49 M3 + 39 M4 + 24 M5 + 26 M6).
