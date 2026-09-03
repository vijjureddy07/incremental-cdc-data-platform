# Incremental & CDC Data Platform

A production-grade, local-first engineering framework designed to demonstrate end-to-end **Incremental Data Ingestion**, **Watermark Processing**, **Change Data Capture (CDC)**, **Sequence Ordering**, **Delta Lake MERGE**, **Databricks Lakeflow AUTO CDC**, **Delta Change Data Feed (CDF)**, and **Deterministic Lakehouse State Reconciliation**.

---

## 1. Project Overview & Architectural Objectives

Modern enterprise data platforms cannot afford full snapshot reloads for massive transactional tables. This repository provides a complete, robust reference implementation demonstrating:

- **Deterministic Transactional Source Modeling**: Compact B2B SaaS subscription domain with referential integrity and explicit PySpark schemas (pinned to PySpark 3.5.x).
- **Transactional Watermark Incremental Ingestion**: Query-based incremental extraction using composite cursors `(updated_at, primary_key)` with SQLite control tables, explicit SQL transactions, optimistic concurrency versioning, and deterministic batch identities.
- **Durable Recoverable Window Contract**: Preserving uncommitted extraction boundaries across worker failures and retrying the exact frozen HIGH boundary even if source data changes mid-stream.
- **Change Data Capture (CDC) Event Streaming**: Granular transaction log replication capturing inserts, updates, and physical deletes with before and after images derived directly from the actual source state.
- **Authoritative Event Sequencing & Normalization**: PySpark window-based deduplication, conflicting duplicate event quarantine, out-of-order sequence normalization, equal-sequence collision quarantine, and dead-letter routing.
- **Delta Lake MERGE & Recovery Engine**: ACID current-state target tables with two-phase applied event ledger, stale resurrection protection, sequence wave grouping with ambiguity checks, HARD/SOFT delete policies, and crash recovery.
- **Databricks Lakeflow Declarative Pipelines & AUTO CDC**: Cloud-native managed CDC ingestion with streaming tables, initial hydration flows (`once=True`), continuous CDC flows, managed tombstones, and SCD Type 2 history tracking.
- **Delta Change Data Feed & Downstream Audit Archive**: Downstream change feed consumption over Delta current-state tables with SQLite checkpointing, multi-table state isolation, deterministic SHA-256 `_change_id`, update pre/postimages, and idempotent MERGE archiving.
- **Engineering Quality & CI/CD**: Real GitHub Actions CI workflow, Databricks Declarative Automation Bundles, secretless GitHub OIDC deployment, and isolated wheel verification.

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                     SOURCE LAYER                                       │
│                                                                                        │
│   ┌──────────────┐       ┌───────────────┐       ┌──────────────┐     ┌───────────┐    │
│   │   accounts   │─────► │ subscriptions │─────► │   invoices   │───► │  payments │    │
│   └──────────────┘       └───────────────┘       └──────────────┘     └───────────┘    │
└──────────┬───────────────────────┬──────────────────────────────────────────┬──────────┘
           │                       │                                          │
   [Snapshot Export]       [Watermark Ingestion]                      [CDC Event Stream]
           │                       │                                          │
           ▼                       ▼                                          ▼
┌─────────────────────┐ ┌───────────────────────────────┐ ┌─────────────────────────────┐
│data/source_snapshot/│ │   data/watermark_landing/     │ │      data/cdc_landing/      │
│   (Parquet Files)   │ │ table=X/batch_id=Y/data.jsonl │ │       (JSONL Batches)       │
└──────────┬──────────┘ └──────────────┬────────────────┘ └──────────────┬──────────────┘
           │                           │                                 │
           │                           ▼                                 │
           │            ┌───────────────────────────────┐                ▼
           │            │     SQLite Control Store      │ ┌─────────────────────────────┐
           │            │  (watermark_state & audits)   │ │  CDCNormalizationPipeline   │
           │            └───────────────────────────────┘ │    (PySpark 3.5 Engine)     │
           │                                              └──────────────┬──────────────┘
           │                                                             │
           │                                              ┌──────────────┴──────────────┐
           │                                              ▼                             ▼
           │                               ┌─────────────────────────────┐┌─────────────────────────────┐
           │                               │    data/normalized_cdc/     ││      data/quarantine/       │
           │                               │processing_id=P/accept.jsonl ││processing_id=P/quarant.jsonl│
           │                               └───────┬──────────────┬──────┘└─────────────────────────────┘
           │                                       │              │
           │                                       │              └─────────────────────────────┐
           │                                       ▼                                            ▼
           │                        ┌─────────────────────────────┐              ┌─────────────────────────────┐
           │                        │      DeltaMergePipeline     │              │      Databricks Lakeflow    │
           │                        │   (Two-Phase MERGE Engine)  │              │     AUTO CDC (Declarative)  │
           │                        └──────┬───────────────┬──────┘              └──────────────┬──────────────┘
           │                               │               │                                    │
           │                               ▼               ▼                                    ▼
           │                ┌────────────────────┐ ┌─────────────────────────────┐ ┌─────────────────────────────┐
           │                │ data/delta/current │ │ data/delta/control/ledger   │ │ Managed Streaming Tables    │
           │                │ (ACID Targets)     │ │ (PENDING -> APPLIED states) │ │ (accounts, subs_history)    │
           │                └──────────┬─────────┘ └─────────────────────────────┘ └─────────────────────────────┘
           │                           │
           │                           ▼
           │            ┌──────────────────────────────────────────────┐
           │            │       Delta Change Data Feed (CDF)           │
           │            │      (delta.enableChangeDataFeed = true)     │
           │            └──────────────────────┬───────────────────────┘
           │                                   │
           │                                   ▼
           │            ┌──────────────────────────────────────────────┐
           │            │    CDFDownstreamPipeline & SQLite Control    │
           │            │      (Deterministic SHA-256 _change_id)      │
           │            └──────────────────────┬───────────────────────┘
           │                                   │
           │                                   ▼
           │            ┌──────────────────────────────────────────────┐
           │            │        Permanent Delta CDF Archive           │
           │            │   (data/delta/downstream/cdf_archive/*)      │
           │            └──────────────────────────────────────────────┘
           │                                   │
           └───────────────────────────────────┼─────────────────────────┐
                                               │                         │
                                               ▼                         ▼
                                ┌──────────────────────────────────────────────┐
                                │           Source Mutation Engine             │
                                │        (Golden Reconciliation Oracle)        │
                                └──────────────────────────────────────────────┘
```

---

## 2. Core Concepts & Engineering Foundations

### Three Distinct CDC Concepts
The platform distinguishes three separate layers of change processing:
1. **Source CDC (Module 3)**: Ingestion, structural validation, event fingerprinting, out-of-order sequence normalization, and dead-letter quarantine from raw change streams.
2. **Target CDC Application (Modules 4 & 5)**: Mutating current-state Delta tables exactly once with replay recovery, stale sequence protection, and delete propagation (imperatively via Delta MERGE in Module 4; declaratively via Lakeflow AUTO CDC in Module 5).
3. **Downstream Change Feed (Module 6)**: Capturing row-level mutations emitted by target Delta tables via Delta Change Data Feed (CDF), deriving deterministic SHA-256 `_change_id`s, and storing permanent history in idempotent Delta archives.

---

## 3. Watermark Incremental Ingestion Architecture

### Composite Watermark Cursors
Single-column timestamp watermarks risk skipping rows or re-ingesting duplicates when multiple records share the exact same timestamp. Module 2 implements a composite cursor pairing `updated_at` with the table's primary key:

$$\text{LOW} < (\text{updated\_at}, \text{primary\_key}) \le \text{HIGH}$$

- **LOW (Exclusive)**: `(updated_at > low_ts) OR (updated_at = low_ts AND primary_key > low_key)`
- **HIGH (Inclusive)**: `(updated_at < high_ts) OR (updated_at = high_ts AND primary_key <= high_key)`

---

## 4. CDC Normalization, Ordering & Quarantine Pipeline

Module 3 transforms raw at-least-once CDC landing files into a trustworthy, authoritative stream using **PySpark DataFrames**:
1. Exact duplicate deduplication via SHA-256 event fingerprints.
2. Conflicting duplicate event ID quarantine under `DUPLICATE_EVENT_CONFLICT`.
3. Authoritative per-entity sequence ordering (normalizing out-of-order arrivals 102→101 into 101→102).
4. Equal-sequence conflict detection and quarantine under `SEQUENCE_CONFLICT`.
5. Portable content-addressed `processing_id` derived from raw byte digests.
6. Decoupled execution timestamps guaranteeing byte-for-byte identical replay outputs.

---

## 5. Delta Lake MERGE, Delete Propagation & Recovery

Module 4 applies canonical accepted CDC events into ACID-compliant Delta Lake current-state tables:
1. Embeds 8 operational metadata lineage columns.
2. Coordinates `PENDING` $\rightarrow$ `APPLIED` two-phase transactional groups using a Delta event ledger.
3. Prevents stale resurrection after physical hard delete by retaining the entity's maximum applied sequence indefinitely.
4. Groups actionable mutations into deterministic sequence waves with ambiguity guards.
5. Implements HARD and SOFT delete policies with crash recovery.

---

## 6. Databricks Lakeflow AUTO CDC Architecture

Module 5 provides the native, managed Databricks Lakeflow implementation:
1. Declarative streaming tables and AUTO CDC flows using modern `pyspark.pipelines`.
2. Initial snapshot hydration flows (`once=True`) with baseline `sequence_number = 0`.
3. Continuous AUTO CDC flows with native delete application (`apply_as_deletes`).
4. Aligned source schema contracts derived from authoritative frozen table schemas.
5. SCD Type 2 history tracking on `subscriptions_history`.
6. Managed tombstones (`pipelines.cdc.tombstoneGCThresholdInSeconds = 604800`) preventing out-of-order delete resurrection.

---

## 7. Delta Change Data Feed & Downstream Archive

Module 6 establishes a restartable downstream change consumer and permanent audit archive:
1. **Delta CDF Reader**: Bounded version range reader consuming `[start_version, end_version]` with canonical metadata validation (`_change_type`, `_commit_version`, `_commit_timestamp`).
2. **Consumer State Store**: Dedicated SQLite store (`data/control/cdf_consumer.db`) with strict multi-table checkpoint isolation.
3. **Deterministic SHA-256 `_change_id`**: Derived from table name, commit version, change type, primary key, and sorted row values.
4. **Permanent Delta Archive**: Durable storage (`data/delta/downstream/cdf_archive/*`) updated via idempotent Delta MERGE.
5. **Crash Recovery**: Checkpoint advances strictly after archive write; crash before checkpoint recovers cleanly without duplicate rows.

---

## 8. CI/CD & Declarative Automation Bundles

1. **GitHub Actions CI (`.github/workflows/ci.yml`)**:
   - Automated quality gate running on Ubuntu, Python 3.11, Java 17 Temurin.
   - Executes Pytest, Ruff linting, wheel packaging, and isolated wheel smoke verification.
2. **Databricks Declarative Automation Bundles (`databricks.yml`)**:
   - Manages deployment of the Lakeflow AUTO CDC pipeline with `dev` and `prod` targets.
   - Serverless configuration with synchronized `src/**` project files.
3. **Secretless GitHub OIDC Deployment (`.github/workflows/databricks-deploy.yml`)**:
   - Manual `workflow_dispatch` trigger with `id-token: write` and `DATABRICKS_AUTH_TYPE: github-oidc`.
   - Zero hardcoded credentials or tokens.

---

## 9. Repository Structure

```
incremental-cdc-data-platform/
├── .github/
│   └── workflows/
│       ├── ci.yml                 # Real GitHub Actions CI workflow
│       └── databricks-deploy.yml  # Secretless OIDC Databricks deploy workflow
├── databricks.yml                 # Declarative Automation Bundle root
├── resources/
│   └── lakeflow.pipeline.yml      # Lakeflow AUTO CDC pipeline resource definition
├── pyproject.toml
├── requirements.txt
├── requirements-dev.txt
├── README.md
├── docs/
│   ├── 01_CDC_FOUNDATIONS.md
│   ├── 02_WATERMARK_INCREMENTAL_INGESTION.md
│   ├── 03_CDC_NORMALIZATION_ORDERING.md
│   ├── 04_DELTA_MERGE_REPLAY_RECOVERY.md
│   ├── 05_LAKEFLOW_AUTO_CDC.md
│   ├── 06_CDF_CICD_FINAL_HARDENING.md
│   └── PROGRESS.md
├── databricks/
│   └── lakeflow/
│       ├── __init__.py
│       ├── config.py
│       ├── contracts.py
│       ├── pipeline.py
│       └── sql/
│           └── auto_cdc_reference.sql
├── src/
│   ├── source/                    # Module 1: Synthetic source & mutation engine
│   ├── cdc/                       # Module 1: CDC generation & serialization
│   ├── watermark/                 # Module 2: Watermark incremental extraction
│   ├── normalization/             # Module 3: CDC normalization & quarantine
│   ├── merge/                     # Module 4: Delta MERGE & recovery engine
│   ├── cdf/                       # Module 6: Delta CDF reader, archive & pipeline
│   └── utils/                     # Shared date, decimal, and path helpers
└── tests/
    ├── conftest.py
    ├── unit/
    └── integration/
```

---

## 10. Quickstart & Verification

### Local Environment Setup

```bash
# 1. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies in editable mode
pip install -r requirements-dev.txt
pip install -e .
```

### Run Full Test Suite (206 tests)

```bash
pytest -v
```

### Run Code Quality & Linting (Ruff)

```bash
ruff check .
```

### Build Wheel Package & Verify Isolated Installation

```bash
python -m build --wheel
```

---

## 11. Project Roadmap (Modules 1–6)

- [x] **Module 1: Source System + Deterministic CDC Event Simulator** *(FROZEN / COMPLETE)*
- [x] **Module 2: Transactional Watermark Incremental Ingestion + Control Tables** *(FROZEN / COMPLETE)*
- [x] **Module 3: CDC Normalization, Ordering, Dedupe & Quarantine** *(FROZEN / COMPLETE)*
- [x] **Module 4: Delta MERGE, Delete Propagation, Idempotent Replay & Recovery** *(FROZEN / COMPLETE)*
- [x] **Module 5: Databricks Lakeflow AUTO CDC** *(FROZEN / COMPLETE / CLOUD VALIDATION PENDING)*
- [x] **Module 6: Delta Change Data Feed, Downstream Recovery, CI/CD & Final Hardening** *(COMPLETE / CLOUD DEPLOYMENT PENDING)*
