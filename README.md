# Incremental & CDC Data Platform

A production-grade, local-first engineering framework designed to demonstrate end-to-end **Incremental Data Ingestion**, **Watermark Processing**, **Change Data Capture (CDC)**, **Sequence Ordering**, and **Deterministic Lakehouse State Reconciliation**.

---

## 1. Project Overview & Architectural Objectives

Modern enterprise data platforms cannot afford full snapshot reloads for massive transactional tables. This repository provides a complete, robust reference implementation demonstrating:

- **Deterministic Transactional Source Modeling**: Compact B2B SaaS subscription domain with referential integrity and explicit PySpark schemas (pinned to PySpark 3.5.x).
- **Transactional Watermark Incremental Ingestion**: Query-based incremental extraction using composite cursors `(updated_at, primary_key)` with SQLite control tables, explicit SQL transactions, optimistic concurrency versioning, and deterministic batch identities.
- **Durable Recoverable Window Contract**: Preserving uncommitted extraction boundaries across worker failures and retrying the exact frozen HIGH boundary even if source data changes mid-stream.
- **Change Data Capture (CDC) Event Streaming**: Granular transaction log replication capturing inserts, updates, and physical deletes with before and after images derived directly from the actual source state.
- **Authoritative Event Sequencing & Normalization**: PySpark window-based deduplication, conflicting duplicate event quarantine, out-of-order sequence normalization, equal-sequence collision quarantine, and dead-letter routing.
- **Golden Mutation Oracle**: In-memory transactional engine providing the exact ground truth for downstream lakehouse validation, with true deep-copy state isolation.

```
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
           │                               └──────────────┬──────────────┘└─────────────────────────────┘
           │                                              │
           └───────────────────────────┬──────────────────┘
                                       │
                                       ▼
                        ┌─────────────────────────────┐
                        │   Source Mutation Engine    │
                        │   (Reconciliation Oracle)   │
                        └─────────────────────────────┘
```

---

## 2. Core Concepts & Engineering Foundations

### Why Full Table Reloads Fail at Scale
In a naive full reload approach, pipelines re-read and overwrite entire tables every cycle ($O(N)$ data transfer). As data grows into millions or billions of rows:
1. **Source Database Strain**: High I/O and shared table locks degrade live application response times.
2. **Network Saturation**: Transferring gigabytes of unchanged data exhausts bandwidth.
3. **Exploding Batch Windows**: Ingestion duration expands linearly, causing pipelines to miss SLA targets.

### Watermark vs. Change Data Capture (CDC)

```
                       ┌──────────────────────────────────────┐
                       │       Incremental Ingestion          │
                       └──────────────────┬───────────────────┘
                                          │
                  ┌───────────────────────┴───────────────────────┐
                  ▼                                               ▼
     ┌───────────────────────────┐                  ┌───────────────────────────┐
     │    Watermark Ingestion    │                  │  Change Data Capture (CDC)│
     ├───────────────────────────┤                  ├───────────────────────────┤
     │ • WHERE updated_at > :t   │                  │ • WAL / Binlog Stream     │
     │ • Periodic batch polling  │                  │ • Continuous / Near Real-T│
     │ • Sees latest state only  │                  │ • Captures intermediate Δ │
     │ • Cannot see hard deletes │                  │ • Captures hard deletes   │
     └───────────────────────────┘                  └───────────────────────────┘
```

### The Physical Delete Blind Spot
When a row is physically deleted from a source database (`DELETE FROM table WHERE id = 'XYZ'`), the record ceases to exist on storage pages. A subsequent high-watermark query (`WHERE updated_at > :last_watermark`) will **never see the deleted record**.

Without a CDC replication stream emitting explicit `DELETE` events (or application-level soft-delete columns), the downstream target lakehouse retains stale deleted rows indefinitely.

---

## 3. Watermark Incremental Ingestion Architecture

### Composite Watermark Cursors
Single-column timestamp watermarks risk skipping rows or re-ingesting duplicates when multiple records share the exact same timestamp. Module 2 implements a composite cursor pairing `updated_at` with the table's primary key:

$$\text{LOW} < (\text{updated\_at}, \text{primary\_key}) \le \text{HIGH}$$

- **LOW (Exclusive)**: `(updated_at > low_ts) OR (updated_at = low_ts AND primary_key > low_key)`
- **HIGH (Inclusive)**: `(updated_at < high_ts) OR (updated_at = high_ts AND primary_key <= high_key)`

### Frozen High-Watermark Window & Durable Recovery
To prevent mid-query transaction commits from corrupting extraction windows, the pipeline captures the current maximum source watermark (`HIGH`) before querying. If extraction or landing fails, the uncommitted window is persisted in `watermark_run_audit` and reused on retry—even if source tables mutate before the retry occurs.

---

## 4. CDC Normalization, Ordering & Quarantine Pipeline

Module 3 transforms raw at-least-once CDC landing files into a trustworthy, authoritative stream using **PySpark DataFrames**:

1. **Exact Duplicate Replay Deduplication**: If multiple records share `event_id` and the identical SHA-256 event fingerprint, a single deterministic representative is retained (`batch_id ASC`, `ingestion_batch_id ASC`, `source_file ASC`, `ingestion_order ASC`) and redundant deliveries are dropped (`exact_duplicates_dropped`).
2. **Conflicting Duplicate Event ID Isolation**: If the same `event_id` appears with differing semantic payloads, all conflicting records are quarantined under `DUPLICATE_EVENT_CONFLICT`.
3. **Authoritative Entity Sequence Normalization**: Events for an entity are ordered strictly by `sequence_number` (normalizing out-of-order arrivals 102→101 into 101→102).
4. **Equal-Sequence Conflict Detection**: Multiple distinct events for the same entity sharing the same sequence number are quarantined under `SEQUENCE_CONFLICT`.
5. **Ingestion-Context Late Arrival Classification**: Classifies late events strictly using ingestion history and timestamp boundaries (`event_timestamp < prior_batches_max_event_timestamp`) rather than name heuristics.
6. **Portable Content-Addressed Processing ID**: Derives `processing_id` from logical file IDs (`batch_id=<id>/<file>`) and raw SHA-256 byte digests, ensuring identical IDs across machines and root paths.
7. **Decoupled Execution Timestamps**: Volatile run timestamps (`normalized_at`, `quarantined_at`) are excluded from `accepted.jsonl` and `quarantine.jsonl`, guaranteeing byte-for-byte identical replay outputs.
8. **Strict Primary Key Validation**: Enforces primary key presence and matching in all payload and before-image dictionaries.
9. **Dead-Letter Quarantine Store**: Malformed JSON lines and invalid structural/semantic records are routed to `data/quarantine/processing_id=<id>/quarantine.jsonl`.

---

## 5. Business Domain & Data Contracts

The platform models a high-fidelity B2B SaaS subscription lifecycle:

```
[accounts] (account_id)
   │ 1:N
   ▼
[subscriptions] (subscription_id, account_id)
   │ 1:N
   ▼
[invoices] (invoice_id, subscription_id)
   │ 1:N
   ▼
[payments] (payment_id, invoice_id)
```

### Table Schemas
1. **`accounts`**: `account_id` (PK), `account_name`, `industry`, `country`, `status`, `created_at`, `updated_at`.
2. **`subscriptions`**: `subscription_id` (PK), `account_id` (FK), `plan_name`, `billing_cycle`, `monthly_amount` (Decimal), `status`, `start_date`, `end_date`, `created_at`, `updated_at`.
3. **`invoices`**: `invoice_id` (PK), `subscription_id` (FK), `invoice_date`, `due_date`, `invoice_amount` (Decimal), `invoice_status`, `created_at`, `updated_at`.
4. **`payments`**: `payment_id` (PK), `invoice_id` (FK), `payment_date`, `payment_amount` (Decimal), `payment_method`, `payment_status`, `created_at`, `updated_at`.

---

## 6. Repository Structure

```
incremental-cdc-data-platform/
├── .gitignore
├── pyproject.toml
├── requirements.txt
├── requirements-dev.txt
├── README.md
├── docs/
│   ├── 01_CDC_FOUNDATIONS.md
│   ├── 02_WATERMARK_INCREMENTAL_INGESTION.md
│   ├── 03_CDC_NORMALIZATION_ORDERING.md
│   └── PROGRESS.md
├── src/
│   ├── source/
│   │   ├── schemas.py           # PySpark StructType & Dataclass contracts
│   │   ├── generator.py         # Deterministic synthetic snapshot generator
│   │   └── mutation_engine.py   # State mutation engine & reconciliation oracle
│   ├── cdc/
│   │   ├── models.py            # Canonical CDCEvent & CDCOperation contracts
│   │   ├── validator.py         # Structural & semantic CDC validator
│   │   ├── generator.py         # Deterministic multi-scenario change batches
│   │   └── serialization.py     # Deterministic JSONL serialization & I/O
│   ├── watermark/
│   │   ├── models.py            # CompositeWatermark & audit domain models
│   │   ├── control_store.py     # Durable SQLite control store & concurrency
│   │   ├── source_adapter.py    # Bounded composite watermark extractor
│   │   ├── landing.py           # Deterministic batch hashing & landing writer
│   │   └── pipeline.py          # Transactional watermark orchestrator
│   ├── normalization/
│   │   ├── models.py            # NormalizedCDCEvent, QuarantinedEvent, metrics
│   │   ├── schema.py            # PySpark StructType schemas
│   │   ├── fingerprint.py       # Deterministic canonical keys & SHA-256 hashing
│   │   ├── reader.py            # Fault-tolerant raw JSONL file reader
│   │   ├── validator.py         # Structural & semantic validation rules
│   │   ├── processor.py         # PySpark deduplication & sequence ordering engine
│   │   ├── writer.py            # Atomic JSONL partition writers & readers
│   │   └── pipeline.py          # End-to-end normalization orchestrator
│   └── utils/
│       └── helpers.py           # Date, Decimal, and path utilities
├── tests/
│   ├── conftest.py
│   ├── unit/
│   │   ├── test_schemas.py
│   │   ├── test_source_generator.py
│   │   ├── test_cdc_models.py
│   │   ├── test_cdc_validator.py
│   │   ├── test_cdc_generator.py
│   │   ├── test_mutation_engine.py
│   │   ├── test_serialization.py
│   │   ├── test_watermark_models.py
│   │   ├── test_watermark_control_store.py
│   │   ├── test_watermark_source_adapter.py
│   │   ├── test_watermark_landing.py
│   │   ├── test_normalization_fingerprint.py
│   │   ├── test_normalization_validator.py
│   │   ├── test_normalization_reader.py
│   │   └── test_normalization_processor.py
│   └── integration/
│       ├── test_end_to_end_simulator.py
│       ├── test_watermark_pipeline.py
│       └── test_normalization_pipeline.py
└── data/
    ├── source_snapshot/         # Local Parquet initial snapshots
    │   ├── accounts/.gitkeep
    │   ├── subscriptions/.gitkeep
    │   ├── invoices/.gitkeep
    │   └── payments/.gitkeep
    ├── cdc_landing/             # Partitioned raw JSONL change streams
    │   └── .gitkeep
    ├── watermark_landing/       # Partitioned incremental watermark landing
    │   └── .gitkeep
    ├── normalized_cdc/          # Partitioned accepted normalized change streams
    │   └── .gitkeep
    └── quarantine/              # Partitioned dead-letter quarantine store
        └── .gitkeep
```

---

## 7. Quickstart & Verification

### Local Environment Setup

```bash
# 1. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies in editable mode
pip install -r requirements-dev.txt
pip install -e .
```

### Run Full Test Suite (117 tests)

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

## 8. Project Roadmap (Modules 1–6)

- [x] **Module 1: Source System + Deterministic CDC Event Simulator** *(FROZEN / COMPLETE)*
  - Synthetic B2B SaaS generator, Parquet initial snapshots, CDC event generator (Inserts, Updates, Deletes, Dups, Out-of-Order, Late, Quarantine), Structured Validator, In-memory Mutation Engine.
- [x] **Module 2: Transactional Watermark Incremental Ingestion + Control Tables** *(FROZEN / COMPLETE)*
  - Durable SQLite control tables, explicit SQL transactions, composite cursors `(updated_at, PK)`, bounded window extraction, durable recoverable window contract, optimistic concurrency versioning, failure recovery, physical delete blind-spot testing.
- [x] **Module 3: CDC Normalization, Ordering, Dedupe & Quarantine** *(COMPLETED)*
  - Raw JSONL ingestion, structural & semantic validation, PySpark window-based deduplication, duplicate-event conflict quarantine, authoritative entity sequence ordering, dead-letter quarantine store, replay determinism.
- [ ] **Module 4: Delta MERGE, Deletes, Replay & Recovery**
  - Silver layer Delta MERGE implementation, hard delete handling, tombstone compaction, deterministic time-travel replay.
- [ ] **Module 5: Databricks Lakeflow AUTO CDC**
  - Modern Lakeflow Declarative Pipeline definitions with native AUTO CDC constructs.
- [ ] **Module 6: Delta Change Data Feed, CI/CD & Final Hardening**
  - Downstream Gold layer consumption via Delta Change Data Feed (CDF), end-to-end reconciliation tests, automated quality gates.
