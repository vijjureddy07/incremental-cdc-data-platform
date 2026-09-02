# Incremental & CDC Data Platform

A production-grade, local-first engineering framework designed to demonstrate end-to-end **Incremental Data Ingestion**, **Watermark Processing**, **Change Data Capture (CDC)**, **Sequence Ordering**, and **Deterministic Lakehouse State Reconciliation**.

---

## 1. Project Overview & Architectural Objectives

Modern enterprise data platforms cannot afford full snapshot reloads for massive transactional tables. This repository provides a complete, robust reference implementation demonstrating:

- **Deterministic Transactional Source Modeling**: Compact B2B SaaS subscription domain with referential integrity and explicit PySpark schemas (pinned to PySpark 3.5.x).
- **Transactional Watermark Incremental Ingestion**: Query-based incremental extraction using composite cursors `(updated_at, primary_key)` with SQLite control tables, explicit SQL transactions, optimistic concurrency versioning, and deterministic batch identities.
- **Durable Recoverable Window Contract**: Preserving uncommitted extraction boundaries across worker failures and retrying the exact frozen HIGH boundary even if source data changes mid-stream.
- **Change Data Capture (CDC) Event Streaming**: Granular transaction log replication capturing inserts, updates, and physical deletes with before and after images derived directly from the actual source state.
- **Authoritative Event Sequencing**: Strict out-of-order and duplicate reconciliation using monotonically increasing sequence numbers (strictly monotonic per business key without using `event_timestamp` as a tiebreaker).
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
           │            ┌───────────────────────────────┐                │
           │            │     SQLite Control Store      │                │
           │            │  (watermark_state & audits)   │                │
           │            └───────────────────────────────┘                │
           │                                                             │
           └───────────────────────────┬─────────────────────────────────┘
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

### Durable SQLite Control Store with Explicit Transactions
Durable local metadata management tracking:
- `watermark_state`: Table name, cursor columns, last committed composite watermark, version, last run ID, update timestamp.
- `watermark_run_audit`: Execution attempt history (`RUNNING`, `SUCCESS`, `NO_DATA`, `FAILED`), row counts, and landing paths.
- **Explicit SQL Transactions**: Uses `BEGIN IMMEDIATE` / `COMMIT` / `ROLLBACK` for robust ACID isolation across all control table writes.
- **Atomic Completion**: `commit_successful_run` atomically executes compare-and-swap watermark update AND marks audit `SUCCESS` in a single transaction.
- **Optimistic Concurrency**: Compare-and-swap SQL version verification (`UPDATE ... WHERE table_name = ? AND version = ?`) preventing concurrent writer collisions (`WatermarkConcurrencyError`).

### Transactional Checkpointing Order
1. Start run audit (`RUNNING`)
2. Read `LOW` watermark & check for recoverable failed/uncompleted window
3. Resolve `HIGH` watermark & compute deterministic `batch_id`
4. Update audit with window boundaries and `batch_id`
5. If `HIGH <= LOW`: mark `NO_DATA`, exit without advancing watermark
6. Extract bounded rows
7. Write landing output (`data/watermark_landing/table=<t>/batch_id=<b>/data.jsonl`)
8. Read back and verify landed file existence and exact row count
9. Atomically commit watermark checkpoint (CAS version check) AND mark audit `SUCCESS`

---

## 4. Business Domain & Data Contracts

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

### Initial Snapshot Counts
- **Accounts**: 40 records
- **Subscriptions**: 60 records
- **Invoices**: 120 records
- **Payments**: 90 records

---

## 5. Repository Structure

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
│   │   └── test_watermark_landing.py
│   └── integration/
│       ├── test_end_to_end_simulator.py
│       └── test_watermark_pipeline.py
└── data/
    ├── source_snapshot/         # Local Parquet initial snapshots
    │   ├── accounts/.gitkeep
    │   ├── subscriptions/.gitkeep
    │   ├── invoices/.gitkeep
    │   └── payments/.gitkeep
    ├── cdc_landing/             # Partitioned raw JSONL change streams
    │   └── .gitkeep
    └── watermark_landing/       # Partitioned incremental watermark landing
        └── .gitkeep
```

---

## 6. Quickstart & Verification

### Local Environment Setup

```bash
# 1. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies in editable mode
pip install -r requirements-dev.txt
pip install -e .
```

### Run Full Test Suite (68 tests)

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

## 7. Project Roadmap (Modules 1–6)

- [x] **Module 1: Source System + Deterministic CDC Event Simulator** *(FROZEN / COMPLETE)*
  - Synthetic B2B SaaS generator, Parquet initial snapshots, CDC event generator (Inserts, Updates, Deletes, Dups, Out-of-Order, Late, Quarantine), Structured Validator, In-memory Mutation Engine.
- [x] **Module 2: Transactional Watermark Incremental Ingestion + Control Tables** *(COMPLETED)*
  - Durable SQLite control tables, explicit SQL transactions, composite cursors `(updated_at, PK)`, bounded window extraction, durable recoverable window contract, optimistic concurrency versioning, failure recovery, physical delete blind-spot testing.
- [ ] **Module 3: CDC Normalization, Ordering, Dedupe & Quarantine**
  - Bronze landing ingestion, PySpark window-based deduplication, authoritative sequence ordering, dead-letter quarantine routing.
- [ ] **Module 4: Delta MERGE, Deletes, Replay & Recovery**
  - Silver layer Delta MERGE implementation, hard delete handling, tombstone compaction, deterministic time-travel replay.
- [ ] **Module 5: Databricks Lakeflow AUTO CDC**
  - Modern Lakeflow Declarative Pipeline definitions with native AUTO CDC constructs.
- [ ] **Module 6: Delta Change Data Feed, CI/CD & Final Hardening**
  - Downstream Gold layer consumption via Delta Change Data Feed (CDF), end-to-end reconciliation tests, automated quality gates.
