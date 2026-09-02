# Incremental & CDC Data Platform

A production-grade, local-first engineering framework designed to demonstrate end-to-end **Incremental Data Ingestion**, **Watermark Processing**, **Change Data Capture (CDC)**, **Sequence Ordering**, and **Deterministic Lakehouse State Reconciliation**.

---

## 1. Project Overview & Architectural Objectives

Modern enterprise data platforms cannot afford full snapshot reloads for massive transactional tables. This repository provides a complete, robust reference implementation demonstrating:

- **Deterministic Transactional Source Modeling**: Compact B2B SaaS subscription domain with referential integrity and explicit PySpark schemas (pinned to PySpark 3.5.x).
- **High-Watermark Incremental Processing**: Fast, lightweight query-based incremental extraction using timestamps (`updated_at`).
- **Change Data Capture (CDC) Event Streaming**: Granular transaction log replication capturing inserts, updates, and physical deletes with before and after images derived directly from the actual source state.
- **Authoritative Event Sequencing**: Strict out-of-order and duplicate reconciliation using monotonically increasing sequence numbers (strictly monotonic per business key without using `event_timestamp` as a tiebreaker).
- **Golden Mutation Oracle**: In-memory transactional engine providing the exact ground truth for downstream lakehouse validation, with true deep-copy state isolation.

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                               SOURCE LAYER                                       │
│                                                                                  │
│   ┌──────────────┐     ┌───────────────┐     ┌──────────────┐     ┌───────────┐  │
│   │   accounts   │───► │ subscriptions │───► │   invoices   │───► │  payments │  │
│   └──────────────┘     └───────────────┘     └──────────────┘     └───────────┘  │
└──────────────────────────┬───────────────────────────┬───────────────────────────┘
                           │                           │
          [Initial Snapshot Export]             [CDC Event Stream]
                           │                           │
                           ▼                           ▼
        ┌─────────────────────────────┐   ┌─────────────────────────────┐
        │    data/source_snapshot/    │   │      data/cdc_landing/      │
        │      (Parquet Files)        │   │       (JSONL Batches)       │
        └──────────────┬──────────────┘   └──────────────┬──────────────┘
                       │                                 │
                       └────────────────┬────────────────┘
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

## 3. Business Domain & Data Contracts

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

## 4. CDC Event Model & Ordering Contract

Every change event follows a strict, strongly typed contract derived directly from the source snapshot baseline:

```json
{
  "event_id": "evt_upd_sub_0001",
  "table_name": "subscriptions",
  "operation": "UPDATE",
  "business_key": {"subscription_id": "SUB-0001"},
  "sequence_number": 15,
  "event_timestamp": "2026-04-01T10:35:00Z",
  "source_commit_timestamp": "2026-04-01T10:35:01Z",
  "batch_id": "batch_001",
  "before_payload": {
    "subscription_id": "SUB-0001",
    "account_id": "ACC-0001",
    "plan_name": "STARTER",
    "billing_cycle": "ANNUAL",
    "monthly_amount": "49.00",
    "status": "PAUSED",
    "start_date": "2026-03-06",
    "end_date": null,
    "created_at": "2026-03-06T00:00:00Z",
    "updated_at": "2026-03-06T16:00:00Z"
  },
  "payload": {
    "subscription_id": "SUB-0001",
    "account_id": "ACC-0001",
    "plan_name": "ENTERPRISE",
    "billing_cycle": "ANNUAL",
    "monthly_amount": "1299.00",
    "status": "PAUSED",
    "start_date": "2026-03-06",
    "end_date": null,
    "created_at": "2026-03-06T00:00:00Z",
    "updated_at": "2026-04-01T10:35:00Z"
  },
  "source_system": "b2b_saas_postgres"
}
```

### Authoritative Sequencing: `sequence_number` vs `event_timestamp`
- **`sequence_number`**: Monotonically increasing sequence assigned by the database transaction commit log (WAL LSN). **Strictly authoritative for ordering**. A change event with `sequence_number <= current_max_seq` is rejected as stale/non-monotonic.
- **`event_timestamp`**: Application event time. Subject to clock drift, server skew, and network retries. **Never used as an authoritative tiebreaker** between conflicting sequence states.
- **`source_commit_timestamp`**: Timestamp when the transaction committed to disk in the source database.
- **`batch_id`**: Ingestion file chunking/partitioning grouping, **not** business ordering.

### Streaming Anomalies Handled Deterministically
1. **Source State Truth**: All `before_payload` images are derived directly from the actual deterministic source snapshot rather than hard-coded fictional values.
2. **Field Preservation**: `payload` preserves all untouched source columns and modifies only business fields and `updated_at`.
3. **Duplicate Events**: At-least-once delivery duplicates are filtered via `event_id` tracking and sequence checks.
4. **Out-of-Order Delivery**: Tested both with pre-sorting and in raw arrival order (`sort_by_sequence=False`) where `seq 102` is accepted and `seq 101` is rejected as stale.
5. **Late-Arriving Events**: Historical events arriving in future batches are merged only if their sequence number exceeds current target state.
6. **Quarantine Fixtures**: Missing PK, missing sequence number, negative sequence, invalid operation (`TRUNCATE`), and missing delete before-image are validated and quarantined.

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
│   │   └── test_serialization.py
│   └── integration/
│       └── test_end_to_end_simulator.py
└── data/
    ├── source_snapshot/         # Local Parquet initial snapshots
    │   ├── accounts/.gitkeep
    │   ├── subscriptions/.gitkeep
    │   ├── invoices/.gitkeep
    │   └── payments/.gitkeep
    └── cdc_landing/             # Partitioned raw JSONL change streams
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

### Run Full Test Suite

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

- [x] **Module 1: Source System + Deterministic CDC Event Simulator** *(CURRENT)*
  - Synthetic B2B SaaS generator, Parquet initial snapshots, CDC event generator (Inserts, Updates, Deletes, Dups, Out-of-Order, Late, Quarantine), Structured Validator, In-memory Mutation Engine.
- [ ] **Module 2: Watermark Incremental Ingestion + Control Tables**
  - High-watermark metadata state tracking, incremental delta extraction, watermark control tables.
- [ ] **Module 3: CDC Normalization, Ordering, Dedupe & Quarantine**
  - Bronze landing ingestion, PySpark window-based deduplication, authoritative sequence ordering, dead-letter quarantine routing.
- [ ] **Module 4: Delta MERGE, Deletes, Replay & Recovery**
  - Silver layer Delta MERGE implementation, hard delete handling, tombstone compaction, deterministic time-travel replay.
- [ ] **Module 5: Databricks Lakeflow AUTO CDC**
  - Modern Lakeflow Declarative Pipeline definitions with native AUTO CDC constructs.
- [ ] **Module 6: Delta Change Data Feed, CI/CD & Final Hardening**
  - Downstream Gold layer consumption via Delta Change Data Feed (CDF), end-to-end reconciliation tests, automated quality gates.
