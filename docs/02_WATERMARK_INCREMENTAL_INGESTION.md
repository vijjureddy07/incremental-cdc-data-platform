# 02 — Transactional Watermark Incremental Ingestion & Control Tables

> **Learning Status**: `NOT STUDIED / PENDING`  
> *Track your conceptual mastery by updating this status as you study each foundation.*

---

## 1. Full vs. Incremental Ingestion

### Full Snapshot Reload ($O(N)$)
A full snapshot reload queries and transfers every single row in the source table during every pipeline run. While simple, it creates linear compute and network costs ($O(N)$), places heavy shared read locks on production OLTP databases, and eventually exceeds SLA ingestion windows as datasets grow.

### Incremental Watermark Ingestion ($O(\Delta)$)
Incremental ingestion queries only records that have been created or modified since the last synchronization point. By processing only new deltas ($\Delta \ll N$), incremental ingestion slashes execution times, reduces source database strain, and enables near real-time ingestion cycles.

---

## 2. Watermark Extraction Windows: LOW and HIGH Boundaries

A watermark pipeline maintains a cursor representing the latest source timestamp processed so far.

```
                  ┌──────────────────────────────────────────────┐
                  │          Source Table Mutation Timeline      │
                  └──────────────────────────────────────────────┘
─────────────────────────────┼──────────────────────────────┼────────────────────────► Time
                             ▲                              ▲
                             │                              │
                     Persisted LOW                   Captured HIGH
                      (Exclusive)                     (Inclusive)
                             │                              │
                             └────── Extraction Window ─────┘
                                   LOW < cursor <= HIGH
```

### Why HIGH Must Be Captured Before Extraction (Freezing the Window)
In a live database, transactions continually insert and update records while a query runs.
- If a query uses an open-ended predicate (`WHERE updated_at > :low`), rows added mid-query may or may not be scanned.
- If the pipeline subsequently commits `NOW()` or `max(scanned_updated_at)` as the new watermark, rows that committed with an earlier timestamp during query execution could be skipped on the next run.
- **Solution**: At the start of the run, the pipeline captures the current maximum source timestamp (`HIGH`) and queries the **bounded window**:
$$\text{LOW} < (\text{updated\_at}, \text{PK}) \le \text{HIGH}$$
Any records inserted or updated after `HIGH` was frozen wait cleanly for the next run.

---

## 3. The Timestamp Collision Problem & Composite Watermarks

### The Flaw of Single-Column Timestamp Watermarks
Consider three records modified in the same second:
```
(2026-09-02T10:00:00Z, ACC-0100)
(2026-09-02T10:00:00Z, ACC-0101)
(2026-09-02T10:00:00Z, ACC-0102)
```
If a batch size limit or extraction boundary commits `2026-09-02T10:00:00Z` after processing `ACC-0100`:
- A strict predicate `WHERE updated_at > '2026-09-02T10:00:00Z'` will **skip `ACC-0101` and `ACC-0102`**, causing silent data loss.
- An inclusive predicate `WHERE updated_at >= '2026-09-02T10:00:00Z'` will **re-read `ACC-0100`**, causing duplicate ingestion on every subsequent cycle.

### The Solution: Composite Watermark `(updated_at, Primary Key)`
A composite cursor pairs the modification timestamp with the entity's primary key as a deterministic tie-breaker:

1. **LOW Exclusive Predicate**:
```sql
(updated_at > :low_timestamp)
OR
(updated_at = :low_timestamp AND primary_key > :low_key)
```

2. **HIGH Inclusive Predicate**:
```sql
(updated_at < :high_timestamp)
OR
(updated_at = :high_timestamp AND primary_key <= :high_key)
```

This guarantees an unambiguous, total order across all rows, preventing both skipped records and duplicate ingestion at timestamp boundaries.

---

## 4. Control Store Architecture & Schema

The control store provides durable state management and operational auditability using SQLite.

```
┌────────────────────────────────────────────────────────────────────────┐
│                          watermark_state                               │
├──────────────────────────┬──────────────────────┬──────────────────────┤
│ table_name (PK)          │ VARCHAR(64)          │ "accounts"           │
│ watermark_column         │ VARCHAR(64)          │ "updated_at"         │
│ tie_breaker_column       │ VARCHAR(64)          │ "account_id"         │
│ last_watermark_timestamp │ VARCHAR(32)          │ "2026-05-11T01:30:00"│
│ last_watermark_key       │ VARCHAR(64)          │ "ACC-0001"           │
│ version                  │ INTEGER              │ 3                    │
│ last_success_run_id      │ VARCHAR(64)          │ "run_acc_98a72b"     │
│ updated_at               │ VARCHAR(32)          │ "2026-05-11T01:35:00"│
└──────────────────────────┴──────────────────────┴──────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│                        watermark_run_audit                             │
├──────────────────────────┬──────────────────────┬──────────────────────┤
│ run_id (PK)              │ VARCHAR(64)          │ "run_acc_98a72b"     │
│ table_name               │ VARCHAR(64)          │ "accounts"           │
│ batch_id                 │ VARCHAR(64)          │ "batch_accounts_7f4a"│
│ low_watermark_timestamp  │ VARCHAR(32)          │ "2026-03-02T07:00:00"│
│ low_watermark_key        │ VARCHAR(64)          │ "ACC-0040"           │
│ high_watermark_timestamp │ VARCHAR(32)          │ "2026-05-11T01:30:00"│
│ high_watermark_key       │ VARCHAR(64)          │ "ACC-0001"           │
│ status                   │ VARCHAR(16)          │ "SUCCESS"            │
│ rows_extracted           │ INTEGER              │ 2                    │
│ landing_path             │ TEXT                 │ "data/watermark_..." │
│ started_at               │ VARCHAR(32)          │ "2026-05-11T01:35:01"│
│ completed_at             │ VARCHAR(32)          │ "2026-05-11T01:35:02"│
│ error_message            │ TEXT                 │ NULL                 │
└──────────────────────────┴──────────────────────┴──────────────────────┘
```

---

## 5. Transactional Watermark Checkpointing & Order of Operations

A watermark must **never** be committed simply because extraction began. It must only advance after target data has landed and verified.

```
       1. Read State ──────► 2. Capture HIGH ──────► 3. Check NO_DATA
             │                      │                       │
             ▼                      ▼                       ▼
       4. Start Audit ─────► 5. Extract Rows ──────► 6. Land JSONL
             │                      │                       │
             ▼                      ▼                       ▼
       7. Verify File ─────► 8. Commit State ──────► 9. Mark SUCCESS
```

### Failure Scenarios & Recovery:
1. **Crash during extraction or landing (Steps 4–6)**:
   - Target watermark remains at `LOW`.
   - Run audit is marked `FAILED`.
   - On retry: the identical bounded window is re-extracted cleanly.
2. **Crash after landing but before checkpoint commit (Step 7/8)**:
   - Landing file exists on disk, but control store remains at `LOW`.
   - On retry: deterministic `batch_id` calculation targets the exact same landing directory, overwriting the file atomically. The checkpoint commits safely without creating duplicate batches.

---

## 6. Deterministic Batch Identity vs. Execution Run ID

- **`batch_id`**: Deterministic SHA-256 hash of `(table_name, low_timestamp, low_key, high_timestamp, high_key)`.
  - Identifies the **logical data payload window**.
  - Retrying the exact same extraction window generates the exact same `batch_id` and targets the exact same landing directory (`table=<table_name>/batch_id=<batch_id>/data.jsonl`).
- **`run_id`**: Unique execution attempt identifier (e.g. `run_accounts_8f7b2c`).
  - Identifies an individual process execution attempt for operational telemetry and audit logging.

---

## 7. Optimistic Concurrency Control

To prevent two concurrent pipeline workers from corrupting watermark state or overwriting newer checkpoints:
1. When worker reads watermark state, it records the `expected_version`.
2. When committing, worker executes compare-and-swap SQL:
```sql
UPDATE watermark_state
SET last_watermark_timestamp = :high_ts,
    last_watermark_key = :high_key,
    version = version + 1,
    last_success_run_id = :run_id,
    updated_at = :now
WHERE table_name = :table_name AND version = :expected_version;
```
3. If another worker advanced the version in the interim (`rowcount == 0`), the worker raises `WatermarkConcurrencyError` and refuses to overwrite the newer state.

---

## 8. Inherent Limitations of Watermark Ingestion (Why CDC is Required)

While watermark incremental ingestion is efficient and lightweight, it has three fundamental architectural limitations:

### A. The Physical Delete Blind Spot
When a row is hard-deleted from a database (`DELETE FROM payments WHERE payment_id = 'PAY-0002'`), the row ceases to exist on storage pages.
- The incremental query `WHERE updated_at > :last_watermark` scans existing rows only.
- The query returns $0$ rows. The downstream lakehouse continues storing the deleted record indefinitely.
- **CDC Solution**: CDC replicates the database transaction log (WAL/Binlog), capturing explicit `DELETE` operations and transmitting them to downstream tables.

### B. Intermediate Mutation Loss
If an account transitions `ACTIVE` $\rightarrow$ `SUSPENDED` $\rightarrow$ `ACTIVE` between hourly polling runs:
- Watermark extraction only captures the latest state (`ACTIVE`).
- The intermediate suspension event and duration are completely lost to analytics.
- **CDC Solution**: Every atomic state transition committed to the transaction log is emitted as an individual change event.

### C. Backdated Timestamp Anomalies
If a legacy application or bug updates a record with a backdated timestamp:
$$\text{record.updated\_at} \le \text{last\_committed\_watermark}$$
- The watermark query will completely miss the record.
- **CDC Solution**: CDC orders events by physical transaction commit sequence (LSN / Sequence Number), which is strictly monotonic regardless of application-level timestamp values.
