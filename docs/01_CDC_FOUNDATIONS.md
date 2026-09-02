# 01 — CDC & Incremental Processing Foundations

> **Learning Status**: `NOT STUDIED / PENDING`  
> *Track your conceptual mastery by updating this status as you study each foundation.*

---

## 1. Full vs. Incremental Ingestion

### Full Ingestion (Snapshot Reload)
In a full ingestion model, the entire dataset is queried and rewritten from source to target during every pipeline cycle:
$$\text{Cost} = O(N) \quad \text{where } N = \text{total rows in table}$$

- **Pros**: Simplest to implement, self-healing against missing updates or deletes.
- **Cons**: 
  - Massive data transfer overhead and I/O saturation.
  - Compute costs scale linearly with database growth.
  - Source database experiences heavy read locks and I/O contention.
  - Ingestion batch windows expand until SLAs are breached.

### Incremental Ingestion (Delta Processing)
Incremental ingestion queries or streams only records modified since the previous synchronization point:
$$\text{Cost} = O(\Delta) \quad \text{where } \Delta \ll N$$

- **Pros**: Minimal compute, sub-minute latency, low source impact.
- **Cons**: Requires explicit change-tracking primitives (watermarks or change logs), ordering enforcement, deduplication, and delete reconciliation.

---

## 2. Watermark Processing vs. Change Data Capture (CDC)

| Dimension | High-Watermark Ingestion | Change Data Capture (CDC) |
| :--- | :--- | :--- |
| **Mechanism** | `SELECT * WHERE updated_at > :last_watermark` | Transaction log streaming (WAL / Binlog / Oplog) |
| **Granularity** | Periodic polling interval (e.g., hourly/daily) | Real-time / Micro-batch change stream |
| **Intermediate States** | Collapses intermediate mutations; only sees latest state | Captures every atomic state transition |
| **Physical Deletes** | ❌ **Invisible** (row is gone; cannot match query) | ✅ **Captured** (explicit `DELETE` event with before-image) |
| **Source Impact** | Table scans / index scans on timestamp columns | Read-only stream from binary replication log |
| **Complexity** | Low | Moderate to High |

---

## 3. The Physical Delete Blind Spot

Why can watermark queries never detect physical deletes?

Consider an account record:
```sql
-- Initial state
INSERT INTO accounts (account_id, status, updated_at) 
VALUES ('ACC-001', 'ACTIVE', '2026-01-01 10:00:00');

-- Later in the day, record is hard-deleted:
DELETE FROM accounts WHERE account_id = 'ACC-001';
```

When an incremental query runs at `2026-01-01 12:00:00` with watermark `2026-01-01 09:00:00`:
```sql
SELECT * FROM accounts WHERE updated_at > '2026-01-01 09:00:00';
```
The row `ACC-001` no longer exists on disk in PostgreSQL/MySQL. The query returns $0$ rows. The downstream Lakehouse continues to store `ACC-001` indefinitely, resulting in silent data corruption.

### Solutions:
1. **Change Data Capture (CDC)**: Transaction log captures `DELETE` operations and emits change events containing the deleted primary key.
2. **Soft Deletes**: Applications set `is_deleted = TRUE` and `updated_at = NOW()`, allowing watermark queries to capture the change before eventual archival.
3. **Full Snapshot Differential Reconciliation**: Periodic reconciliation job comparing primary key sets.

---

## 4. Change Event Anatomy: Before-Images & After-Images

A robust CDC event contract provides complete auditability and downstream reconciliation guarantees.

```
                  ┌──────────────────────────────────────────────┐
                  │                 CDC Event                    │
                  ├──────────────────────────────────────────────┤
                  │ • event_id: "evt_982734"                     │
                  │ • table_name: "subscriptions"                │
                  │ • operation: "UPDATE"                        │
                  │ • business_key: {"subscription_id": "S-01"}  │
                  │ • sequence_number: 1420                      │
                  │ • event_timestamp: "2026-04-01T10:00:00Z"    │
                  │ • source_commit_timestamp: "2026-04-01T..."  │
                  └───────────────┬──────────────────────────────┘
                                  │
                 ┌────────────────┴────────────────┐
                 ▼                                 ▼
      ┌─────────────────────┐           ┌─────────────────────┐
      │    before_payload   │           │       payload       │
      │    (Before-Image)   │           │    (After-Image)    │
      ├─────────────────────┤           ├─────────────────────┤
      │ plan: "STARTER"     │   ───►    │ plan: "ENTERPRISE"  │
      │ amount: 49.00       │           │ amount: 1299.00     │
      └─────────────────────┘           └─────────────────────┘
```

### Operation Semantics:
- **`INSERT`**: `payload` contains the initial row state; `before_payload` is `NULL`.
- **`UPDATE`**: `payload` contains the new state; `before_payload` contains the prior state.
- **`DELETE`**: `payload` is `NULL`; `before_payload` contains the record state immediately prior to deletion.

---

## 5. Event Sequencing & Ordering Contracts

### Why Event Timestamps Cannot Alone Determine Ordering
1. **Clock Drift / Skew**: Source application servers across clusters experience minor clock drift (NTP synchronization variations).
2. **Network Jitter & Retries**: An event generated at $t_1$ may be retried and land in the buffer after an event generated at $t_2$.
3. **Sub-millisecond Concurrency**: Multiple updates to the same record within the same millisecond share identical timestamps.

### The Authoritative Mechanism: `sequence_number`
- **Log Sequence Numbers (LSN)** or per-entity sequence counters provide a strict, monotonically increasing total order per business key.
- Downstream merge engines must prioritize `sequence_number` over landing order or client timestamps:
$$\text{State}(K) = \text{Payload}\left(\arg\max_{e \in \text{Events}(K)} e.\text{sequence\_number}\right)$$
- **Strict Monotonicity Rule**: If an incoming event has a sequence number less than or equal to the highest applied sequence for that business key (`sequence_number <= current_max_sequence`), it is rejected as stale/non-monotonic.
- **No Timestamp Tiebreaker**: `event_timestamp` must NEVER be used to pick a winner between conflicting equal-sequence records. A deterministic transactional pipeline treats duplicate or equal sequences as non-monotonic.

---

## 6. Real-World Streaming Anomalies

### A. Duplicate Delivery (At-Least-Once Delivery)
Message queues (Kafka, Event Hubs, SQS) guarantee *at-least-once* delivery. Network retries cause identical `event_id` records to appear multiple times.
- **Remedy**: Idempotent processing where duplicate `event_id` records or stale sequence numbers are discarded without state alteration.

### B. Out-of-Order Delivery
Due to multi-partition ingestion or concurrent retry workers, Sequence $102$ may arrive before Sequence $101$.
- **Remedy**: Sequence-aware state engines buffer or evaluate sequence numbers. Applying a stale sequence ($101$) after a newer sequence ($102$) has already been merged must be a no-op.

### C. Late-Arriving Data
An event generated hours or days in the past arrives in the current micro-batch.
- **Remedy**: Watermark thresholds and sequence comparisons ensure historical updates are applied only if they represent a newer sequence for the given business key, or stored in history tables.

---

## 7. Idempotency and Replayability

### Idempotency
An operation is idempotent if executing it once has the exact same effect as executing it multiple times:
$$f(f(x)) = f(x)$$

In CDC pipelines, applying a batch of change events multiple times must produce identical target table contents.

### Deterministic Replay
If a downstream corruption occurs or business logic changes:
1. Truncate target tables.
2. Reset pipeline checkpoint to offset $0$ / snapshot timestamp.
3. Replay CDC events in sequence order.
4. Target table converges to the identical mathematical state.

---

## 8. Checkpoints and State Consistency

- **Checkpointing**: Persisting the exact offset / watermark / sequence number successfully processed by the pipeline.
- **Atomic Commits**: Target data writes and checkpoint updates must occur in a single atomic transaction (e.g., Delta Lake ACID transactions) to eliminate partial failure states.

---

## 9. Exactly-Once vs. Effectively-Once Processing

- **True Exactly-Once**: End-to-end distributed 2-phase commit across message broker, compute engine, and storage. Rare and computationally expensive.
- **Effectively-Once (At-Least-Once + Idempotent Sink)**: The standard architecture in modern data engineering. Messages may be read multiple times, but deterministic deduplication and Delta Lake `MERGE` guarantee the target state is identical to an exactly-once execution.
