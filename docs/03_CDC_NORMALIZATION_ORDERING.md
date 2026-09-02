# 03 — CDC Normalization, Ordering, Deduplication & Quarantine

> **Learning Status**: `NOT STUDIED / PENDING`  
> *Track your conceptual mastery by updating this status as you study each foundation.*

---

## 1. Raw Ingestion vs. Canonical Normalization

In modern event-driven architectures, Change Data Capture (CDC) events emitted from transactional source databases (e.g., PostgreSQL WAL, MySQL Binlog, DynamoDB Streams) arrive through message brokers and landing storage in an **at-least-once, out-of-order, and potentially corrupted state**.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                   RAW CDC LANDING                                      │
│                                                                                        │
│  • Duplicates / Replays            • Missing Keys / Bad Data     • Out-of-Order Sequences │
│  • Conflicting Event IDs           • Malformed JSON lines        • Late Arrivals          │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
                       ┌─────────────────────────────────────────┐
                       │        CDCNormalizationPipeline         │
                       │          (PySpark 3.5 Engine)           │
                       └────────────────────┬────────────────────┘
                                            │
                    ┌───────────────────────┴───────────────────────┐
                    ▼                                               ▼
     ┌─────────────────────────────┐                 ┌─────────────────────────────┐
     │   data/normalized_cdc/      │                 │      data/quarantine/       │
     │  processing_id=<id>/        │                 │    processing_id=<id>/      │
     │       accepted.jsonl        │                 │      quarantine.jsonl       │
     ├─────────────────────────────┤                 ├─────────────────────────────┤
     │ • Authoritative Sequence    │                 │ • Explicit Reason Codes     │
     │ • Exact Duplicates Dropped  │                 │ • Preserved Raw Line/Record │
     │ • Replay-Safe Fingerprints  │                 │ • Malformed JSON & Conflicts│
     │ • Portable Logical Paths    │                 │ • Deterministic Tie-Breaking│
     └─────────────────────────────┘                 └─────────────────────────────┘
```

The fundamental purpose of Module 3 is to transform raw CDC files into a trustworthy canonical stream by answering:
> **Given a dirty CDC event stream, which events are safe to apply downstream, in what order, and which events must be quarantined?**

---

## 2. At-Least-Once Delivery & Deduplication

Distributed event streaming platforms (Kafka, Event Hubs, SQS) guarantee **at-least-once delivery**. A network retry, producer re-send, or consumer restart frequently redelivers events.

### Exact Duplicate Deliveries vs. Conflicting Event IDs

```
                                  Incoming Event
                                  (event_id: E1)
                                        │
                    ┌───────────────────┴───────────────────┐
                    ▼                                       ▼
      Same Content / Fingerprint               Different Content / Payload
     ┌───────────────────────────┐            ┌───────────────────────────┐
     │  Exact Duplicate Delivery │            │ Duplicate Event Conflict  │
     ├───────────────────────────┤            ├───────────────────────────┤
     │ • Keep exactly 1 copy     │            │ • Quarantine ALL copies   │
     │ • Deterministic winner    │            │ • Reason:                 │
     │ • Drop additional replays │            │   DUPLICATE_EVENT_CONFLICT│
     └───────────────────────────┘            └───────────────────────────┘
```

1. **Exact Duplicate Delivery**:
   - Multiple records share the same `event_id` and the **identical SHA-256 event fingerprint**.
   - **Deterministic Representative Selection**: Selected via stable provenance ordering: `batch_id ASC`, `ingestion_batch_id ASC`, `source_file ASC`, and `ingestion_order ASC`.
   - **Resolution**: Keep the deterministic winner (`row_number() == 1`), drop redundant copies, and track `exact_duplicates_dropped`.
2. **Conflicting Duplicate Event ID**:
   - Multiple records share the same `event_id` but have **different fingerprints** (e.g., payload, operation, or sequence changed).
   - **Resolution**: Do NOT arbitrarily pick a winner. Quarantine **all conflicting records** under `DUPLICATE_EVENT_CONFLICT` to prevent silent state corruption.

---

## 3. Authoritative Sequence Ordering vs. Timestamp Ordering

### The Fallacy of Physical Timestamp Ordering
Using `event_timestamp` or `source_commit_timestamp` to order change events is dangerous:
- **Clock Drift**: Distributed application servers experience millisecond to second clock offsets (NTP skews).
- **Batch Commits**: Multiple mutations committed in the same millisecond share identical timestamps.
- **Backdated Edits**: Historical corrections produce backdated timestamps.

### Authoritative Sequence Monotonicity
For this platform, change events carry a strictly monotonic integer `sequence_number` scoped to each entity's namespace (`entity_sequence_key = table_name:business_key_canonical`).

```
Landing Arrival:     [ Seq 102 (status: TRIAL) ]  ──►  [ Seq 101 (country: GB) ]
                                      │
                                      ▼  (PySpark Window Sort)
Normalized Stream:   [ Seq 101 (country: GB) ]    ──►  [ Seq 102 (status: TRIAL) ]
```

- **Out-of-Order Normalization**: If Sequence 102 arrives before Sequence 101, the normalization engine orders them deterministically as `101 -> 102`.
- **Cross-Entity Sequence Independence**: `ACC-0001` at sequence 10 and `ACC-0002` at sequence 10 are completely independent and valid.
- **Equal-Sequence Conflicts**: If two distinct events share the same `(entity_sequence_key, sequence_number)`, the engine quarantines both under `SEQUENCE_CONFLICT`.

---

## 4. Ingestion-Context Late-Arriving Events

A late-arriving event is defined strictly through **deterministic ingestion context and timestamp boundaries**, rather than substring name heuristics:

1. Input ingestion batches are ordered deterministically by `ingestion_batch_id` (e.g. `batch_001` precedes `batch_002`).
2. For each batch $B_i$ in sorted batch order:
   - Calculate $\text{prior\_max\_ts} = \max(\text{event\_timestamp})$ across all valid records in batches $B_0, \dots, B_{i-1}$.
   - An event in batch $B_i$ ($i > 0$) is late when $\text{event\_timestamp} < \text{prior\_max\_ts}$ (strict $<$ comparison).
   - Events in the first batch ($B_0$) are never marked late.
3. **Handling**: Preserved in the accepted normalized stream and tagged with `is_late_arrival = True`.

---

## 5. Portable Canonical Manifest & Processing Identity

### Machine-Independent Content Addressing
To guarantee identical `processing_id` across different host machines, absolute paths (`/Users/...`, `/tmp/...`) are replaced by portable logical file IDs and raw byte digests:

1. **Logical File ID**: `batch_id=<batch-id>/<filename>` (e.g. `batch_id=batch_001/accounts.jsonl`).
2. **Raw Byte Digest**: `sha256(raw_file_bytes)`.
3. **Canonical Manifest**: Sorted list of entries `f"{logical_file_id}:{file_sha256}"`.
4. **Processing ID**: `proc_<sha256(manifest)[:16]>`.

```
Manifest:
  batch_id=batch_001/accounts.jsonl:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
  batch_id=batch_001/subscriptions.jsonl:a2c1...
                       │
                       ▼  (SHA-256 Digest)
         processing_id = proc_84886e920983
```

- **Invariance**: Identical files in different temporary directories produce the **exact same `processing_id`**.
- **Tamper Detection**: Modifying a single byte in any raw input file produces a **different `processing_id`**.

---

## 6. Separation of Deterministic Output & Audit Execution

Volatile execution timestamps (`normalized_at`, `quarantined_at`, `started_at`, `completed_at`, `run_id`) are decoupled from persistent logical files:
- **`accepted.jsonl`** and **`quarantine.jsonl`**: Contain only deterministic logical records and portable logical provenance (`source_file = batch_id=...`). Replaying the same inputs produces **byte-for-byte identical files**.
- **`NormalizationAuditMetrics`**: Captures operational run metrics (`run_id`, `started_at`, `completed_at`, `raw_records_seen`, `accepted_records`, `exact_duplicates_dropped`, `quarantined_records`).

---

## 7. Machine-Readable Quarantine Taxonomy

| Quarantine Code | Category | Trigger Condition |
| :--- | :--- | :--- |
| `MALFORMED_JSON` | Syntactic | Line cannot be parsed as a valid JSON object. |
| `MISSING_EVENT_ID` | Structural | Missing or empty `event_id`. |
| `UNKNOWN_TABLE` | Structural | Table name not in recognized schema registry. |
| `UNSUPPORTED_OPERATION` | Semantic | Operation not in `INSERT`, `UPDATE`, `DELETE`. |
| `MISSING_BUSINESS_KEY` | Structural | Missing or empty `business_key` dictionary. |
| `INVALID_BUSINESS_KEY` | Structural | Business key missing table's defined primary key. |
| `MISSING_SEQUENCE` | Structural | Missing `sequence_number`. |
| `INVALID_SEQUENCE` | Semantic | Sequence number $\le 0$ or non-integer. |
| `MISSING_EVENT_TIMESTAMP` | Structural | Missing or empty `event_timestamp`. |
| `INVALID_EVENT_TIMESTAMP` | Semantic | `event_timestamp` string cannot be parsed as valid ISO 8601 UTC timestamp. |
| `MISSING_COMMIT_TIMESTAMP` | Structural | Missing or empty `source_commit_timestamp`. |
| `INVALID_COMMIT_TIMESTAMP` | Semantic | `source_commit_timestamp` string cannot be parsed as valid ISO 8601 UTC timestamp. |
| `MISSING_SOURCE_SYSTEM` | Structural | Missing `source_system`. |
| `MISSING_PAYLOAD` | Semantic | `INSERT` or `UPDATE` missing after-image `payload`. |
| `MISSING_BEFORE_IMAGE` | Semantic | `UPDATE` or `DELETE` missing before-image `before_payload`. |
| `UNEXPECTED_DELETE_PAYLOAD` | Semantic | `DELETE` containing non-null after-image `payload`. |
| `BUSINESS_KEY_PAYLOAD_MISMATCH` | Consistency | Missing or mismatched primary key in present `payload` or `before_payload`. |
| `DUPLICATE_EVENT_CONFLICT` | Conflict | Same `event_id` appears with conflicting payloads. |
| `SEQUENCE_CONFLICT` | Conflict | Same entity has multiple distinct events at same sequence. |

---

## 8. Replay Determinism & Audit Reconciliation

### Audit Invariant Reconciliation
Every run produces an audit metrics object satisfying:
$$\text{raw\_records\_seen} = \text{accepted\_records} + \text{exact\_duplicates\_dropped} + \text{quarantined\_records}$$

---

## 9. Why Normalization Precedes Delta Lake MERGE (Module 4 Handoff)

Directly executing Delta Lake `MERGE` against raw CDC streams causes severe data corruption:
1. **Unordered Merges**: Applying sequence 102 before sequence 101 overwrites final state with stale data.
2. **Ambiguous Conflicts**: Conflicting duplicate events cause non-deterministic row updates.
3. **Corrupt Payloads**: Malformed rows break column types and abort long-running ACID transactions.

By inserting the **CDC Normalization Engine** between raw landing and the silver layer, Module 4 can safely execute deterministic Delta Lake `MERGE` operations on a pristine, pre-ordered stream.
