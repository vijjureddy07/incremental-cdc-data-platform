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
     │ • Drop additional replays │            │ • Reason:                 │
     │ • Safe for downstream     │            │   DUPLICATE_EVENT_CONFLICT│
     └───────────────────────────┘            └───────────────────────────┘
```

1. **Exact Duplicate Delivery**:
   - Multiple records share the same `event_id` and the **identical SHA-256 event fingerprint**.
   - **Resolution**: Keep the first instance (`row_number() == 1`), drop redundant copies, and track `exact_duplicates_dropped`.
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

## 4. Late-Arriving Events

A late-arriving event is an event whose `event_timestamp` is historically older than the current ingestion batch, but whose structure and sequence number are valid.

- **Handling**: Preserved in the accepted normalized stream and tagged with `is_late_arrival = True`.
- **Downstream Application**: Module 4 MERGE logic uses the authoritative `sequence_number` to determine whether the late event represents a new state or has already been superseded by a higher sequence.

---

## 5. Canonical Keys & Deterministic Event Fingerprints

### Canonical Business Key Formatting
To prevent JSON dictionary key ordering differences from altering identity:
```json
{"region": "US", "account_id": "ACC-0001"}
```
is normalized into alphabetical, whitespace-free canonical JSON:
```json
{"account_id":"ACC-0001","region":"US"}
```

### Deterministic SHA-256 Event Fingerprint
Computed across stable semantic fields:
```
sha256(event_id | table_name | operation | canonical_key | sequence | event_ts | commit_ts | canonical_payload | canonical_before | source_system)
```
Excluded: transient ingestion metadata (`source_file`, `ingestion_batch_id`, `ingestion_order`, Spark partition IDs).

---

## 6. Machine-Readable Quarantine Taxonomy

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
| `MISSING_COMMIT_TIMESTAMP` | Structural | Missing or empty `source_commit_timestamp`. |
| `MISSING_SOURCE_SYSTEM` | Structural | Missing `source_system`. |
| `MISSING_PAYLOAD` | Semantic | `INSERT` or `UPDATE` missing after-image `payload`. |
| `MISSING_BEFORE_IMAGE` | Semantic | `UPDATE` or `DELETE` missing before-image `before_payload`. |
| `UNEXPECTED_DELETE_PAYLOAD` | Semantic | `DELETE` containing non-null after-image `payload`. |
| `BUSINESS_KEY_PAYLOAD_MISMATCH` | Consistency | Primary key in payload does not match `business_key`. |
| `DUPLICATE_EVENT_CONFLICT` | Conflict | Same `event_id` appears with conflicting payloads. |
| `SEQUENCE_CONFLICT` | Conflict | Same entity has multiple distinct events at same sequence. |

---

## 7. Replay Determinism & Audit Reconciliation

### Deterministic Processing Identity (`processing_id`)
Generated via SHA-256 across sorted input file paths and contents (`proc_<sha256[:16]>`). Replaying the pipeline over the exact same input batch produces the identical `processing_id` and atomic overwrites.

### Audit Invariant Reconciliation
Every run produces an audit metrics object satisfying:
$$\text{raw\_records\_seen} = \text{accepted\_records} + \text{exact\_duplicates\_dropped} + \text{quarantined\_records}$$

---

## 8. Why Normalization Precedes Delta Lake MERGE (Module 4 Handoff)

Directly executing Delta Lake `MERGE` against raw CDC streams causes severe data corruption:
1. **Unordered Merges**: Applying sequence 102 before sequence 101 overwrites final state with stale data.
2. **Ambiguous Conflicts**: Conflicting duplicate events cause non-deterministic row updates.
3. **Corrupt Payloads**: Malformed rows break column types and abort long-running ACID transactions.

By inserting the **CDC Normalization Engine** between raw landing and the silver layer, Module 4 can safely execute deterministic Delta Lake `MERGE` operations on a pristine, pre-ordered stream.
