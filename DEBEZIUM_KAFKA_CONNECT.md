# Debezium & Kafka Connect — Deep Implementation Reference

---

## 1. How PostgreSQL Logical Replication Works

### Write-Ahead Log (WAL)

PostgreSQL writes every committed change to the WAL before touching the actual data files. The WAL is an append-only sequence of binary records that guarantees durability — if the server crashes mid-write, it replays the WAL on restart.

```
Transaction commits:
  INSERT INTO source.customers ...
  UPDATE source.orders ...
  DELETE FROM source.products ...
       │
       ▼
   WAL file (binary log of all changes)
       │
       ├──► Physical replication (streaming replica — block-level copy)
       └──► Logical replication (decoded row events — what Debezium uses)
```

**Logical replication** decodes the binary WAL records into structured row-level events. Unlike physical replication (which copies raw disk pages), logical replication gives you INSERT/UPDATE/DELETE with before/after values, making it suitable for CDC.

### Enabling Logical Replication on RDS

Three RDS parameter group settings are required:

```
rds.logical_replication = 1       # enables WAL level = logical
max_replication_slots   = 5       # max concurrent CDC consumers
max_wal_senders         = 5       # max WAL sender processes
wal_sender_timeout      = 0       # never kill idle replication connections
```

Without `rds.logical_replication=1`, the WAL is written at `replica` level (block-level only) and logical decoding is unavailable.

### Publications

A **publication** is a named set of tables whose changes flow into the logical replication stream. You create it once:

```sql
CREATE PUBLICATION poc_publication
  FOR TABLE source.customers, source.products, source.orders,
            source.order_items, source.inventory;
```

Each publication can control which operations to include:

```sql
-- All operations (default)
CREATE PUBLICATION full_pub FOR TABLE source.customers;

-- Only inserts and updates (no deletes)
CREATE PUBLICATION inserts_only FOR TABLE source.customers
  WITH (publish = 'insert, update');
```

Debezium connects to this publication via the `publication.name` config. If the publication doesn't exist, Debezium tries to create it — which requires superuser. Pre-creating it (as we do) is safer.

### Replication Slots

A **replication slot** is a cursor into the WAL stream. PostgreSQL tracks the position of each slot and guarantees that WAL segments needed by any active slot are not deleted, even if they're old.

```sql
-- View all slots
SELECT slot_name, plugin, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots;
```

| Field | Meaning |
|---|---|
| `slot_name` | Unique name — must match `slot.name` in Debezium config |
| `plugin` | Logical decoding plugin (`pgoutput` in our case) |
| `active` | True when Debezium is connected |
| `restart_lsn` | Oldest WAL position the slot needs — WAL before this can be recycled |
| `confirmed_flush_lsn` | Last position Debezium confirmed processing |

**Critical:** An inactive slot with a stale `restart_lsn` will cause WAL accumulation and eventually fill up disk. Always drop slots when decommissioning a connector:

```sql
SELECT pg_drop_replication_slot('poc4_debezium_slot');
```

### pgoutput Plugin

`pgoutput` is the built-in PostgreSQL logical decoding plugin (available since PG 10). It decodes WAL records into a text protocol that Debezium understands. The alternative (`decoderbufs`) requires a server extension and is not available on RDS.

```
Debezium config:  "plugin.name": "pgoutput"
```

---

## 2. Debezium PostgreSQL Connector

### How Debezium Reads Changes

```
PostgreSQL WAL
     │
     │  replication protocol (streaming replication connection)
     ▼
Debezium ──── pg_replication_slot (poc4_debezium_slot)
     │
     │  logical decoding via pgoutput
     │  produces row-level events: INSERT / UPDATE / DELETE
     ▼
Kafka Connect framework
     │
     │  serializes to JSON (or Avro) + publishes
     ▼
Kafka topic: poc4.source.<table>
```

Debezium operates in two sequential phases:

### Phase 1: Initial Snapshot

When the connector starts with a new replication slot, it first takes a consistent snapshot of all tables in the publication. During snapshot:

1. Acquires an ACCESS SHARE lock (non-blocking for reads/writes)
2. Records the current WAL LSN (Log Sequence Number)
3. SELECTs all rows from each table and emits a `READ` event per row
4. Releases the lock and switches to streaming from the recorded LSN

The snapshot ensures the Kafka topic starts with the full table state, not just future changes.

```
Snapshot event structure:
{
  "op": "r",           ← "read" (snapshot)
  "before": null,
  "after": { ...row... },
  "source": { "snapshot": "true", ... }
}
```

### Phase 2: Streaming (Change Data Capture)

After snapshot, Debezium tails the replication slot in real time. Every committed transaction produces events:

```
INSERT → op: "c" (create)
UPDATE → op: "u" (update)   ← includes "before" and "after" values
DELETE → op: "d" (delete)   ← includes "before" value only
```

**Full event envelope example (UPDATE):**
```json
{
  "schema": { ... },
  "payload": {
    "before": {
      "id": 42,
      "status": "pending",
      "updated_at": 1778950000000
    },
    "after": {
      "id": 42,
      "status": "delivered",
      "updated_at": 1778951000000
    },
    "source": {
      "version": "3.2.6.Final",
      "connector": "postgresql",
      "name": "poc4",
      "db": "pocdb",
      "schema": "source",
      "table": "orders",
      "lsn": 29884416,
      "txId": 845
    },
    "op": "u",
    "ts_ms": 1778951234567
  }
}
```

### ExtractNewRecordState SMT (Unwrap)

The raw Debezium envelope (before/after/op) is too complex for a simple JDBC sink. The `ExtractNewRecordState` Single Message Transform flattens it:

```json
Connector config:
{
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.drop.tombstones": "false",
  "transforms.unwrap.delete.handling.mode": "rewrite"
}
```

**What it does:**

| Input (raw envelope) | Output (after unwrap) |
|---|---|
| INSERT `op=c`, `after={...}` | The `after` row directly |
| UPDATE `op=u`, `before={...}`, `after={...}` | The `after` row directly |
| DELETE `op=d`, `before={...}` | A message with `__deleted=true` field + all `before` values |
| Tombstone (null value) | Passed through (because `drop.tombstones=false`) |

`delete.handling.mode=rewrite` turns deletes into a flat message with `__deleted=true`. This lets the JDBC sink recognize them as deletes (via `delete.enabled=true`) rather than ignoring them.

### Message Key

The Kafka message key contains the primary key of the row, serialized as JSON:

```json
{
  "schema": {
    "type": "struct",
    "fields": [{"field": "id", "type": "int32"}],
    "name": "poc4.source.customers.Key"
  },
  "payload": {"id": 42}
}
```

The JDBC sink reads this key (`pk.mode=record_key`, `pk.fields=id`) to know which column to use for `ON CONFLICT`.

### Timestamp Handling — The Critical Detail

This is the most common source of Debezium → JDBC Sink failures.

| Column type | temporal.precision.mode | Debezium emits | Kafka schema type |
|---|---|---|---|
| `TIMESTAMPTZ` | any | ISO-8601 string `"2026-05-16T01:00:00Z"` | `io.debezium.time.ZonedTimestamp` (string) |
| `TIMESTAMP(6)` (default) | `connect` | Microseconds since epoch (int64) | `io.debezium.time.MicroTimestamp` |
| `TIMESTAMP(3)` | `connect` | Milliseconds since epoch (int64) | `org.apache.kafka.connect.data.Timestamp` |

**Rule:** Use `TIMESTAMP(3)` in source schema + `temporal.precision.mode=connect` to get standard Kafka Connect Timestamp type. The JDBC sink (Confluent 10.x) binds this as `::int8` regardless, so the target column must be `BIGINT` to accept it without a cast.

**To view timestamps in the target:**
```sql
SELECT id, to_timestamp(order_date / 1000.0) AS order_date_readable
FROM target.orders;
```

### Heartbeat

```json
"heartbeat.interval.ms": "10000"
```

On idle databases with no changes, Debezium sends periodic heartbeat events to advance the replication slot's `confirmed_flush_lsn`. Without heartbeats, the slot stays at its last-seen LSN and WAL accumulates indefinitely on the RDS instance.

---

## 3. Kafka Connect Framework

### Architecture

```
MSK Connect Worker (JVM process)
├── Connector (configuration + lifecycle)
│     └── manages N Tasks
└── Task (actual data movement thread)
      ├── Source Task: polls external system, produces Kafka records
      └── Sink Task: consumes Kafka records, writes to external system
```

A **Connector** is the configuration and management layer. A **Task** is the execution unit that actually moves data. MSK Connect autoscales workers (JVMs) and distributes tasks across them.

### Connector Lifecycle

```
CREATE connector
    │
    ▼
CREATING (MSK Connect provisions workers)
    │
    ▼
RUNNING ──── Task RUNNING ──── data flowing normally
    │              │
    │         Task FAILED ──── unrecoverable error (connector shows RUNNING!)
    │
    ▼
FAILED (connector-level failure — MSK could not provision)
    │
    ▼
DELETING → DELETED
```

**Important:** A connector in `RUNNING` state can have a `FAILED` task. Always check task-level state, not just connector state. MSK does not expose a task restart API — you must delete and recreate the connector.

### Converters

Converters serialize/deserialize data between Kafka (bytes) and Kafka Connect (Java objects with schemas).

```json
"key.converter": "org.apache.kafka.connect.json.JsonConverter",
"key.converter.schemas.enable": "true",
"value.converter": "org.apache.kafka.connect.json.JsonConverter",
"value.converter.schemas.enable": "true"
```

With `schemas.enable=true`, each Kafka message contains both a schema and a payload:

```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {"field": "id",     "type": "int32"},
      {"field": "name",   "type": "string"},
      {"field": "status", "type": "string"}
    ]
  },
  "payload": {
    "id": 42,
    "name": "Kimberly Lane",
    "status": "active"
  }
}
```

The schema is embedded in every message. This is verbose but requires no external schema registry. For production, Avro + Confluent Schema Registry is more efficient.

### Single Message Transforms (SMTs)

SMTs are applied in the connector worker process, before records reach Kafka (source) or the target system (sink).

**Source connector SMTs (applied after Debezium produces the record):**
```json
"transforms": "unwrap",
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
```

**Sink connector SMTs (applied before JDBC writes the record):**
```json
"transforms": "routeToTable",
"transforms.routeToTable.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.routeToTable.regex": "poc4\\.source\\.(.*)",
"transforms.routeToTable.replacement": "$1"
```

The `RegexRouter` strips the `poc4.source.` prefix from the topic name so `poc4.source.customers` routes to the `customers` table (resolved against `currentSchema=target` in the JDBC URL).

### MSK Connect Specifics

**`connector.client.config.override.policy = None`** (hardcoded by AWS)

MSK Connect sets this policy on every connector and it cannot be changed. It blocks all `consumer.override.*` and `producer.override.*` settings. In practice this means:
- `consumer.override.auto.offset.reset=latest` is silently rejected
- Sink connectors always start from `auto.offset.reset=earliest` (Kafka's default)
- To skip old messages, use a new topic prefix (which creates new topics) or a new consumer group

**Plugin delivery via S3**

MSK Connect does not pull plugins from the internet at runtime. Plugins must be uploaded as ZIPs to S3 before the connector is created. The ZIP must contain the connector JARs in the expected Confluent Hub layout.

**No REST API**

Self-managed Kafka Connect exposes a REST API (`POST /connectors`, `GET /connectors/{name}/status`, `POST /connectors/{name}/tasks/{taskId}/restart`). MSK Connect has no equivalent REST endpoint — all management is via `aws kafkaconnect` CLI or the AWS Console.

### Consumer Group Offsets

The JDBC sink connector uses a Kafka consumer group to track which messages have been processed. The group ID is `connect-<connector-name>` (e.g., `connect-poc-jdbc-sink-v4`).

Offsets are committed to the `__consumer_offsets` internal topic after each batch of records is successfully written to the target. If the connector crashes before committing, it re-processes records from the last committed offset on restart (at-least-once delivery).

---

## 4. Confluent JDBC Sink Connector

### Upsert Mode

```json
"insert.mode": "upsert",
"pk.mode": "record_key",
"pk.fields": "id"
```

Generates for each record:
```sql
INSERT INTO "pocdb"."target"."customers"
  ("id", "name", "email", "status", "created_at", "updated_at")
VALUES (...)
ON CONFLICT ("id")
DO UPDATE SET
  "name" = EXCLUDED."name",
  "email" = EXCLUDED."email",
  "status" = EXCLUDED."status",
  ...
```

This handles INSERT, UPDATE, and snapshot `READ` events identically — they all upsert by `id`. The target row count never exceeds the source row count.

**Requirement:** The target table must have a PRIMARY KEY or UNIQUE constraint on `id` for `ON CONFLICT` to work. `CREATE TABLE ... LIKE source INCLUDING ALL` copies the backing unique index but not the PRIMARY KEY constraint declaration — add it explicitly:

```sql
ALTER TABLE target.customers    ADD PRIMARY KEY (id);
ALTER TABLE target.products     ADD PRIMARY KEY (id);
ALTER TABLE target.orders       ADD PRIMARY KEY (id);
ALTER TABLE target.order_items  ADD PRIMARY KEY (id);
ALTER TABLE target.inventory    ADD PRIMARY KEY (id);
```

### Delete Propagation

```json
"delete.enabled": "true"
```

When the JDBC sink receives a tombstone message (null value, key present) or a message with `__deleted=true` (from `delete.handling.mode=rewrite`), it issues:

```sql
DELETE FROM "pocdb"."target"."customers" WHERE "id" = 42
```

### PostgreSQL Dialect

```json
"dialect.name": "PostgreSqlDatabaseDialect"
```

Must be set explicitly. Without it, JDBC Sink 10.9.3 fails with `Unable to find dialect with name ''`. The PostgreSQL dialect handles:
- `ON CONFLICT ... DO UPDATE` syntax (instead of MySQL's `ON DUPLICATE KEY`)
- Correct casting of numeric types
- Schema-qualified table names

---

## 5. Data Flow — Message by Message

Here is what happens when you run `INSERT INTO source.customers ...`:

```
1. PostgreSQL commits the INSERT
2. WAL record written: {table=customers, op=INSERT, after={id=1, name="Alice", ...}}
3. Debezium reads WAL via replication slot poc4_debezium_slot
4. Debezium wraps in envelope:
   {op:"c", before:null, after:{id:1, name:"Alice",...}, source:{...}}
5. ExtractNewRecordState SMT unwraps → {id:1, name:"Alice", status:"active", ...}
6. JsonConverter serializes to bytes
7. Published to Kafka topic: poc4.source.customers (partition determined by key hash)
8. JDBC Sink consumer group reads from poc4.source.customers
9. RegexRouter SMT strips prefix → table name: "customers"
10. Sink builds: INSERT INTO "pocdb"."target"."customers" (...) VALUES (...) ON CONFLICT (id) DO UPDATE SET ...
11. Executes against RDS target schema via JDBC
12. Consumer offset committed to __consumer_offsets
```

Total latency from step 1 to step 12: typically 2–8 seconds.
