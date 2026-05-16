# DataStream POC — Step-by-Step Implementation Guide

This guide walks through building the full CDC pipeline from scratch: RDS PostgreSQL (source) → Debezium on MSK Connect → Amazon MSK Kafka → Confluent JDBC Sink on MSK Connect → RDS PostgreSQL (target).

---

## Prerequisites

| Tool | Version | Install |
|---|---|---|
| Node.js | 18+ | `winget install OpenJS.NodeJS.LTS` |
| AWS CDK | 2.x | `npm install -g aws-cdk` |
| AWS CLI | v2 | `winget install Amazon.AWSCLI` |
| Python | 3.9+ | `winget install Python.Python.3` |
| jq | any | `winget install jqlang.jq` |
| Git Bash | any | included with Git for Windows |

**AWS credentials** must be configured with permissions to create VPC, RDS, MSK, IAM roles, S3, and CloudWatch resources:

```bash
aws configure
# or use SSO: aws sso login --profile <profile>
```

**Verify everything is accessible:**

```bash
aws sts get-caller-identity   # confirms credentials work
node --version                # 18+
cdk --version                 # 2.x
```

---

## Step 1 — Deploy CDK Infrastructure

The CDK project is in `cdk/`. It creates four stacks in dependency order: VPC → RDS → MSK → Connectors.

### 1a. Install CDK dependencies

```bash
cd cdk
npm install
```

### 1b. Bootstrap CDK (once per account/region)

CDK bootstrap creates the S3 bucket and IAM roles CDK needs to deploy assets:

```bash
cdk bootstrap aws://<ACCOUNT_ID>/us-east-1
```

### 1c. Find your public IP

The VPC stack optionally restricts RDS port 5432 to your IP only. Highly recommended:

```bash
curl -s ifconfig.me
# Example output: 203.0.113.42
```

### 1d. Deploy all stacks

```bash
cdk deploy --all --context devIp=<YOUR_IP>/32 --require-approval never
```

This takes 15–25 minutes. CDK deploys the four stacks:

**VpcStack** — VPC with public + private subnets across 2 AZs, NAT gateway, three security groups:
- `sg-rds`: port 5432 open to your IP + MSK Connect workers
- `sg-msk`: port 9092 (plaintext) from MSK Connect workers only
- `sg-msk-connect`: outbound to RDS and MSK

```typescript
// cdk/lib/vpc-stack.ts (key excerpt)
this.vpc = new ec2.Vpc(this, 'Vpc', {
  maxAzs: 2,
  natGateways: 1,
  subnetConfiguration: [
    { cidrMask: 24, name: 'Public',  subnetType: ec2.SubnetType.PUBLIC },
    { cidrMask: 24, name: 'Private', subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
  ],
});
```

**RdsStack** — Single `db.t3.micro` PostgreSQL 15 instance with a custom parameter group enabling logical replication:

```typescript
// cdk/lib/rds-stack.ts (key excerpt)
const parameterGroup = new rds.ParameterGroup(this, 'PgParams', {
  engine: rds.DatabaseInstanceEngine.postgres({ version: rds.PostgresEngineVersion.VER_15 }),
  parameters: {
    'rds.logical_replication': '1',   // enables WAL level = logical (required for Debezium)
    max_replication_slots: '5',
    max_wal_senders: '5',
    wal_sender_timeout: '0',          // never kill idle CDC connections
  },
});
```

**MskStack** — Provisioned MSK with `kafka.t3.small` brokers, Kafka 3.6.0, PLAINTEXT transport, auto-create topics enabled:

```typescript
// cdk/lib/msk-stack.ts (key excerpt)
this.cluster = new msk.CfnCluster(this, 'PocMsk', {
  kafkaVersion: '3.6.0',
  numberOfBrokerNodes: 2,
  brokerNodeGroupInfo: { instanceType: 'kafka.t3.small', ... },
  encryptionInfo: { encryptionInTransit: { clientBroker: 'PLAINTEXT' } },
  // MSK configuration includes: auto.create.topics.enable=true
});
```

**ConnectorsStack** — S3 plugin bucket, CloudWatch log group, IAM execution role for MSK Connect:

```typescript
// cdk/lib/connectors-stack.ts (key excerpt)
this.executionRole = new iam.Role(this, 'MskConnectRole', {
  assumedBy: new iam.ServicePrincipal('kafkaconnect.amazonaws.com'),
});
this.pluginBucket.grantRead(this.executionRole);
rdsStack.secret.grantRead(this.executionRole);
connectLogGroup.grantWrite(this.executionRole);
// + kafka-cluster:* actions on MSK + ec2:* for VPC networking
```

### 1e. Note the stack outputs

After deploy, CDK prints all stack outputs. Save these — the scripts read them from CloudFormation automatically, but you need them for local tool setup:

```
DataStreamRdsStack.DbEndpoint     = datastreamrdsstack-pocdb...rds.amazonaws.com
DataStreamRdsStack.DbSecretArn    = arn:aws:secretsmanager:us-east-1:...:secret:datastream-poc-db-secret-...
DataStreamConnectorsStack.PluginBucketName = datastream-poc-plugins-<account>-us-east-1
```

Set environment variables for the Python tools:

```bash
export AWS_REGION=us-east-1
export DB_HOST=<DbEndpoint from output>
export DB_SECRET_ARN=<DbSecretArn from output>
```

---

## Step 2 — Initialize the Database Schema

This step creates the `source` and `target` schemas with 5 tables each.

### Why two schemas in one database?

Debezium reads from `source`; the JDBC sink writes to `target`. Using one RDS instance cuts costs in half vs. two separate instances. The JDBC URL uses `currentSchema=target` to route all sink writes to the correct schema.

### 2a. Retrieve credentials

```bash
# Show username and password clearly
python show_credentials.py
```

Or from the AWS CLI:

```bash
aws secretsmanager get-secret-value \
  --secret-id "$DB_SECRET_ARN" \
  --query SecretString --output text | python -m json.tool
```

### 2b. Run the schema script

Using psql (if installed):

```bash
psql "host=$DB_HOST dbname=pocdb user=pocadmin password=<PASSWORD>" \
  -f sql/schema.sql
```

Using Python + psycopg2 (if psql is not available):

```bash
pip install psycopg2-binary boto3
python -c "
import os, json, boto3, psycopg2
secret = json.loads(boto3.client('secretsmanager', region_name='us-east-1')
  .get_secret_value(SecretId=os.environ['DB_SECRET_ARN'])['SecretString'])
conn = psycopg2.connect(host=os.environ['DB_HOST'], dbname='pocdb',
  user=secret['username'], password=secret['password'])
conn.autocommit = True
cur = conn.cursor()
with open('sql/schema.sql') as f:
    [cur.execute(s) for s in f.read().split(';') if s.strip() and not s.strip().startswith('\\\\')]
print('Schema created.')
"
```

### 2c. What the schema creates

**Source tables** — where application data is written, CDC reads from here:

```sql
-- sql/schema.sql (source tables)
CREATE TABLE source.customers (
  id         SERIAL PRIMARY KEY,
  name       TEXT NOT NULL,
  email      TEXT NOT NULL,
  status     TEXT NOT NULL DEFAULT 'active',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- (+ products, orders, order_items, inventory — same pattern)
```

**Target tables** — CDC destination, structure mirrors source but with no foreign keys:

```sql
CREATE TABLE target.customers  (LIKE source.customers  INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
-- (+ products, orders, order_items, inventory)
```

**Important:** `LIKE ... INCLUDING DEFAULTS INCLUDING CONSTRAINTS` copies CHECK and NOT NULL constraints but does NOT copy the PRIMARY KEY. This must be added separately (see Step 7).

---

## Step 3 — Create the Debezium Publication

A PostgreSQL **publication** is a named set of tables whose changes flow into the logical replication stream. Debezium reads from this publication.

```bash
psql "host=$DB_HOST dbname=pocdb user=pocadmin password=<PASSWORD>" \
  -f sql/publication.sql
```

**Contents of `sql/publication.sql`:**

```sql
CREATE PUBLICATION poc_publication
  FOR TABLE source.customers, source.products, source.orders,
            source.order_items, source.inventory;
```

**Why pre-create the publication?**

If no publication exists when Debezium starts, it attempts to auto-create one — which requires superuser. RDS's `pocadmin` user is not a superuser. Pre-creating it avoids this and gives us explicit control over which tables are included.

**Verify the publication exists:**

```sql
SELECT pubname, pubtables FROM pg_publication
  JOIN pg_publication_rel ON pg_publication.oid = pg_publication_rel.prpubid
  JOIN pg_class ON pg_class.oid = pg_publication_rel.prrelid;
```

---

## Step 4 — Upload Connector Plugins to S3

MSK Connect does not pull JARs from the internet at runtime. All connector JARs must be packaged into ZIPs and uploaded to S3 before creating the connectors.

```bash
export AWS_REGION=us-east-1
./cdk/connectors/setup-connectors.sh
```

### What the script does

**Downloads two connector packages from Confluent Hub:**

| Plugin | Version | Purpose |
|---|---|---|
| `debezium/debezium-connector-postgresql` | 3.2.6-1 | Reads WAL from PostgreSQL via logical replication |
| `confluentinc/kafka-connect-jdbc` | 10.9.3 | Writes records to PostgreSQL via JDBC |

**Adds the PostgreSQL JDBC driver to the sink ZIP:**

The Confluent JDBC sink ZIP does not include the PostgreSQL driver. The script downloads `postgresql-42.7.3.jar` from Maven Central and adds it to the extracted sink ZIP before re-packaging.

```bash
# From setup-connectors.sh
curl -fsSL -o "$WORK_DIR/jdbc-raw.zip" \
  "${CONFLUENT_HUB}/confluentinc/kafka-connect-jdbc/versions/10.9.3/archive"
curl -fsSL -o "$WORK_DIR/jdbc-sink/postgresql-42.7.3.jar" \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"
```

**Uploads both ZIPs to S3:**

```
s3://datastream-poc-plugins-<account>-us-east-1/debezium-connector-postgresql.zip
s3://datastream-poc-plugins-<account>-us-east-1/kafka-connect-jdbc.zip
```

---

## Step 5 — Create MSK Connect Custom Plugins

`setup-connectors.sh` (continued from Step 4) creates two **custom plugins** in MSK Connect — one per ZIP:

```bash
# From setup-connectors.sh
aws kafkaconnect create-custom-plugin \
  --name "poc-debezium-postgresql" \
  --content-type "ZIP" \
  --location "{\"s3Location\":{\"bucketArn\":\"${BUCKET_ARN}\",\"fileKey\":\"debezium-connector-postgresql.zip\"}}" \
  --region "$REGION"
```

MSK Connect validates the ZIP structure and unpacks the JARs. This takes 5–10 minutes per plugin. The script polls until both reach `ACTIVE` state before proceeding.

**Verify custom plugins are active:**

```bash
aws kafkaconnect list-custom-plugins --region us-east-1 \
  --query "customPlugins[*].{Name:name,State:customPluginState}" \
  --output table
```

Expected output:
```
-------------------------------------------
|          ListCustomPlugins              |
+-----------------------------+-----------+
|            Name             |   State   |
+-----------------------------+-----------+
|  poc-debezium-postgresql    |  ACTIVE   |
|  poc-confluent-jdbc-sink    |  ACTIVE   |
+-----------------------------+-----------+
```

---

## Step 6 — Create the Debezium Source Connector

The Debezium connector connects to PostgreSQL via the replication protocol, creates a replication slot, and streams row-level change events to Kafka topics.

```bash
export AWS_REGION=us-east-1
./cdk/connectors/recreate-source-connector.sh
```

### What the script configures

The script reads CDK outputs from CloudFormation (DB endpoint, credentials, VPC subnets) and creates the connector with this configuration:

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",

  "database.hostname": "<rds-endpoint>",
  "database.port": "5432",
  "database.user": "<from-secrets-manager>",
  "database.password": "<from-secrets-manager>",
  "database.dbname": "pocdb",

  "topic.prefix": "poc4",
  "schema.include.list": "source",
  "plugin.name": "pgoutput",
  "publication.name": "poc_publication",
  "slot.name": "poc4_debezium_slot",

  "temporal.precision.mode": "connect",
  "heartbeat.interval.ms": "10000",

  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "true",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "true",

  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.drop.tombstones": "false",
  "transforms.unwrap.delete.handling.mode": "rewrite"
}
```

### Key configuration decisions

**`plugin.name: pgoutput`** — The built-in PostgreSQL logical decoding plugin. Available on RDS without any server extensions. The only alternative (`decoderbufs`) requires a third-party extension that RDS does not support.

**`temporal.precision.mode: connect`** — Controls how Debezium encodes timestamp columns:
- Without this: `TIMESTAMP(6)` columns emit microseconds as `io.debezium.time.MicroTimestamp` (no standard Kafka schema type)
- With `connect`: `TIMESTAMP(3)` columns emit milliseconds as `org.apache.kafka.connect.data.Timestamp` (standard Connect type)

The target schema uses `BIGINT` for timestamp columns (see Step 7) because the JDBC Sink always binds `int64` fields as PostgreSQL `bigint`, regardless of the Connect logical type.

**`transforms.unwrap`** — `ExtractNewRecordState` SMT flattens the Debezium envelope. Without it, every Kafka message contains `{before: {...}, after: {...}, op: "c"}`. With it, Kafka messages contain only the `after` row for inserts/updates, and a `{__deleted: true, ...before}` record for deletes.

**`slot.name: poc4_debezium_slot`** — PostgreSQL tracks WAL consumption per slot. Debezium creates this slot on first start. If the connector is deleted without dropping the slot, WAL will accumulate on RDS.

**`topic.prefix: poc4`** — Topics are named `<prefix>.source.<table>`. The prefix was bumped to `poc4` during debugging to get fresh consumer offsets with no stale messages.

**`kafka-connect-version: 3.7.x`** — Debezium 3.2.6 requires a modern Kafka Connect runtime. The JDBC sink uses an older version (2.7.1) because it was built against that API.

### Connector lifecycle

After creation, MSK Connect provisions worker JVMs. This takes 5–10 minutes. Monitor with:

```bash
aws kafkaconnect list-connectors --region us-east-1 \
  --query "connectors[*].{Name:connectorName,State:connectorState}" \
  --output table
```

When `RUNNING`, Debezium immediately takes an **initial snapshot** — it SELECTs all rows from each source table and emits a `READ` event (op=`r`) per row. Then it switches to streaming WAL changes in real time.

### Verify the replication slot was created

```sql
SELECT slot_name, plugin, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots;
```

Expected: `poc4_debezium_slot` with `active = true` when the connector is running.

---

## Step 7 — Fix Schema for JDBC Sink Compatibility

This step resolves a type mismatch: the JDBC Sink connector (Confluent 10.9.3 on Kafka Connect 2.7.1) binds all `int64` values as PostgreSQL `bigint` via `::int8` cast. PostgreSQL will not implicitly cast `bigint` to `timestamp`, so target timestamp columns must be `BIGINT`.

### 7a. Fix source timestamp precision

Change all source timestamp columns from `TIMESTAMPTZ` to `TIMESTAMP(3)` so Debezium emits standard millisecond epochs (not microseconds or ISO-8601 strings):

```bash
python fix_schema_timestamps_ms.py
```

**What this does:**

```python
# fix_schema_timestamps_ms.py (key excerpt)
alters = [
    "ALTER TABLE source.customers  ALTER COLUMN created_at   TYPE TIMESTAMP(3)",
    "ALTER TABLE source.customers  ALTER COLUMN updated_at   TYPE TIMESTAMP(3)",
    "ALTER TABLE source.products   ALTER COLUMN updated_at   TYPE TIMESTAMP(3)",
    "ALTER TABLE source.orders     ALTER COLUMN order_date   TYPE TIMESTAMP(3)",
    "ALTER TABLE source.orders     ALTER COLUMN updated_at   TYPE TIMESTAMP(3)",
    "ALTER TABLE source.inventory  ALTER COLUMN last_updated TYPE TIMESTAMP(3)",
]
```

**Why `TIMESTAMP(3)` and not `TIMESTAMP(6)` or `TIMESTAMPTZ`?**

| Column type | Debezium emits | Kafka schema type | JDBC Sink binding |
|---|---|---|---|
| `TIMESTAMPTZ` | ISO-8601 string | `io.debezium.time.ZonedTimestamp` | text — fails on timestamp target |
| `TIMESTAMP(6)` | microseconds (int64) | `io.debezium.time.MicroTimestamp` | `::int8` — fails on timestamp target |
| `TIMESTAMP(3)` | milliseconds (int64) | `org.apache.kafka.connect.data.Timestamp` | `::int8` — works on BIGINT target |

### 7b. Fix target timestamp columns to BIGINT

```bash
python fix_target_timestamps_bigint.py
```

**What this does (must drop DEFAULT first):**

```python
# fix_target_timestamps_bigint.py (key excerpt)
ts_columns = [
    ("target.customers", "created_at"), ("target.customers", "updated_at"),
    ("target.products",  "updated_at"), ("target.orders",    "order_date"),
    ("target.orders",    "updated_at"), ("target.inventory", "last_updated"),
]
for table, col in ts_columns:
    # Must drop DEFAULT (now()) before converting to BIGINT
    cur.execute(f"ALTER TABLE {table} ALTER COLUMN {col} DROP DEFAULT")
    cur.execute(f"""
        ALTER TABLE {table} ALTER COLUMN {col}
        TYPE BIGINT USING EXTRACT(EPOCH FROM {col})::BIGINT * 1000
    """)
```

The `USING` clause converts existing `TIMESTAMP` values to millisecond epochs during the migration.

**To read timestamps in human-readable form from the target:**

```sql
SELECT id, to_timestamp(order_date / 1000.0) AS order_date_readable
FROM target.orders;
```

### 7c. Add PRIMARY KEY to target tables

`CREATE TABLE ... LIKE source INCLUDING DEFAULTS INCLUDING CONSTRAINTS` copies CHECK constraints and NOT NULL constraints, but **does not copy PRIMARY KEY**. The JDBC Sink's `ON CONFLICT (id) DO UPDATE` requires a PRIMARY KEY or UNIQUE constraint on `id`:

```bash
python fix_target_constraints.py
```

**What this does:**

```python
# fix_target_constraints.py (key excerpt)
for t in ['customers', 'products', 'orders', 'order_items', 'inventory']:
    # Check if already exists (idempotent)
    cur.execute(f"""SELECT 1 FROM information_schema.table_constraints
                    WHERE table_schema='target' AND table_name='{t}'
                    AND constraint_type='PRIMARY KEY'""")
    if not cur.fetchone():
        cur.execute(f"ALTER TABLE target.{t} ADD PRIMARY KEY (id)")
```

Without this, every insert to the target fails with:
```
ERROR: no unique or exclusion constraint matching ON CONFLICT specification
```

---

## Step 8 — Create the JDBC Sink Connector

The sink connector reads from Kafka topics and upserts records into the `target` schema via JDBC.

```bash
export AWS_REGION=us-east-1
./cdk/connectors/recreate-sink-connector.sh
```

### What the script configures

```json
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "tasks.max": "1",

  "connection.url": "jdbc:postgresql://<rds-endpoint>:5432/pocdb?currentSchema=target",
  "connection.user": "<from-secrets-manager>",
  "connection.password": "<from-secrets-manager>",

  "topics.regex": "poc4\\.source\\..*",
  "insert.mode": "upsert",
  "pk.mode": "record_key",
  "pk.fields": "id",

  "auto.create": "false",
  "auto.evolve": "false",
  "delete.enabled": "true",
  "dialect.name": "PostgreSqlDatabaseDialect",

  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "true",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "true",

  "transforms": "routeToTable",
  "transforms.routeToTable.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.routeToTable.regex": "poc4\\.source\\.(.*)",
  "transforms.routeToTable.replacement": "$1"
}
```

### Key configuration decisions

**`currentSchema=target` in JDBC URL** — Routes all JDBC operations to the `target` schema without needing schema-qualified table names in every query.

**`insert.mode: upsert` + `pk.mode: record_key`** — For each Kafka message, the sink generates:
```sql
INSERT INTO target.customers (id, name, email, status, ...)
VALUES (...)
ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, email=EXCLUDED.email, ...
```
This handles INSERT, UPDATE, and snapshot READ events identically. Row count in target never exceeds source row count.

**`delete.enabled: true`** — When the source deletes a row, Debezium (with `delete.handling.mode=rewrite`) produces a message with `__deleted=true`. The sink issues `DELETE FROM target.customers WHERE id=<key>`.

**`auto.create: false`** — Prevents the sink from auto-creating tables with potentially wrong column types. We pre-create the target tables in Step 2.

**`dialect.name: PostgreSqlDatabaseDialect`** — Required in JDBC Sink 10.9.3. Without it the connector fails to start. This dialect enables `ON CONFLICT ... DO UPDATE` syntax and correct type mappings.

**`RegexRouter` SMT** — Strips the `poc4.source.` prefix from topic names so `poc4.source.customers` routes to the `customers` table (in the `target` schema via the JDBC URL). Without this, the sink would try to create/write to a table named `poc4.source.customers`.

**`kafka-connect-version: 2.7.1`** — The Confluent JDBC Sink 10.9.3 was built against Kafka Connect 2.7.1 API. Using a newer runtime breaks compatibility.

**Consumer group name** — MSK Connect names the consumer group `connect-<connector-name>`. For this connector it is `connect-poc-jdbc-sink-v4`. The `v4` suffix ensures a fresh consumer group (no committed offsets from prior test runs).

**Why can't you reset the consumer group offset directly?** MSK Connect sets `connector.client.config.override.policy=None` (hardcoded by AWS). This blocks all `consumer.override.*` settings including `consumer.override.auto.offset.reset=latest`. The only way to get a fresh start is to use a new connector name — which creates a new consumer group with no committed offsets.

### Monitor the sink connector

```bash
aws kafkaconnect list-connectors --region us-east-1 \
  --query "connectors[*].{Name:connectorName,State:connectorState}" \
  --output table
```

The sink starts consuming from offset 0 of all `poc4.source.*` topics. If the source connector already ran a snapshot, the sink will receive all those rows immediately and upsert them into the target schema.

---

## Step 9 — Seed Data and Verify Replication

Install inject.py dependencies:

```bash
cd inject
pip install -r requirements.txt   # psycopg2-binary, faker, click, boto3
cd ..
```

### 9a. Check current state

```bash
python db_status.py
```

Output shows source vs target row counts. Both should be 0 initially.

### 9b. Seed 20 rows per table

```bash
python inject/inject.py seed
```

This inserts 20 rows per table (100 total) using Faker-generated data.

### 9c. Verify replication

Allow 10–30 seconds for CDC propagation, then check:

```bash
python inject/inject.py status
# or
python db_status.py
```

Expected:
```
╭──────────────┬────────┬────────┬─────╮
│ Table        │ Source │ Target │ Lag │
├──────────────┼────────┼────────┼─────┤
│ customers    │     20 │     20 │   0 │
│ products     │     20 │     20 │   0 │
│ orders       │     20 │     20 │   0 │
│ order_items  │     20 │     20 │   0 │
│ inventory    │     20 │     20 │   0 │
╰──────────────┴────────┴────────┴─────╯
```

---

## Step 10 — Run Test Scenarios

### Test A — Volume test (insert N rows, measure latency)

```bash
python inject/inject.py test-volume --count 100 --wait 60
python inject/inject.py test-volume --count 1000 --wait 120
```

The command:
1. Clears all source tables via `DELETE` (CDC propagates deletions to target)
2. Seeds N rows per table
3. Polls until target counts match source counts
4. Reports latency per table and pass/fail

### Test B — Update propagation (proves upsert, not insert)

```bash
python inject/inject.py test-updates --sample 5
```

The command:
1. Updates 5 rows per table with sentinel values (e.g. `status='cdc_verified'`, `price=9999.99`)
2. Waits for CDC propagation
3. Verifies the exact sentinel values appear in the target

This proves that UPDATE events upsert the existing target row (same `id`) rather than inserting duplicates.

### Test C — Value-level verification

```bash
python inject/inject.py verify --sample 10
```

Samples 10 rows per table and compares column values between source and target. Reports MATCH or MISMATCH per column.

### Test D — Single operations

```bash
# Insert one row
python inject/inject.py insert --table customers

# Update one row
python inject/inject.py update --table orders

# Delete one row
python inject/inject.py delete --table products

# Check status after each
python inject/inject.py status
```

### Test E — Bulk load

```bash
# 5000 rows into orders (executemany, 500-row batches)
python inject/inject.py bulk --table orders --count 5000

# 1000 rows across all 5 tables concurrently (threaded)
python inject/inject.py bulk-all --count 1000
```

### Test F — Chaos (random mix of operations)

```bash
# 5 operations/second for 30 seconds
python inject/inject.py chaos --rate 5 --duration 30
```

---

## Monitoring and Validation in AWS

### CloudWatch — Connector logs

All MSK Connect worker logs go to `/datastream-poc/msk-connect`. Search for errors:

**Using Logs Insights (AWS Console → CloudWatch → Logs Insights):**

```
# Find all ERROR lines in the last hour
fields @timestamp, @message
| filter @message like /ERROR/
| sort @timestamp desc
| limit 50
```

```
# Find task failure messages
fields @timestamp, @message
| filter @message like /FAILED|Exception|task/
| sort @timestamp desc
```

**Using AWS CLI (Git Bash):**

```bash
MSYS_NO_PATHCONV=1 aws logs filter-log-events \
  --log-group-name /datastream-poc/msk-connect \
  --region us-east-1 \
  --filter-pattern ERROR \
  --start-time $(date -d '1 hour ago' +%s000) \
  --query "events[*].message" \
  --output text
```

### MSK Console — Consumer group lag

AWS Console → MSK → Clusters → `datastream-poc` → Consumer groups → `connect-poc-jdbc-sink-v4`

Shows the lag (unconsumed messages) per topic partition. Lag = 0 means the sink is caught up.

### RDS direct queries

```sql
-- Compare row counts across schemas
SELECT
  (SELECT COUNT(*) FROM source.customers) AS src_customers,
  (SELECT COUNT(*) FROM target.customers) AS tgt_customers;

-- Verify replication slot is active and advancing
SELECT slot_name, active, confirmed_flush_lsn FROM pg_replication_slots;

-- Check for WAL accumulation (inactive slots are dangerous)
SELECT slot_name, restart_lsn, confirmed_flush_lsn, active
FROM pg_replication_slots
WHERE NOT active;

-- View timestamps in human-readable form
SELECT id, to_timestamp(order_date / 1000.0) AS order_date_readable
FROM target.orders
LIMIT 5;
```

### Connector task state

A connector in `RUNNING` state can have a `FAILED` task. Always check task-level state:

```bash
aws kafkaconnect list-connectors --region us-east-1 \
  --query "connectors[*].{Name:connectorName,State:connectorState}" \
  --output table
```

If a task fails, the connector shows `RUNNING` but stops processing. MSK Connect has no task-restart API — delete and recreate the connector using the recreate scripts.

---

## Resetting for a New Test Run

### Option A — Clear source (CDC propagates deletes to target)

```bash
# Delete all source rows — CDC sends delete events to Kafka → target
psql "host=$DB_HOST dbname=pocdb user=pocadmin password=<PASSWORD>" \
  -f sql/clear_source.sql

# Wait 15s for propagation
python inject/inject.py status   # should show all zeros

# Reseed
python inject/inject.py seed
```

### Option B — Reset target independently (bypass CDC)

```bash
psql "host=$DB_HOST dbname=pocdb user=pocadmin password=<PASSWORD>" \
  -f sql/clear_target.sql
```

`TRUNCATE ... RESTART IDENTITY CASCADE` bypasses CDC and resets target directly. Useful when you want to test snapshot replay without re-deleting source data.

---

## Teardown

```bash
cd cdk
cdk destroy --all
```

Before destroying, drop the replication slot to release WAL (optional, since RDS will be deleted anyway):

```sql
SELECT pg_drop_replication_slot('poc4_debezium_slot');
```

CDK destroy removes in reverse order: MSK Connect connectors → MSK cluster → RDS → VPC → S3. Takes ~10 minutes. RDS deletion protection is **off** so no manual intervention is needed.

---

## Troubleshooting Reference

| Symptom | Root cause | Fix |
|---|---|---|
| Target count = 0, no errors | Sink reading from wrong offset | Bump topic prefix, recreate connectors |
| `character varying` → `timestamp` error | `TIMESTAMPTZ` columns emit ISO-8601 strings | Change to `TIMESTAMP(3)` |
| Microsecond bigint (16 digits) → `timestamp` error | `TIMESTAMP(6)` + `connect` mode = microseconds | Change to `TIMESTAMP(3)` |
| Millisecond bigint (13 digits) → `timestamp` error | JDBC Sink 10.9.3 always binds `::int8` | Change target columns to `BIGINT` |
| `no unique or exclusion constraint` on ON CONFLICT | `LIKE ... INCLUDING CONSTRAINTS` skips PRIMARY KEY | `ALTER TABLE target.x ADD PRIMARY KEY (id)` |
| `default for column cannot be cast to bigint` | Timestamp columns have `DEFAULT now()` | `ALTER COLUMN DROP DEFAULT` first, then `ALTER COLUMN TYPE BIGINT USING ...` |
| Connector task FAILED, connector shows RUNNING | Unrecoverable task error | Delete and recreate connector |
| WAL accumulating on RDS | Inactive replication slot with stale `restart_lsn` | `SELECT pg_drop_replication_slot('<slot>')` |
| `consumer.override.*` silently ignored | MSK Connect `connector.client.config.override.policy=None` | Use a new connector name to reset the consumer group |
| Git Bash path conversion in `aws logs` | MSYS converts `/datastream-poc/...` to Windows path | Prefix command with `MSYS_NO_PATHCONV=1` |
| DBVisualizer "password authentication failed" | Password contains special chars, was truncated on copy | Use `python show_credentials.py` to print it cleanly |
