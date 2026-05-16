#!/usr/bin/env bash
# setup-connectors.sh
#
# Downloads Kafka Connect plugin JARs, uploads to S3, creates MSK Connect
# custom plugins, then creates the Debezium source and JDBC sink connectors.
#
# Run AFTER: cdk deploy --all
# Requires: AWS CLI v2, jq, curl
#
# Usage (from repo root in Git Bash):
#   export AWS_REGION=us-east-1
#   ./cdk/connectors/setup-connectors.sh

set -euo pipefail

REGION="${AWS_REGION:-$(aws configure get region)}"
STACK_PREFIX="DataStream"

log() { echo "[$(date +%H:%M:%S)] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

command -v jq  >/dev/null || die "jq not found. Install: winget install jqlang.jq"
command -v curl >/dev/null || die "curl not found."

# ── Read CDK stack outputs ─────────────────────────────────────────────────────

log "Reading CDK stack outputs..."

_cfn_output() {
  aws cloudformation describe-stacks \
    --stack-name "$1" --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue" \
    --output text
}

PLUGIN_BUCKET=$(_cfn_output "${STACK_PREFIX}ConnectorsStack" "PluginBucketName")
CONNECT_ROLE_ARN=$(_cfn_output "${STACK_PREFIX}ConnectorsStack" "MskConnectRoleArn")
PRIVATE_SUBNETS=$(_cfn_output "${STACK_PREFIX}ConnectorsStack" "PrivateSubnets")
CONNECT_SG=$(_cfn_output "${STACK_PREFIX}ConnectorsStack" "MskConnectSgId")
MSK_CLUSTER_ARN=$(_cfn_output "${STACK_PREFIX}MskStack" "MskClusterArn")
DB_ENDPOINT=$(_cfn_output "${STACK_PREFIX}RdsStack" "DbEndpoint")
DB_SECRET_ARN=$(_cfn_output "${STACK_PREFIX}RdsStack" "DbSecretArn")

log "Plugin bucket : $PLUGIN_BUCKET"
log "Connect role  : $CONNECT_ROLE_ARN"
log "MSK cluster   : $MSK_CLUSTER_ARN"
log "DB endpoint   : $DB_ENDPOINT"

# ── Fetch DB credentials ───────────────────────────────────────────────────────

log "Fetching DB credentials from Secrets Manager..."
SECRET_JSON=$(aws secretsmanager get-secret-value \
  --secret-id "$DB_SECRET_ARN" --region "$REGION" \
  --query SecretString --output text)
DB_USER=$(echo "$SECRET_JSON" | jq -r '.username')
DB_PASS=$(echo "$SECRET_JSON" | jq -r '.password')

# ── Get MSK bootstrap brokers ──────────────────────────────────────────────────

log "Fetching MSK bootstrap brokers..."
BOOTSTRAP_BROKERS=$(aws kafka get-bootstrap-brokers \
  --cluster-arn "$MSK_CLUSTER_ARN" --region "$REGION" \
  --query "BootstrapBrokerString" --output text)
log "Bootstrap brokers: $BOOTSTRAP_BROKERS"

# ── Download connector plugins ─────────────────────────────────────────────────

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

# Confluent Hub versions — confirmed working as of POC setup
DEBEZIUM_SRC_VERSION="3.2.6-1"       # debezium/debezium-connector-postgresql
CONFLUENT_JDBC_VERSION="10.9.3"       # confluentinc/kafka-connect-jdbc
PG_JDBC_VERSION="42.7.3"

CONFLUENT_HUB="https://api.hub.confluent.io/api/plugins"

log "  Debezium source version  : $DEBEZIUM_SRC_VERSION (Confluent Hub)"
log "  Confluent JDBC version   : $CONFLUENT_JDBC_VERSION (Confluent Hub)"

# Download source connector ZIP directly from Confluent Hub — no extraction needed
log "Downloading Debezium PostgreSQL source connector v${DEBEZIUM_SRC_VERSION}..."
curl -fsSL -o "$WORK_DIR/debezium-connector-postgresql.zip" \
  "${CONFLUENT_HUB}/debezium/debezium-connector-postgresql/versions/${DEBEZIUM_SRC_VERSION}/archive"
log "  Source plugin ZIP ready."

# Download Confluent JDBC sink, add PostgreSQL JDBC driver, re-zip
log "Downloading Confluent JDBC sink connector v${CONFLUENT_JDBC_VERSION}..."
curl -fsSL -o "$WORK_DIR/jdbc-raw.zip" \
  "${CONFLUENT_HUB}/confluentinc/kafka-connect-jdbc/versions/${CONFLUENT_JDBC_VERSION}/archive"
mkdir -p "$WORK_DIR/jdbc-sink"
unzip -q "$WORK_DIR/jdbc-raw.zip" -d "$WORK_DIR/jdbc-sink"
log "  Adding PostgreSQL JDBC driver v${PG_JDBC_VERSION}..."
curl -fsSL -o "$WORK_DIR/jdbc-sink/postgresql-${PG_JDBC_VERSION}.jar" \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"
python -c "
import zipfile, os, sys
src = sys.argv[1]; out = sys.argv[2]
with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED) as zf:
    for root, dirs, files in os.walk(src):
        for f in files:
            fp = os.path.join(root, f)
            zf.write(fp, os.path.relpath(fp, src))
" "$WORK_DIR/jdbc-sink" "$WORK_DIR/kafka-connect-jdbc.zip"
log "  Sink plugin ZIP ready."

# ── Upload to S3 ───────────────────────────────────────────────────────────────

log "Uploading plugin ZIPs to s3://${PLUGIN_BUCKET}/ ..."
aws s3 cp "$WORK_DIR/debezium-connector-postgresql.zip" \
  "s3://${PLUGIN_BUCKET}/debezium-connector-postgresql.zip" --region "$REGION"
aws s3 cp "$WORK_DIR/kafka-connect-jdbc.zip" \
  "s3://${PLUGIN_BUCKET}/kafka-connect-jdbc.zip" --region "$REGION"
log "  Upload complete."

# ── Create MSK Connect custom plugins ─────────────────────────────────────────

BUCKET_ARN="arn:aws:s3:::${PLUGIN_BUCKET}"

log "Creating Debezium source custom plugin..."
DEBEZIUM_PLUGIN_ARN=$(aws kafkaconnect create-custom-plugin \
  --name "poc-debezium-postgresql" \
  --content-type "ZIP" \
  --location "{\"s3Location\":{\"bucketArn\":\"${BUCKET_ARN}\",\"fileKey\":\"debezium-connector-postgresql.zip\"}}" \
  --region "$REGION" \
  --query "customPluginArn" --output text)
log "  Source plugin ARN: $DEBEZIUM_PLUGIN_ARN"

log "Creating Confluent JDBC sink custom plugin..."
SINK_PLUGIN_ARN=$(aws kafkaconnect create-custom-plugin \
  --name "poc-confluent-jdbc-sink" \
  --content-type "ZIP" \
  --location "{\"s3Location\":{\"bucketArn\":\"${BUCKET_ARN}\",\"fileKey\":\"kafka-connect-jdbc.zip\"}}" \
  --region "$REGION" \
  --query "customPluginArn" --output text)
log "  Sink plugin ARN: $SINK_PLUGIN_ARN"

_wait_for_plugin() {
  local ARN="$1" NAME="$2"
  log "Waiting for plugin '$NAME' to become ACTIVE (5-10 min)..."
  while true; do
    STATE=$(aws kafkaconnect describe-custom-plugin \
      --custom-plugin-arn "$ARN" --region "$REGION" \
      --query "customPluginState" --output text)
    log "  $NAME state: $STATE"
    [ "$STATE" = "ACTIVE" ] && return 0
    [ "$STATE" = "CREATE_FAILED" ] && die "Plugin '$NAME' creation failed. Check CloudWatch logs."
    sleep 30
  done
}

_wait_for_plugin "$DEBEZIUM_PLUGIN_ARN" "poc-debezium-postgresql"
_wait_for_plugin "$SINK_PLUGIN_ARN" "poc-confluent-jdbc-sink"

# ── Build VPC config JSON for connectors ──────────────────────────────────────

SUBNET_JSON_ARRAY=$(echo "$PRIVATE_SUBNETS" | tr ',' '\n' | jq -R . | jq -sc .)

KAFKA_CLUSTER_JSON=$(jq -n \
  --arg brokers "$BOOTSTRAP_BROKERS" \
  --argjson subnets "$SUBNET_JSON_ARRAY" \
  --arg sg "$CONNECT_SG" \
  '{
    apacheKafkaCluster: {
      bootstrapServers: $brokers,
      vpc: { subnets: $subnets, securityGroups: [$sg] }
    }
  }')

# ── Create Debezium source connector ──────────────────────────────────────────

log "Creating Debezium source connector..."

SOURCE_CONFIG=$(jq -n \
  --arg host "$DB_ENDPOINT" \
  --arg user "$DB_USER" \
  --arg pass "$DB_PASS" \
  '{
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": $host,
    "database.port": "5432",
    "database.user": $user,
    "database.password": $pass,
    "database.dbname": "pocdb",
    "topic.prefix": "poc",
    "schema.include.list": "source",
    "plugin.name": "pgoutput",
    "temporal.precision.mode": "connect",
    "publication.name": "poc_publication",
    "slot.name": "poc_debezium_slot",
    "heartbeat.interval.ms": "10000",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite"
  }')

aws kafkaconnect create-connector \
  --connector-name "poc-debezium-source" \
  --kafka-cluster "$KAFKA_CLUSTER_JSON" \
  --kafka-cluster-client-authentication '{"authenticationType":"NONE"}' \
  --kafka-cluster-encryption-in-transit '{"encryptionType":"PLAINTEXT"}' \
  --kafka-connect-version "3.7.x" \
  --plugins "[{\"customPlugin\":{\"customPluginArn\":\"${DEBEZIUM_PLUGIN_ARN}\",\"revision\":1}}]" \
  --service-execution-role-arn "$CONNECT_ROLE_ARN" \
  --capacity '{"autoScaling":{"maxWorkerCount":2,"mcuCount":1,"minWorkerCount":1,"scaleInPolicy":{"cpuUtilizationPercentage":20},"scaleOutPolicy":{"cpuUtilizationPercentage":80}}}' \
  --connector-configuration "$SOURCE_CONFIG" \
  --log-delivery '{"workerLogDelivery":{"cloudWatchLogs":{"enabled":true,"logGroup":"/datastream-poc/msk-connect"}}}' \
  --region "$REGION"

log "  Source connector created."

# ── Create JDBC sink connector ─────────────────────────────────────────────────

log "Creating JDBC sink connector..."

SINK_CONFIG=$(jq -n \
  --arg host "$DB_ENDPOINT" \
  --arg user "$DB_USER" \
  --arg pass "$DB_PASS" \
  '{
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": ("jdbc:postgresql://" + $host + ":5432/pocdb?currentSchema=target"),
    "connection.user": $user,
    "connection.password": $pass,
    "topics.regex": "poc\\.source\\..*",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "pk.fields": "id",
    "auto.create": "false",
    "auto.evolve": "false",
    "delete.enabled": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
    "transforms": "routeToTable",
    "transforms.routeToTable.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.routeToTable.regex": "poc\\.source\\.(.*)",
    "transforms.routeToTable.replacement": "$1",
    "dialect.name": "PostgreSqlDatabaseDialect"
  }')

aws kafkaconnect create-connector \
  --connector-name "poc-jdbc-sink" \
  --kafka-cluster "$KAFKA_CLUSTER_JSON" \
  --kafka-cluster-client-authentication '{"authenticationType":"NONE"}' \
  --kafka-cluster-encryption-in-transit '{"encryptionType":"PLAINTEXT"}' \
  --kafka-connect-version "2.7.1" \
  --plugins "[{\"customPlugin\":{\"customPluginArn\":\"${SINK_PLUGIN_ARN}\",\"revision\":1}}]" \
  --service-execution-role-arn "$CONNECT_ROLE_ARN" \
  --capacity '{"autoScaling":{"maxWorkerCount":2,"mcuCount":1,"minWorkerCount":1,"scaleInPolicy":{"cpuUtilizationPercentage":20},"scaleOutPolicy":{"cpuUtilizationPercentage":80}}}' \
  --connector-configuration "$SINK_CONFIG" \
  --log-delivery '{"workerLogDelivery":{"cloudWatchLogs":{"enabled":true,"logGroup":"/datastream-poc/msk-connect"}}}' \
  --region "$REGION"

log "  Sink connector created."

log ""
log "✓ Connector setup complete."
log ""
log "  Next steps:"
log "  export DB_SECRET_ARN=${DB_SECRET_ARN}"
log "  export DB_HOST=${DB_ENDPOINT}"
log "  export AWS_REGION=${REGION}"
log "  python inject/inject.py init"
log "  python inject/inject.py seed"
log "  python inject/inject.py status"
log ""
log "  Connector logs: CloudWatch > Log groups > /datastream-poc/msk-connect"
