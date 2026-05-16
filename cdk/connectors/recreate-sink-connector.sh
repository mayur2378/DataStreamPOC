#!/usr/bin/env bash
# recreate-sink-connector.sh
#
# Deletes poc-jdbc-sink (if it exists) and recreates it.
# Run this after recreate-source-connector.sh when you need to reset
# the sink's consumer offsets (e.g., after a source config change).
#
# Usage (from repo root in Git Bash):
#   export AWS_REGION=us-east-1
#   ./cdk/connectors/recreate-sink-connector.sh

set -euo pipefail
export MSYS_NO_PATHCONV=1

REGION="${AWS_REGION:-$(aws configure get region)}"
STACK_PREFIX="DataStream"

log() { echo "[$(date +%H:%M:%S)] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

command -v jq >/dev/null || die "jq not found."

# ── Read stack outputs ─────────────────────────────────────────────────────────

log "Reading CDK stack outputs..."

_cfn_output() {
  aws cloudformation describe-stacks \
    --stack-name "$1" --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue" \
    --output text
}

CONNECT_ROLE_ARN=$(_cfn_output "${STACK_PREFIX}ConnectorsStack" "MskConnectRoleArn")
PRIVATE_SUBNETS=$(_cfn_output "${STACK_PREFIX}ConnectorsStack" "PrivateSubnets")
CONNECT_SG=$(_cfn_output "${STACK_PREFIX}ConnectorsStack" "MskConnectSgId")
MSK_CLUSTER_ARN=$(_cfn_output "${STACK_PREFIX}MskStack" "MskClusterArn")
DB_ENDPOINT=$(_cfn_output "${STACK_PREFIX}RdsStack" "DbEndpoint")
DB_SECRET_ARN=$(_cfn_output "${STACK_PREFIX}RdsStack" "DbSecretArn")

# ── Fetch credentials and brokers ─────────────────────────────────────────────

log "Fetching DB credentials..."
SECRET_JSON=$(aws secretsmanager get-secret-value \
  --secret-id "$DB_SECRET_ARN" --region "$REGION" \
  --query SecretString --output text)
DB_USER=$(echo "$SECRET_JSON" | jq -r '.username')
DB_PASS=$(echo "$SECRET_JSON" | jq -r '.password')

log "Fetching MSK bootstrap brokers..."
BOOTSTRAP_BROKERS=$(aws kafka get-bootstrap-brokers \
  --cluster-arn "$MSK_CLUSTER_ARN" --region "$REGION" \
  --query "BootstrapBrokerString" --output text)

# ── Find existing Confluent JDBC sink custom plugin ────────────────────────────

log "Looking up Confluent JDBC sink custom plugin ARN..."
SINK_PLUGIN_ARN=$(aws kafkaconnect list-custom-plugins \
  --region "$REGION" \
  --query "customPlugins[?name=='poc-confluent-jdbc-sink'].customPluginArn" \
  --output text)

[ -z "$SINK_PLUGIN_ARN" ] || [ "$SINK_PLUGIN_ARN" = "None" ] && \
  die "Custom plugin 'poc-confluent-jdbc-sink' not found. Run setup-connectors.sh first."

log "  Plugin ARN: $SINK_PLUGIN_ARN"

# ── Delete existing sink connector if present ──────────────────────────────────

for SINK_NAME in "poc-jdbc-sink" "poc-jdbc-sink-v2" "poc-jdbc-sink-v3" "poc-jdbc-sink-v4"; do
  EXISTING_ARN=$(aws kafkaconnect list-connectors \
    --region "$REGION" \
    --query "connectors[?connectorName=='${SINK_NAME}'].connectorArn" \
    --output text 2>/dev/null || true)
  if [ -n "$EXISTING_ARN" ] && [ "$EXISTING_ARN" != "None" ]; then
    log "Deleting existing ${SINK_NAME} connector..."
    aws kafkaconnect delete-connector --connector-arn "$EXISTING_ARN" --region "$REGION"
    log "Waiting for deletion..."
    while aws kafkaconnect describe-connector --connector-arn "$EXISTING_ARN" --region "$REGION" &>/dev/null; do
      sleep 10
    done
    log "  Deleted ${SINK_NAME}."
  fi
done
log "No pre-existing sink connectors remain."

# ── Build VPC config ──────────────────────────────────────────────────────────

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

# ── Create sink connector ─────────────────────────────────────────────────────

log "Creating poc-jdbc-sink-v4 connector (fresh consumer group on poc4 topics)..."

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
    "topics.regex": "poc4\\.source\\..*",
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
    "transforms.routeToTable.regex": "poc4\\.source\\.(.*)",
    "transforms.routeToTable.replacement": "$1",
    "dialect.name": "PostgreSqlDatabaseDialect"
  }')

aws kafkaconnect create-connector \
  --connector-name "poc-jdbc-sink-v4" \
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

log ""
log "✓ poc-jdbc-sink-v4 created (fresh consumer group on poc4 topics)."
log "  Seed new data AFTER it reaches RUNNING state so there is something to consume."
log "  Allow 5-10 min to reach RUNNING state."
log ""
log "  Check state:"
log "  aws kafkaconnect list-connectors --region $REGION --query \"connectors[*].{Name:connectorName,State:connectorState}\" --output table"
