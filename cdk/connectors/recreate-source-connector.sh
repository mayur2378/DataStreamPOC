#!/usr/bin/env bash
# recreate-source-connector.sh
#
# Deletes poc-debezium-source (if it exists) and recreates it.
# Safe to run after a failed connector without re-running the full setup.
#
# Usage (from repo root in Git Bash):
#   export AWS_REGION=us-east-1
#   ./cdk/connectors/recreate-source-connector.sh

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

# ── Find existing Debezium custom plugin ──────────────────────────────────────

log "Looking up Debezium custom plugin ARN..."
DEBEZIUM_PLUGIN_ARN=$(aws kafkaconnect list-custom-plugins \
  --region "$REGION" \
  --query "customPlugins[?name=='poc-debezium-postgresql'].customPluginArn" \
  --output text)

[ -z "$DEBEZIUM_PLUGIN_ARN" ] || [ "$DEBEZIUM_PLUGIN_ARN" = "None" ] && \
  die "Custom plugin 'poc-debezium-postgresql' not found. Run setup-connectors.sh first."

log "  Plugin ARN: $DEBEZIUM_PLUGIN_ARN"

# ── Delete existing source connector if present ───────────────────────────────

EXISTING_ARN=$(aws kafkaconnect list-connectors \
  --region "$REGION" \
  --query "connectors[?connectorName=='poc-debezium-source'].connectorArn" \
  --output text 2>/dev/null || true)

if [ -n "$EXISTING_ARN" ] && [ "$EXISTING_ARN" != "None" ]; then
  log "Deleting existing poc-debezium-source connector..."
  aws kafkaconnect delete-connector --connector-arn "$EXISTING_ARN" --region "$REGION"
  log "Waiting for deletion..."
  while aws kafkaconnect describe-connector --connector-arn "$EXISTING_ARN" --region "$REGION" &>/dev/null; do
    sleep 10
  done
  log "  Deleted."
else
  log "No existing source connector found, proceeding to create."
fi

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

# ── Create source connector ───────────────────────────────────────────────────

log "Creating poc-debezium-source connector (kafka-connect-version 3.7.x)..."

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
    "topic.prefix": "poc4",
    "schema.include.list": "source",
    "plugin.name": "pgoutput",
    "temporal.precision.mode": "connect",
    "publication.name": "poc_publication",
    "slot.name": "poc4_debezium_slot",
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

log ""
log "✓ Source connector created. Allow 5-10 min for it to reach RUNNING state."
log ""
log "  Check state:"
log "  aws kafkaconnect list-connectors --region $REGION --query \"connectors[*].{Name:connectorName,State:connectorState}\" --output table"
