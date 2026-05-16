#!/usr/bin/env bash
# teardown.sh
#
# Deletes MSK Connect connectors and custom plugins, then runs cdk destroy --all.
# Must be run BEFORE cdk destroy because connectors depend on MSK and VPC.
#
# Usage:
#   ./cdk/connectors/teardown.sh

set -euo pipefail

REGION="${AWS_REGION:-$(aws configure get region)}"

log() { echo "[$(date +%H:%M:%S)] $*"; }

delete_connector() {
  local name="$1"
  ARN=$(aws kafkaconnect list-connectors \
    --region "$REGION" \
    --query "connectors[?connectorName=='${name}'].connectorArn" \
    --output text 2>/dev/null || true)
  if [ -n "$ARN" ] && [ "$ARN" != "None" ]; then
    log "Deleting connector: $name"
    aws kafkaconnect delete-connector --connector-arn "$ARN" --region "$REGION"
    log "  Waiting for deletion..."
    while aws kafkaconnect describe-connector --connector-arn "$ARN" --region "$REGION" &>/dev/null; do
      sleep 10
    done
    log "  Deleted: $name"
  else
    log "Connector not found (already deleted?): $name"
  fi
}

delete_plugin() {
  local name="$1"
  ARN=$(aws kafkaconnect list-custom-plugins \
    --region "$REGION" \
    --query "customPlugins[?name=='${name}'].customPluginArn" \
    --output text 2>/dev/null || true)
  if [ -n "$ARN" ] && [ "$ARN" != "None" ]; then
    log "Deleting custom plugin: $name"
    aws kafkaconnect delete-custom-plugin --custom-plugin-arn "$ARN" --region "$REGION"
    log "  Deleted: $name"
  else
    log "Plugin not found (already deleted?): $name"
  fi
}

log "=== DataStream POC Teardown ==="
log ""

# Step 1: Delete connectors first (they depend on plugins)
delete_connector "poc-debezium-source"
delete_connector "poc-jdbc-sink"
delete_connector "poc-jdbc-sink-v2"
delete_connector "poc-jdbc-sink-v3"
delete_connector "poc-jdbc-sink-v4"

# Step 2: Delete custom plugins
delete_plugin "poc-debezium-postgresql"
delete_plugin "poc-confluent-jdbc-sink"

# Step 3: Empty S3 plugin bucket so CDK can delete it
PLUGIN_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name "DataStreamConnectorsStack" \
  --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='PluginBucketName'].OutputValue" \
  --output text 2>/dev/null || true)

if [ -n "$PLUGIN_BUCKET" ] && [ "$PLUGIN_BUCKET" != "None" ]; then
  log "Emptying S3 bucket: $PLUGIN_BUCKET"
  aws s3 rm "s3://$PLUGIN_BUCKET" --recursive --region "$REGION" 2>/dev/null || true
fi

# Step 4: CDK destroy all stacks
log "Running cdk destroy --all ..."
cd "$(dirname "$0")/../.."
npx cdk destroy --all --force --app "npx ts-node --prefer-ts-exts cdk/bin/cdk.ts"

log ""
log "✓ Teardown complete. All POC resources removed."
