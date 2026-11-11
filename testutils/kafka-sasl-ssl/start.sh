#!/usr/bin/env bash
# Formats the storage on the first start of the container, then starts Kafka.
set -euo pipefail

CONFIG=/opt/kafka/config/sasl-ssl/server.properties

/opt/kafka/bin/kafka-storage.sh format --cluster-id "$CLUSTER_ID" --config "$CONFIG" --ignore-formatted
exec /opt/kafka/bin/kafka-server-start.sh "$CONFIG"
