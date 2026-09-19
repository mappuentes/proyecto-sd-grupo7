#!/usr/bin/env bash
# Uso: bash create-topics.sh

set -euo pipefail

BROKER_CONTAINER="${BROKER_CONTAINER:-kafka}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"

topic_creation() { docker exec -i "$BROKER_CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" "$@"; }
topic_config() { docker exec -i "$BROKER_CONTAINER" kafka-configs --bootstrap-server "$BOOTSTRAP" "$@"; }

topic_creation --create --if-not-exists \
  --topic satellites.tle.raw \
  --partitions 1 \
  --replication-factor 1

topic_config --alter \
  --entity-type topics \
  --entity-name satellites.tle.raw \
  --add-config cleanup.policy=compact

topic_creation --create --if-not-exists \
  --topic satellites.position \
  --partitions 1 \
  --replication-factor 1

topic_config --alter \
  --entity-type topics \
  --entity-name satellites.position \
  --add-config cleanup.policy=delete,retention.ms=3600000