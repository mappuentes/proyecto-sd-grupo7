#!/usr/bin/env bash
#  Uso:  bash create-topics.sh
 
set -euo pipefail
 
BROKER_CONTAINER="${BROKER_CONTAINER:-kafka}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
 
topic_creation() { docker exec -i "$BROKER_CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" "$@"; }
 
#initial topics
topic_creation --create --if-not-exists --topic satellites.tle.raw

topic_creation --create --if-not-exists --topic satellites.position