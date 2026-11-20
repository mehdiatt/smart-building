#!/bin/bash
# Owner: Person B
# Creates the topics defined in docs/interface-contract.md (section 2).
# Run automatically by the kafka-init service in docker-compose.
# Manual run: docker compose run --rm kafka-init

set -e
BOOTSTRAP="${BOOTSTRAP:-kafka:29092}"
KT=/opt/kafka/bin/kafka-topics.sh
RETENTION_MS=86400000   # 24 h

create() {
  local topic=$1 partitions=$2
  $KT --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$topic" --partitions "$partitions" --replication-factor 1 \
      --config retention.ms=$RETENTION_MS
  echo "OK  $topic ($partitions partitions)"
}

create building.telemetry  3
create building.alerts     1
create building.aggregates 3
create building.invalid    1

echo "--- Existing topics ---"
$KT --bootstrap-server "$BOOTSTRAP" --list
