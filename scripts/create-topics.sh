#!/usr/bin/env bash
# Creates the Kafka topics used by this demo.
# Run after `docker compose up -d` and once the kafka container is healthy.
set -euo pipefail

BOOTSTRAP_SERVER="localhost:9092"
KAFKA_TOPICS_SH="/opt/kafka/bin/kafka-topics.sh"

create_topic() {
  local topic_name=$1
  local partitions=$2
  local replication=$3

  echo "Creating topic '${topic_name}' (partitions=${partitions}, replication=${replication})..."
  docker exec kafka "${KAFKA_TOPICS_SH}" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create --if-not-exists \
    --topic "${topic_name}" \
    --partitions "${partitions}" \
    --replication-factor "${replication}"
}

# raw car GPS pings, keyed by carId
create_topic "car-gps-data" 3 1

# windowed per-car speed aggregates emitted by the Kafka Streams app
create_topic "car-speed-stats" 3 1

echo ""
echo "Current topics:"
docker exec kafka "${KAFKA_TOPICS_SH}" --bootstrap-server "${BOOTSTRAP_SERVER}" --list
