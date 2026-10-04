#!/usr/bin/env bash
# End-to-end demo in a single terminal: provision topics, publish a run of
# readings, then consume until the topic is drained and print the summary.
#
#   Local broker:  docker compose up -d && npm run demo
#   Hosted broker: put the credentials in .env, then npm run demo
set -euo pipefail

cd "$(dirname "$0")/.."

# Load .env into the environment first so the defaults below only apply to
# variables the user has not configured.
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

: "${KAFKA_BROKERS:=localhost:9092}"
: "${KAFKA_SSL:=false}"
: "${SENSOR_COUNT:=4}"
: "${READINGS_PER_SENSOR:=15}"
: "${PUBLISH_INTERVAL_MS:=250}"
: "${CONSUMER_IDLE_TIMEOUT_MS:=5000}"
# Stable group => each demo run only sees the records it just published.
: "${CONSUMER_GROUP:=sensor-analytics-demo}"
export KAFKA_BROKERS KAFKA_SSL SENSOR_COUNT READINGS_PER_SENSOR PUBLISH_INTERVAL_MS \
  CONSUMER_IDLE_TIMEOUT_MS CONSUMER_GROUP

echo "==> broker: $KAFKA_BROKERS (ssl=$KAFKA_SSL), consumer group: $CONSUMER_GROUP"

node scripts/create-topics.js

echo "==> producing $((SENSOR_COUNT * READINGS_PER_SENSOR)) readings"
node src/producer.js

echo "==> consuming until idle (${CONSUMER_IDLE_TIMEOUT_MS}ms)"
node src/consumer.js

echo "==> artefacts"
ls -l output/