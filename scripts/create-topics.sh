#!/bin/bash

set -e

KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"


docker exec kafka "$KAFKA_TOPICS" \
  --bootstrap-server localhost:29092 \
  --create \
  --if-not-exists \
  --topic pubg.combat-events \
  --partitions 3 \
  --replication-factor 1

docker exec kafka "$KAFKA_TOPICS" \
  --bootstrap-server localhost:29092 \
  --create \
  --if-not-exists \
  --topic pubg.match-events \
  --partitions 3 \
  --replication-factor 1

docker exec kafka "$KAFKA_TOPICS" \
  --bootstrap-server localhost:29092 \
  --create \
  --if-not-exists \
  --topic pubg.player-events \
  --partitions 3 \
  --replication-factor 1

docker exec kafka "$KAFKA_TOPICS" \
  --bootstrap-server localhost:29092 \
  --create \
  --if-not-exists \
  --topic pubg.dead-letter \
  --partitions 1 \
  --replication-factor 1

echo "Topics created."
