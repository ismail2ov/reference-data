#!/bin/bash
set -e

BROKER="broker:29092"

echo "⏳ Waiting for Kafka to be ready at $BROKER ..."

# Wait until Kafka responds
while ! kafka-topics --bootstrap-server $BROKER --list >/dev/null 2>&1; do
  echo "Kafka not available yet. Retrying in 2 seconds..."
  sleep 2
done

echo "✅ Kafka is ready. Creating topics..."

kafka-topics --bootstrap-server $BROKER \
  --create --if-not-exists \
  --topic trades-topic \
  --replication-factor 1 \
  --partitions 1

kafka-topics --bootstrap-server $BROKER \
  --create --if-not-exists \
  --topic enriched-trades-topic \
  --replication-factor 1 \
  --partitions 1

kafka-topics --bootstrap-server $BROKER \
  --create --if-not-exists \
  --topic isin-topic \
  --replication-factor 1 \
  --partitions 1

echo "✅ All topics created successfully"
