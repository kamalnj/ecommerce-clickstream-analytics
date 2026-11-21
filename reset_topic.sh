#!/bin/bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete \
  --topic evenements_web --bootstrap-server localhost:9092 2>/dev/null
sleep 5
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic evenements_web --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092
