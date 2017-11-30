#!/usr/bin/env bash
kafka-topics --create --topic reviews \
  --zookeeper localhost:2181 \
  --partitions 3 \
  --replication-factor 1
