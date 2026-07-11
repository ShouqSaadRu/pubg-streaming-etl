#!/bin/bash
set -e
SPARK_VERSION="4.0.3"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VERSION}"
spark-submit \
  --master "local[*]" \
  --packages "${KAFKA_PACKAGE}" \
  spark/combat_stream.py
