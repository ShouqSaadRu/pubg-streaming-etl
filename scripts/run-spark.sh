#!/bin/bash
set -e

SPARK_VERSION="4.0.3"

KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VERSION}"
POSTGRES_PACKAGE="org.postgresql:postgresql:42.7.7"

spark-submit \
  --master "local[*]" \
  --packages "${KAFKA_PACKAGE},${POSTGRES_PACKAGE}" \
  spark/combat_stream.py