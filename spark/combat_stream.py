from __future__ import annotations

import os

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
KAFKA_COMBAT_TOPIC = os.getenv(
    "KAFKA_COMBAT_TOPIC",
    "pubg.combat-events",
)
CHECKPOINT_LOCATION = os.getenv(
    "SPARK_COMBAT_CHECKPOINT",
    "checkpoints/combat-stream",
)

COMBAT_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("match_id", StringType(), False),
        StructField("killer", StringType(), True),
        StructField("killer_account_id", StringType(), True),
        StructField("killer_team_id", IntegerType(), True),
        StructField("victim", StringType(), True),
        StructField("victim_account_id", StringType(), True),
        StructField("victim_team_id", IntegerType(), True),
        StructField("weapon_raw", StringType(), True),
        StructField("damage_reason_raw", StringType(), True),
        StructField("damage_type_raw", StringType(), True),
        StructField("distance_raw", DoubleType(), True),
        StructField("is_headshot", BooleanType(), True),
        StructField("is_suicide", BooleanType(), True),
        StructField("assist_account_ids", ArrayType(StringType()), True),
    ]
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("PUBG Combat Streaming ETL")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_raw_combat_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_COMBAT_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(kafka_stream: DataFrame) -> DataFrame:
    return (
        kafka_stream
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_json"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn(
            "event",
            F.from_json(F.col("raw_json"), COMBAT_EVENT_SCHEMA),
        )
        .select(
            "kafka_key",
            "raw_json",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "event.*",
        )
    )


def transform_events(parsed: DataFrame) -> DataFrame:

    raw_distance = F.col("distance_raw")

    cleaned_weapon = F.regexp_replace(
        F.regexp_replace(F.col("weapon_raw"), r"^Weap", ""),
        r"_C$",
        "",
    )

    return (
        parsed
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("match_id").isNotNull())
        .withColumn("event_timestamp", F.to_timestamp("event_time"))
        .withColumn(
            "is_environmental_death",
            F.col("killer").isNull() | (F.col("killer") == "Environment"),
        )
        .withColumn(
            "weapon_clean",
            F.when(
                F.col("weapon_raw").isNull() | (F.col("weapon_raw") == "Unknown"),
                F.lit(None).cast("string"),
            ).otherwise(cleaned_weapon),
        )
        .withColumn(
            "distance_meters_clean",
            F.when(
                raw_distance < 0,
                F.lit(None).cast("double"),
            ).otherwise(F.round(raw_distance / F.lit(100.0), 2)),
        )
        .withColumn(
            "is_valid_event",
            F.col("event_timestamp").isNotNull() & F.col("victim").isNotNull(),
        )
        .withColumn("processed_at", F.current_timestamp())
        #.withWatermark("event_timestamp", "10 minutes")
        #.dropDuplicatesWithinWatermark(["event_id"])
        .select(
            "event_id",
            "event_type",
            "event_timestamp",
            "match_id",
            "killer",
            "killer_account_id",
            "killer_team_id",
            "victim",
            "victim_account_id",
            "victim_team_id",
            F.col("weapon_raw").alias("weapon_raw"),
            "weapon_clean",
            F.col("distance_raw").alias("distance_raw"),
            "distance_meters_clean",
            F.col("damage_reason_raw").alias("damage_reason_raw"),
            F.col("damage_type_raw").alias("damage_type_raw"),
            "is_headshot",
            "is_suicide",
            "is_environmental_death",
            "assist_account_ids",
            "is_valid_event",
            "processed_at",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
    )


def main() -> None:
    print("1")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("2")
    raw_stream = read_raw_combat_stream(spark)
    print("3")
    print(raw_stream)
    parsed_stream = parse_events(raw_stream)
    print("4")
    print(parsed_stream)
    clean_stream = transform_events(parsed_stream)
    print("5")
    print(clean_stream)
    query = (
        clean_stream.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", "100")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="5 seconds")
        .queryName("pubg_combat_clean_console")
        .start()
    )

    print(
        f"Spark is reading {KAFKA_COMBAT_TOPIC} "
        f"from {KAFKA_BOOTSTRAP_SERVERS}"
    )
    print(f"Checkpoint: {CHECKPOINT_LOCATION}")

    query.awaitTermination()


if __name__ == "__main__":
    main()
