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

# Define where Spark stores streaming progress and state
# so the pipeline can resume after it is restarted.
CHECKPOINT_LOCATION = os.getenv(
    "SPARK_COMBAT_CHECKPOINT",
    "checkpoints/combat-stream",
)


# Define the expected structure and data types
# of the combat-event JSON messages received from Kafka.
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
        StructField(
            "assist_account_ids",
            ArrayType(StringType()),
            True,
        ),
    ]
)


# Create and configure the Spark application that will
# process PUBG combat events as a streaming pipeline.
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("PUBG Combat Streaming ETL")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


# Connect Spark Structured Streaming to Kafka and create
# a streaming DataFrame containing raw Kafka records.
def read_raw_combat_stream(
    spark: SparkSession,
) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option("subscribe", KAFKA_COMBAT_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


# Convert Kafka key and value bytes into strings,
# parse each JSON value using the combat-event schema,
# and preserve Kafka metadata for tracing and debugging.
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
            F.from_json(
                F.col("raw_json"),
                COMBAT_EVENT_SCHEMA,
            ),
        )
        .withColumn(
            "json_parsed_successfully",
            F.col("event").isNotNull(),
        )
        .select(
            "kafka_key",
            "raw_json",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "json_parsed_successfully",
            "event.*",
        )
    )

# Clean, validate, enrich, and deduplicate parsed combat events
# before selecting the final columns used by downstream systems.
def transform_events(
    parsed: DataFrame,
) -> DataFrame:

    raw_distance = F.col("distance_raw")

    # Remove technical PUBG prefixes and suffixes
    # from the raw weapon class name.
    cleaned_weapon = F.regexp_replace(
        F.regexp_replace(
            F.col("weapon_raw"),
            r"^Weap",
            "",
        ),
        r"_C$",
        "",
    )

    return (
        parsed

        .filter(F.col("event_id").isNotNull())
        .filter(F.col("match_id").isNotNull())

        .withColumn(
            "event_timestamp",
            F.to_timestamp("event_time"),
        )

        # Identify deaths caused by the game environment
        # instead of another player.
        .withColumn(
            "is_environmental_death",
            F.col("killer").isNull()
            | (F.col("killer") == "Environment"),
        )

        # Replace unknown weapons with null and clean
        # valid weapon class names.
        .withColumn(
            "weapon_clean",
            F.when(
                F.col("weapon_raw").isNull()
                | (
                    F.col("weapon_raw")
                    == "Unknown"
                ),
                F.lit(None).cast("string"),
            ).otherwise(cleaned_weapon),
        )

        # Replace invalid negative distances with null
        # and convert valid centimeters into meters.
        .withColumn(
            "distance_meters_clean",
            F.when(
                raw_distance < 0,
                F.lit(None).cast("double"),
            ).otherwise(
                F.round(
                    raw_distance / F.lit(100.0),
                    2,
                )
            ),
        )

        # Mark whether the record contains the minimum
        # information required to be considered usable.
        .withColumn(
            "is_valid_event",
            F.col("event_timestamp").isNotNull()
            & F.col("victim").isNotNull(),
        )

        # Record when Spark processed the event.
        .withColumn(
            "processed_at",
            F.current_timestamp(),
        )

        # Allow Spark to keep deduplication state for late events
        # arriving within ten minutes of their event time.
        .withWatermark(
            "event_timestamp",
            "10 minutes",
        )

        # Remove repeated events that have the same event ID
        # within the watermark-managed streaming state.
        .dropDuplicatesWithinWatermark(
            ["event_id"]
        )

        # Select and order the final output columns.
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
            "weapon_raw",
            "weapon_clean",
            "distance_raw",
            "distance_meters_clean",
            "damage_reason_raw",
            "damage_type_raw",
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


# Build the complete streaming pipeline:
# create Spark, read Kafka records, parse JSON,
# transform events, and continuously print the results.
def main() -> None:
    spark = create_spark_session()

    # Reduce unnecessary Spark log output.
    spark.sparkContext.setLogLevel("WARN")

    # Define the pipeline stages.
    raw_stream = read_raw_combat_stream(spark)
    parsed_stream = parse_events(raw_stream)
    clean_stream = transform_events(parsed_stream)

    rejected_stream = get_rejected_events(
        parsed_stream
    )
    
    # Start a streaming query that prints cleaned records
    # to the terminal every five seconds.
    clean_query = (
        clean_stream.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", "100")
        .option(
            "checkpointLocation",
            CHECKPOINT_LOCATION,
        )
        .trigger(processingTime="5 seconds")
        .queryName(
            "pubg_combat_clean_console"
        )
        .start()
    )
    
    rejected_query = (
        rejected_stream.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", "100")
        .option(
            "checkpointLocation",
            "checkpoints/combat-rejected",
        )
        .trigger(processingTime="5 seconds")
        .queryName(
            "pubg_combat_rejected_console"
        )
        .start()
    )

    print(
        f"Spark is reading {KAFKA_COMBAT_TOPIC} "
        f"from {KAFKA_BOOTSTRAP_SERVERS}"
    )
    print(
        f"Checkpoint: {CHECKPOINT_LOCATION}"
    )

    # Keep the application running while Spark waits
    # for and processes new Kafka messages.
    spark.streams.awaitAnyTermination()


# Select malformed or incomplete Kafka records and attach
# a rejection reason so they can be investigated later.
def get_rejected_events(
    parsed: DataFrame,
) -> DataFrame:
    return (
        parsed
        .filter(
            ~F.col("json_parsed_successfully")
            | F.col("event_id").isNull()
            | F.col("match_id").isNull()
        )
        .withColumn(
            "rejection_reason",
            F.when(
                ~F.col("json_parsed_successfully"),
                F.lit("INVALID_JSON_OR_SCHEMA_MISMATCH"),
            )
            .when(
                F.col("event_id").isNull(),
                F.lit("MISSING_EVENT_ID"),
            )
            .when(
                F.col("match_id").isNull(),
                F.lit("MISSING_MATCH_ID"),
            )
            .otherwise(
                F.lit("UNKNOWN_VALIDATION_ERROR")
            ),
        )
        .withColumn(
            "rejected_at",
            F.current_timestamp(),
        )
        .select(
            "raw_json",
            "rejection_reason",
            "rejected_at",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
    )

if __name__ == "__main__":
    main()