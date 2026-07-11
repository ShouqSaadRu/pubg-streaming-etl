# Spark Structured Streaming stage

Copy these files into the root of the existing PUBG project:

```text
spark/
run-spark.sh
requirements-spark.txt
```

## Install PySpark

```bash
source .venv/bin/activate
pip install -r requirements-spark.txt
java -version
```

## Run the Spark ETL stream

Kafka must already be running.

```bash
chmod +x run-spark.sh
./run-spark.sh
```

On the first run, Spark reads from the earliest Kafka offsets. After that, the checkpoint controls where it resumes.

## Produce another random match

In another terminal:

```bash
source .venv/bin/activate
python -m producer.producer
```

## Transformations

- Parse Kafka JSON with an explicit schema
- Convert event time into a timestamp
- Convert source distance from centimetres to metres
- Convert negative distance to null
- Remove `Weap` and `_C` from weapon names
- Detect environmental deaths
- Validate required fields
- Add processing timestamp
- Deduplicate by `event_id`
- Apply a ten-minute event-time watermark
- Preserve Kafka partition, offset, and timestamp

## Reset Spark progress

```bash
rm -rf checkpoints/combat-stream
```
