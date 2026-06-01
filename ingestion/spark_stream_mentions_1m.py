"""
File: ingestion/spark_stream_mentions_1m.py
Author: Santiago (P1 hot-path) + Albert (P2 wiring)
Created: 2026-05-11
Updated: 2026-06-01

Pipeline Stage: P1/P2 — Hot-Path Streaming Aggregator

Description
-----------
Spark Structured Streaming job. Reads the `character-mentions` Kafka topic
(produced by ingestion/stream_producer.py), windows mentions into 1-minute
tumbling buckets keyed by (character_name, domain), and writes the aggregate
as Parquet. The output directory is read by:

  - the Exploitation Zone (exposed as `fact_mentions_1m` view in exploit.duckdb)
  - the Streamlit dashboard (live tile)

Output schema
-------------
    window_start    TIMESTAMP    start of the 1-minute window
    window_end      TIMESTAMP    end of the 1-minute window
    character_name  STRING       canonical figure key (matches dim_figure.figure_id)
    domain          STRING       philosophy | physics | art | literature
    mention_count   BIGINT       number of mentions in the window
    avg_sentiment   DOUBLE       mean sentiment_score in the window
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("STREAM_TOPIC", "character-mentions")

OUTPUT_PATH = os.getenv(
    "STREAM_OUTPUT_PATH",
    "/opt/airflow/streaming/fact_mentions_1m",
)
CHECKPOINT_PATH = os.getenv(
    "STREAM_CHECKPOINT_PATH",
    "/opt/airflow/streaming/checkpoints/fact_mentions_1m",
)

# ─── Spark Session ────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("character_mentions_1m_stream")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ─── Schema (matches ingestion/stream_producer.py) ────────────────────────────
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("character_name", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("message", StringType(), True),
    StructField("sentiment_score", DoubleType(), True),
    StructField("source", StringType(), True),
])

# ─── Read from Kafka ──────────────────────────────────────────────────────────
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
)

# ─── Aggregate: 1-minute tumbling windows, keyed by (character, domain) ───────
mentions_1m = (
    parsed_stream
    .withWatermark("event_time", "30 seconds")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("character_name"),
        col("domain"),
    )
    .agg(
        count("*").alias("mention_count"),
        avg("sentiment_score").alias("avg_sentiment"),
    )
)

# ─── Write to Parquet ─────────────────────────────────────────────────────────
query = (
    mentions_1m
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("character_name"),
        col("domain"),
        col("mention_count"),
        col("avg_sentiment"),
    )
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

query.awaitTermination()
