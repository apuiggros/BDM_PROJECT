from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "character-mentions"

spark = (
    SparkSession.builder
    .appName("character_mentions_1m_stream")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("character_name", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("message", StringType(), True),
    StructField("sentiment_score", DoubleType(), True),
    StructField("source", StringType(), True),
])

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

mentions_1m = (
    parsed_stream
    .withWatermark("event_time", "30 seconds")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("character_name"),
        col("domain")
    )
    .agg(
        count("*").alias("mention_count"),
        avg("sentiment_score").alias("avg_sentiment")
    )
)

query = (
    mentions_1m
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("character_name"),
        col("domain"),
        col("mention_count"),
        col("avg_sentiment")
    )
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", "/app/trusted/streaming/fact_mentions_1m")
    .option("checkpointLocation", "/app/trusted/streaming/checkpoints/fact_mentions_1m")
    .start()
)

query.awaitTermination()
