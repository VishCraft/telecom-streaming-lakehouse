"""Kafka to Delta Lake streaming pipeline for synthetic telecom events."""
from __future__ import annotations

import os
from pyspark.sql import SparkSession, functions as F, types as T

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "network-events")
DATA_DIR = os.getenv("DATA_DIR", "/data")

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("region", T.StringType()),
    T.StructField("node_id", T.StringType()),
    T.StructField("metric", T.StringType()),
    T.StructField("value", T.DoubleType()),
])
SILVER_SCHEMA = T.StructType(EVENT_SCHEMA.fields + [T.StructField("ingested_at", T.TimestampType())])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("telecom-streaming-lakehouse")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def source_stream(spark: SparkSession):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def with_quality_status(kafka_df):
    decoded = kafka_df.select(
        F.col("value").cast("string").alias("payload"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("topic"), F.col("partition"), F.col("offset"),
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
    )
    parsed = decoded.select("payload", "kafka_timestamp", "topic", "partition", "offset", "event.*")
    required = (
        F.col("event_id").isNotNull() & (F.length(F.trim("event_id")) > 0)
        & F.col("event_time").isNotNull()
        & F.col("region").isNotNull() & (F.length(F.trim("region")) > 0)
        & F.col("node_id").isNotNull() & (F.length(F.trim("node_id")) > 0)
        & F.col("metric").isNotNull() & F.col("value").isNotNull()
    )
    metric_ok = (
        ((F.col("metric") == "rssi") & F.col("value").between(-200, 0))
        | ((F.col("metric") == "latency_ms") & F.col("value").between(0, 60000))
        | ((F.col("metric") == "throughput_mbps") & F.col("value").between(0, 1000000))
    )
    return parsed.withColumn(
        "quality_status",
        F.when(~required, F.lit("missing_or_malformed_required_field"))
        .when(~metric_ok, F.lit("unsupported_metric_or_out_of_range"))
        .otherwise(F.lit("valid")),
    ).withColumn("ingested_at", F.current_timestamp())


def ensure_silver_delta_table(spark: SparkSession, silver_path: str) -> None:
    """Create an empty Delta table so the gold streaming reader can start cleanly."""
    if not os.path.exists(os.path.join(silver_path, "_delta_log")):
        spark.createDataFrame([], SILVER_SCHEMA).write.format("delta").mode("overwrite").save(silver_path)


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    raw_path = f"{DATA_DIR}/bronze/events"
    silver_path = f"{DATA_DIR}/silver/events"
    quarantine_path = f"{DATA_DIR}/quarantine/events"
    gold_path = f"{DATA_DIR}/gold/kpi_1m"
    checkpoints = f"{DATA_DIR}/checkpoints"

    # Bronze preserves the source payload and Kafka coordinates for replay and audit.
    raw = source_stream(spark).select(
        F.col("value").cast("string").alias("payload"),
        F.col("topic"), F.col("partition"), F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    bronze_query = (
        raw.writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", f"{checkpoints}/bronze")
        .queryName("bronze-events").start(raw_path)
    )

    checked = with_quality_status(source_stream(spark))
    invalid_query = (
        checked.filter(F.col("quality_status") != "valid")
        .writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", f"{checkpoints}/quarantine")
        .queryName("quarantine-events").start(quarantine_path)
    )
    valid = (
        checked.filter(F.col("quality_status") == "valid")
        .withWatermark("event_time", "2 minutes")
        .dropDuplicates(["event_id"])
        .select(*[F.col(name) for name in EVENT_SCHEMA.fieldNames()], "ingested_at")
    )
    ensure_silver_delta_table(spark, silver_path)
    silver_query = (
        valid.writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", f"{checkpoints}/silver")
        .queryName("silver-events").start(silver_path)
    )

    # Aggregate the deduplicated silver table to avoid counting repeated event IDs in gold.
    silver_stream = spark.readStream.format("delta").schema(SILVER_SCHEMA).load(silver_path)
    gold = (
        silver_stream.withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", "1 minute").alias("event_window"), "region", "metric")
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.avg("value"), 3).alias("avg_value"),
            F.round(F.min("value"), 3).alias("min_value"),
            F.round(F.max("value"), 3).alias("max_value"),
        )
        .select(
            F.col("event_window.start").alias("window_start"),
            F.col("event_window.end").alias("window_end"),
            "region", "metric", "event_count", "avg_value", "min_value", "max_value",
        )
    )
    gold_query = (
        gold.writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", f"{checkpoints}/gold")
        .queryName("gold-kpi-1m").start(gold_path)
    )

    print(f"Streaming {TOPIC} from {BOOTSTRAP}; Delta output root: {DATA_DIR}")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
