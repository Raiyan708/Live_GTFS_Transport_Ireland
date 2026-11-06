"""Streaming job #1: Kafka  ->  S3 raw zone (lossless).

Delivery: file sink + checkpoint  =  effectively exactly-once into S3
(Spark writes a manifest of committed files; a replayed batch overwrites the same files).
"""
from __future__ import annotations

from pyspark.sql import functions as F

from gtfsrt.streaming.common import (
    build_spark, checkpoint, read_kafka, s3_uri, trigger_seconds,
)
from gtfsrt.config import Topics


def main() -> None:
    spark = build_spark("raw-sink")
    topics = Topics.from_env()
    src = read_kafka(spark, f"{topics.trip_updates},{topics.vehicle_positions}")

    raw = src.select(
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("value_json"),  # original payload, untouched
    )
    # Partition by ARRIVAL time (Kafka timestamp), not event time: late events can never
    # reopen a closed partition, so downstream loads of past hours are stable.
    raw = (
        raw.withColumn("ingest_date", F.date_format("kafka_ts", "yyyy-MM-dd"))
        .withColumn("ingest_hour", F.date_format("kafka_ts", "HH"))
    )

    query = (
        raw.writeStream.format("parquet")
        .option("path", s3_uri("raw"))
        .option("checkpointLocation", checkpoint("raw_sink"))
        .partitionBy("topic", "ingest_date", "ingest_hour")
        .outputMode("append")
        .trigger(processingTime=trigger_seconds(30))
        .queryName("raw_sink")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
