"""Streaming job #4: alerts topic  ->  S3 curated/alerts  (cold-path history of the hot-path alerts).

Why a separate job instead of writing to S3 from the alert detector?
  * Failure isolation: a slow/broken S3 must never delay a rider-facing alert.
  * The alerts topic becomes the single source of truth; the warehouse is just another consumer.
Idempotence: dropDuplicates on alert_id (+ watermark) removes at-least-once duplicates.
"""
from __future__ import annotations

import os

from pyspark.sql import functions as F

from gtfsrt.config import Topics
from gtfsrt.streaming.common import (
    build_spark, checkpoint, parse_alerts, read_kafka, s3_uri, trigger_seconds,
)

WATERMARK = os.getenv("ALERT_SINK_WATERMARK", "10 minutes")


def main() -> None:
    spark = build_spark("alerts-sink")
    topics = Topics.from_env()

    alerts = (
        parse_alerts(read_kafka(spark, topics.delay_alerts))
        .withWatermark("alert_ts", WATERMARK)
        # alert_ts is part of the key so the watermark can evict dedup state (same reason as route_aggregates)
        .dropDuplicates(["alert_id", "alert_ts"])
        .withColumn("alert_date", F.date_format("alert_ts", "yyyy-MM-dd"))
    )

    query = (
        alerts.writeStream.format("parquet")
        .option("path", s3_uri("curated", "alerts"))
        .option("checkpointLocation", checkpoint("alerts_sink"))
        .partitionBy("alert_date")
        .outputMode("append")
        .trigger(processingTime=trigger_seconds(30))
        .queryName("alerts_sink")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
