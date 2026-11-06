"""Streaming job #2 (COLD PATH): 5-minute tumbling-window delay statistics per route  ->  S3 curated.

Event-time semantics:
  * window is over event_ts (when the vehicle reported), not when Spark saw it
  * 2-minute watermark: windows are finalised once max(event_ts) - 2min passes their end
  * append mode: each window row is written exactly once, when final (file sinks require append)
Known limitation (see DECISION_LOG): metrics are per OBSERVATION, not per distinct trip.
"""
from __future__ import annotations

import os

from pyspark.sql import functions as F

from gtfsrt.config import AlertConfig, Topics
from gtfsrt.streaming.common import (
    build_spark, checkpoint, parse_trip_updates, read_kafka, s3_uri, trigger_seconds,
)

WATERMARK = os.getenv("WATERMARK_DELAY", "2 minutes")
WINDOW = os.getenv("AGG_WINDOW", "5 minutes")


def main() -> None:
    spark = build_spark("route-aggregates")
    topics = Topics.from_env()
    on_threshold = AlertConfig.from_env().on_seconds

    events = (
        parse_trip_updates(read_kafka(spark, topics.trip_updates))
        # Only meaningful delay readings: cancelled/no-delay rows would distort averages.
        .where((F.col("schedule_relationship") == "SCHEDULED")
               & F.col("current_delay_seconds").isNotNull()
               & F.col("route_id").isNotNull())
        .withWatermark("event_ts", WATERMARK)
        # Producer restarts / retries can re-send the same reading. event_ts MUST be in the key:
        # that is what lets Spark evict old keys once the watermark passes (else state grows forever).
        .dropDuplicates(["start_date", "trip_id", "event_ts"])
    )

    agg = (
        events.groupBy(F.window("event_ts", WINDOW).alias("w"), F.col("route_id"))
        .agg(
            F.count("*").alias("observations"),
            F.approx_count_distinct("trip_key").alias("trips_observed"),
            F.round(F.avg("current_delay_seconds"), 1).alias("avg_delay_seconds"),
            F.max("current_delay_seconds").alias("max_delay_seconds"),
            F.percentile_approx("current_delay_seconds", 0.95).alias("p95_delay_seconds"),
            F.sum(F.when(F.col("current_delay_seconds") >= on_threshold, 1).otherwise(0)).alias("late_observations"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "route_id", "observations", "trips_observed", "avg_delay_seconds",
            "max_delay_seconds", "p95_delay_seconds", "late_observations",
            (F.col("late_observations") / F.col("observations")).alias("late_ratio"),
        )
        .withColumn("window_date", F.date_format("window_start", "yyyy-MM-dd"))
    )

    query = (
        agg.writeStream.format("parquet")
        .option("path", s3_uri("curated", "route_delay_5m"))
        .option("checkpointLocation", checkpoint("route_aggregates"))
        .partitionBy("window_date")
        .outputMode("append")
        .trigger(processingTime=trigger_seconds(30))
        .queryName("route_aggregates")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
