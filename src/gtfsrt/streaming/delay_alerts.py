"""Streaming job #3 (HOT PATH): per-trip stateful delay detection  ->  Kafka alerts topic.

All business rules are in gtfsrt.alerting (pure Python, unit-tested). This file only:
  * groups rows by trip_key,
  * hands (state, ordered observations) to the pure functions,
  * persists the returned state,
  * arms an event-time timeout so silent trips get closed out.

Delivery: Spark's Kafka sink is AT-LEAST-ONCE. Duplicates after a crash are harmless
because alert_id is deterministic and the consumer writes idempotently (decision D0.5).
"""
from __future__ import annotations

import os
from typing import Iterator

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from gtfsrt.alerting import Observation, TripState, expire, process_batch
from gtfsrt.config import AlertConfig, Topics
from gtfsrt.streaming.common import (
    ALERT_SCHEMA, bootstrap_servers, build_spark, checkpoint, parse_trip_updates,
    read_kafka, trigger_seconds,
)

CFG = AlertConfig.from_env()
EXPIRY_MS = int(os.getenv("ALERT_EXPIRY_SECONDS", "1800")) * 1000  # silent this long -> trip is over
WATERMARK = os.getenv("WATERMARK_DELAY", "2 minutes")

_ALERT_COLS = [f.name for f in ALERT_SCHEMA.fields]


def _to_frame(alerts: list[dict]) -> pd.DataFrame:
    # An empty frame with the right columns is a valid "emit nothing" result.
    return pd.DataFrame(alerts, columns=_ALERT_COLS).astype(object).where(lambda d: d.notna(), None)


def update_trip(
    key: tuple, pdf_iter: Iterator[pd.DataFrame], state: GroupState
) -> Iterator[pd.DataFrame]:
    # --- timeout path: no updates for EXPIRY -> close any open episode, drop the state ---
    if state.hasTimedOut:
        old = TripState.from_tuple(state.get)
        _, alerts = expire(old, CFG)
        state.remove()
        yield _to_frame(alerts)
        return

    current = TripState.from_tuple(state.get) if state.exists else None

    observations = []
    for pdf in pdf_iter:  # a group may arrive as several pandas chunks
        for r in pdf.itertuples(index=False):
            observations.append(
                Observation(
                    trip_id=r.trip_id,
                    start_date=r.start_date,
                    event_time=int(r.event_time),
                    delay_seconds=None if pd.isna(r.current_delay_seconds) else int(r.current_delay_seconds),
                    route_id=r.route_id if isinstance(r.route_id, str) else None,
                    vehicle_id=r.vehicle_id if isinstance(r.vehicle_id, str) else None,
                    next_stop_id=r.next_stop_id if isinstance(r.next_stop_id, str) else None,
                )
            )

    new_state, alerts = process_batch(current, observations, CFG)
    state.update(new_state.to_tuple())

    # Event-time timeouts must be strictly greater than the current watermark.
    timeout_ms = max(new_state.last_event_time * 1000 + EXPIRY_MS, state.getCurrentWatermarkMs() + 1)
    state.setTimeoutTimestamp(timeout_ms)
    yield _to_frame(alerts)


def main() -> None:
    spark = build_spark("delay-alerts")
    topics = Topics.from_env()

    events = (
        parse_trip_updates(read_kafka(spark, topics.trip_updates))
        # Watermark is REQUIRED for event-time timeouts; it also bounds how late data may be.
        .withWatermark("event_ts", WATERMARK)
        .select("trip_key", "trip_id", "start_date", "event_time", "current_delay_seconds",
                "route_id", "vehicle_id", "next_stop_id", "event_ts")
    )

    alerts = events.groupBy("trip_key").applyInPandasWithState(
        update_trip,
        outputStructType=ALERT_SCHEMA,
        stateStructType=TripState.SPARK_SCHEMA,
        outputMode="append",
        timeoutConf=GroupStateTimeout.EventTimeTimeout,
    )

    out = alerts.select(
        F.col("trip_key").alias("key"),  # same key -> same partition -> alerts for a trip stay ordered
        F.to_json(F.struct(*[F.col(c) for c in ALERT_SCHEMA.fieldNames()])).alias("value"),
    )

    query = (
        out.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers())
        .option("topic", topics.delay_alerts)
        .option("checkpointLocation", checkpoint("delay_alerts"))
        .outputMode("append")
        .trigger(processingTime=trigger_seconds(5))  # hot path: low latency
        .queryName("delay_alerts")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
