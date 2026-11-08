"""The Spark twin of trip_start must agree with the pure-Python function on every kind of input (runs in the Spark container)."""
from pyspark.sql import functions as F

from gtfsrt.streaming.common import only_started_trips, with_scheduled_start
from gtfsrt.trip_start import has_started, scheduled_start_epoch

CASES = [("20261108", "21:35:00"), ("20260701", "08:00:00"), ("20261108", "25:10:00"), ("20260328", "00:30:00"),
         ("20261108", "08:00"), ("20261108", None), (None, "08:00:00"), ("20261308", "08:00:00"), ("2026-11-08", "08:00:00")]


def test_scheduled_start_matches_the_python_function(spark):
    df = spark.createDataFrame(CASES, "start_date string, start_time string")
    rows = with_scheduled_start(df).select("start_date", "start_time", F.unix_timestamp("scheduled_start_ts").alias("epoch")).collect()
    got = {(r.start_date, r.start_time): r.epoch for r in rows}
    for d, t in CASES:
        assert got[(d, t)] == scheduled_start_epoch(d, t), (d, t, got[(d, t)], scheduled_start_epoch(d, t))


def test_only_started_trips_matches_has_started(spark):
    base = scheduled_start_epoch("20261108", "21:35:00")
    rows = [(base - 1, "20261108", "21:35:00"), (base, "20261108", "21:35:00"), (base + 600, "20261108", "21:35:00"),
            (base - 3600, "20261108", None), (base - 3600, "20261108", "garbage")]
    df = spark.createDataFrame(rows, "event_time long, start_date string, start_time string").withColumn("event_ts", F.col("event_time").cast("timestamp"))
    kept = sorted(r.event_time for r in only_started_trips(df).collect())
    expected = sorted(e for e, d, t in rows if has_started(e, d, t))
    assert kept == expected and len(kept) == 4
