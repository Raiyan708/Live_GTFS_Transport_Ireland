"""Curated -> serving transforms (plain PySpark; NO awsglue import so they are testable anywhere).

What these fix (see DECISION_LOG Phase 3):
  * dedupe on natural keys  (restarts / replays can emit a row twice)
  * conform: Europe/Dublin local time, peak flag, severity label, minutes
  * compact: caller repartitions to ~1 file per date partition (streaming leaves many tiny files)
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

LOCAL_TZ = "Europe/Dublin"  # delays matter to people in LOCAL time (UTC would mislabel BST rush hour)
PEAK_HOURS = ((7, 9), (16, 18))  # inclusive local-hour ranges: morning and evening commute


def _is_peak(hour_col):
    cond = F.lit(False)
    for lo, hi in PEAK_HOURS:
        cond = cond | hour_col.between(lo, hi)
    return cond


def curate_route_delay(df: DataFrame) -> DataFrame:
    """One row per (window_start, route_id). If duplicates exist keep the most-observed row."""
    ranked = df.withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("window_start", "route_id").orderBy(
                F.col("observations").desc(), F.col("window_end").desc()
            )
        ),
    ).where("_rn = 1").drop("_rn")

    local = F.from_utc_timestamp("window_start", LOCAL_TZ)
    return ranked.select(
        "window_start",
        "window_end",
        "route_id",
        "observations",
        "trips_observed",
        "avg_delay_seconds",
        # Negative = early. Kept as-is: early running is real signal, not an error to clip.
        F.round(F.col("avg_delay_seconds") / 60.0, 2).alias("avg_delay_minutes"),
        "max_delay_seconds",
        "p95_delay_seconds",
        "late_observations",
        "late_ratio",
        F.to_date(local).alias("local_date"),
        F.hour(local).alias("local_hour"),
        F.dayofweek(local).alias("local_day_of_week"),  # 1=Sunday .. 7=Saturday (Spark convention)
        _is_peak(F.hour(local)).alias("is_peak"),
        F.current_timestamp().alias("curated_at"),
        "window_date",
    )


def curate_alerts(df: DataFrame) -> DataFrame:
    """One row per alert_id (idempotent consumers already assume this)."""
    ranked = df.withColumn(
        "_rn", F.row_number().over(Window.partitionBy("alert_id").orderBy(F.col("alert_ts").asc()))
    ).where("_rn = 1").drop("_rn")

    local = F.from_utc_timestamp("alert_ts", LOCAL_TZ)
    severity = (
        F.when(F.col("threshold_seconds") >= 900, "CRITICAL")
        .when(F.col("threshold_seconds") >= 600, "SEVERE")
        .when(F.col("threshold_seconds").isNotNull(), "MODERATE")
        .otherwise(None)
    )
    return ranked.select(
        "alert_id", "alert_type", "trip_key", "trip_id", "start_date", "route_id", "vehicle_id",
        "delay_seconds",
        F.round(F.col("delay_seconds") / 60.0, 2).alias("delay_minutes"),
        "tier", "threshold_seconds",
        severity.alias("severity"),
        "episode", "event_time", "alert_ts", "next_stop_id", "reason",
        F.to_date(local).alias("local_date"),
        F.hour(local).alias("local_hour"),
        _is_peak(F.hour(local)).alias("is_peak"),
        F.current_timestamp().alias("curated_at"),
        "alert_date",
    )


def filter_dates(df: DataFrame, col: str, dates: list[str]) -> DataFrame:
    """Partition pruning: Spark pushes this down so only the requested date folders are read."""
    return df.where(F.col(col).cast("string").isin(dates))
