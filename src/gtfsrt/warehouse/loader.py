"""Idempotent partition-replace load: S3 serving/<table>/<part>=<date>/  ->  Snowflake RAW.<table>.

Mirrors Glue's "overwrite this date partition" contract, so the pair is rerunnable:

    BEGIN
    DELETE FROM <table> WHERE <part_col> = <date>
    COPY INTO <table> FROM (SELECT ... FROM @stage/<table>/<part>=<date>/)  FORCE = TRUE
    INSERT INTO LOAD_AUDIT ...
    COMMIT            (any failure -> ROLLBACK, so a failed COPY never leaves the day deleted)

STATUS: statements are syntax-checked in tests (sqlglot, Snowflake dialect) but NOT executed
against a real Snowflake account.  [UNVERIFIED]
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Sequence

STAGE = "@GTFSR.RAW.SERVING_STAGE"
FILE_FORMAT = "GTFSR.RAW.PARQUET_FF"
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_date(value: str) -> str:
    """COPY's FROM path cannot be a bound parameter, so the date is interpolated into SQL.
    Strict validation here is the injection defence."""
    if not _DATE_RE.match(value):
        raise ValueError(f"invalid date {value!r}; expected YYYY-MM-DD")
    return value


@dataclass(frozen=True)
class TableLoad:
    table: str  # RAW table name, also the folder name under serving/
    part_col: str  # partition column in the table (e.g. WINDOW_DATE)
    part_name: str  # partition folder key (e.g. window_date)
    columns: Sequence[tuple[str, str]]  # (target column, parquet field + cast), loaded from $1


ROUTE_DELAY = TableLoad(
    table="ROUTE_DELAY_5M", part_col="WINDOW_DATE", part_name="window_date",
    columns=[
        ("WINDOW_START", "window_start::TIMESTAMP_NTZ"), ("WINDOW_END", "window_end::TIMESTAMP_NTZ"),
        ("ROUTE_ID", "route_id::VARCHAR"), ("OBSERVATIONS", "observations::NUMBER"),
        ("TRIPS_OBSERVED", "trips_observed::NUMBER"), ("AVG_DELAY_SECONDS", "avg_delay_seconds::FLOAT"),
        ("AVG_DELAY_MINUTES", "avg_delay_minutes::FLOAT"), ("MAX_DELAY_SECONDS", "max_delay_seconds::NUMBER"),
        ("P95_DELAY_SECONDS", "p95_delay_seconds::NUMBER"), ("LATE_OBSERVATIONS", "late_observations::NUMBER"),
        ("LATE_RATIO", "late_ratio::FLOAT"), ("LOCAL_DATE", "local_date::DATE"),
        ("LOCAL_HOUR", "local_hour::NUMBER"), ("LOCAL_DAY_OF_WEEK", "local_day_of_week::NUMBER"),
        ("IS_PEAK", "is_peak::BOOLEAN"), ("CURATED_AT", "curated_at::TIMESTAMP_NTZ"),
    ],
)

ALERTS = TableLoad(
    table="ALERTS", part_col="ALERT_DATE", part_name="alert_date",
    columns=[
        ("ALERT_ID", "alert_id::VARCHAR"), ("ALERT_TYPE", "alert_type::VARCHAR"),
        ("TRIP_KEY", "trip_key::VARCHAR"), ("TRIP_ID", "trip_id::VARCHAR"), ("START_DATE", "start_date::VARCHAR"),
        ("ROUTE_ID", "route_id::VARCHAR"), ("VEHICLE_ID", "vehicle_id::VARCHAR"),
        ("DELAY_SECONDS", "delay_seconds::NUMBER"), ("DELAY_MINUTES", "delay_minutes::FLOAT"),
        ("TIER", "tier::NUMBER"), ("THRESHOLD_SECONDS", "threshold_seconds::NUMBER"),
        ("SEVERITY", "severity::VARCHAR"), ("EPISODE", "episode::NUMBER"), ("EVENT_TIME", "event_time::NUMBER"),
        ("ALERT_TS", "alert_ts::TIMESTAMP_NTZ"), ("NEXT_STOP_ID", "next_stop_id::VARCHAR"),
        ("REASON", "reason::VARCHAR"), ("LOCAL_DATE", "local_date::DATE"), ("LOCAL_HOUR", "local_hour::NUMBER"),
        ("IS_PEAK", "is_peak::BOOLEAN"), ("CURATED_AT", "curated_at::TIMESTAMP_NTZ"),
    ],
)

TABLES = {t.table: t for t in (ROUTE_DELAY, ALERTS)}


def delete_sql(t: TableLoad, date: str) -> str:
    validate_date(date)
    return f"DELETE FROM GTFSR.RAW.{t.table} WHERE {t.part_col} = '{date}'"


def copy_sql(t: TableLoad, date: str) -> str:
    validate_date(date)
    targets = ", ".join([c for c, _ in t.columns] + [t.part_col, "_SOURCE_FILE", "_SOURCE_ROW"])
    selects = ", ".join(
        [f"$1:{expr}" for _, expr in t.columns]
        # Spark's partitionBy strips the partition column out of the files: recover it from the path.
        + [f"TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, '{t.part_name}=([0-9-]+)', 1, 1, 'e', 1))",
           "METADATA$FILENAME", "METADATA$FILE_ROW_NUMBER"]
    )
    folder = f"{STAGE}/{t.table.lower()}/{t.part_name}={date}/"
    return (
        f"COPY INTO GTFSR.RAW.{t.table} ({targets}) "
        f"FROM (SELECT {selects} FROM {folder}) "
        f"FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}') "
        # ABORT: a bad file fails the whole statement (-> rollback) instead of silently loading a subset.
        # FORCE: Glue rewrites files each run; idempotence comes from DELETE above, not from load history.
        f"ON_ERROR = ABORT_STATEMENT FORCE = TRUE"
    )


def count_sql(t: TableLoad, date: str) -> str:
    validate_date(date)
    return f"SELECT COUNT(*) FROM GTFSR.RAW.{t.table} WHERE {t.part_col} = '{date}'"


def load_partition(conn: Any, t: TableLoad, date: str, run_id: str) -> int:
    """Run the transaction on one DB-API connection. Returns rows now present for that date."""
    validate_date(date)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        cur.execute(delete_sql(t, date))
        cur.execute(copy_sql(t, date))
        cur.execute(count_sql(t, date))
        rows = int(cur.fetchone()[0])
        cur.execute(
            "INSERT INTO GTFSR.RAW.LOAD_AUDIT (TABLE_NAME, PARTITION_DATE, ROWS_LOADED, RUN_ID) "
            "VALUES (%s, %s, %s, %s)",
            (t.table, date, rows, run_id),
        )
        cur.execute("COMMIT")
        return rows
    except Exception:
        cur.execute("ROLLBACK")
        raise
    finally:
        cur.close()
