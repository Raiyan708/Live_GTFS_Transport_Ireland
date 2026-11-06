import os
from datetime import datetime

import pytest

from glue.curate_job import resolve_dates, run
from gtfsrt.transform.curate import curate_alerts, curate_route_delay, filter_dates


def ts(s):  # naive UTC datetime
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


ROUTE_SCHEMA = (
    "window_start timestamp, window_end timestamp, route_id string, observations long, trips_observed long, "
    "avg_delay_seconds double, max_delay_seconds int, p95_delay_seconds int, late_observations long, "
    "late_ratio double, window_date date"
)


def route_rows(spark, rows):
    return spark.createDataFrame(rows, ROUTE_SCHEMA)


def rr(start, route="46A", obs=10, avg=60.0, date=None):
    d = date or start[:10]
    return (ts(start), ts(start[:11] + "00:00:00"), route, obs, 3, avg, 100, 90, 0, 0.0,
            datetime.strptime(d, "%Y-%m-%d").date())


def test_dublin_local_time_respects_daylight_saving(spark):
    # 2026-07-01 07:00 UTC == 08:00 BST (local hour 8, peak)   |   2026-01-15 07:00 UTC == 07:00 GMT
    df = route_rows(spark, [rr("2026-07-01 07:00:00", "SUMMER"), rr("2026-01-15 07:00:00", "WINTER")])
    out = {r.route_id: r for r in curate_route_delay(df).collect()}
    assert out["SUMMER"].local_hour == 8 and out["WINTER"].local_hour == 7
    assert out["SUMMER"].is_peak and out["WINTER"].is_peak


def test_peak_boundaries_are_inclusive_and_midday_is_offpeak(spark):
    # January => Dublin == UTC, so UTC hour == local hour
    hours = {6: False, 7: True, 9: True, 10: False, 15: False, 16: True, 18: True, 19: False}
    df = route_rows(spark, [rr(f"2026-01-15 {h:02d}:00:00", f"R{h}") for h in hours])
    got = {int(r.route_id[1:]): r.is_peak for r in curate_route_delay(df).collect()}
    assert got == hours


def test_duplicate_windows_keep_the_most_observed_row(spark):
    df = route_rows(spark, [rr("2026-01-15 08:00:00", obs=5, avg=10.0), rr("2026-01-15 08:00:00", obs=9, avg=99.0)])
    out = curate_route_delay(df).collect()
    assert len(out) == 1 and out[0].observations == 9 and out[0].avg_delay_seconds == 99.0


def test_negative_delay_is_preserved_not_clipped(spark):
    out = curate_route_delay(route_rows(spark, [rr("2026-01-15 08:00:00", avg=-90.0)])).collect()[0]
    assert out.avg_delay_seconds == -90.0 and out.avg_delay_minutes == -1.5


ALERT_SCHEMA = (
    "alert_id string, alert_type string, trip_key string, trip_id string, start_date string, route_id string, "
    "vehicle_id string, delay_seconds int, tier int, threshold_seconds int, episode int, event_time long, "
    "next_stop_id string, reason string, alert_ts timestamp, alert_date date"
)


def alert_row(aid, thr, when="2026-01-15 08:00:00"):
    t = ts(when)
    return (aid, "DELAY_STARTED", "20260115|T", "T", "20260115", "46A", "V", 700, 1, thr, 1,
            int(t.replace(tzinfo=None).timestamp()), "S1", None, t, t.date())


def test_alert_dedup_and_severity_labels(spark):
    rows = [alert_row("a", 300), alert_row("a", 300), alert_row("b", 600), alert_row("c", 900), alert_row("d", None)]
    out = {r.alert_id: r for r in curate_alerts(spark.createDataFrame(rows, ALERT_SCHEMA)).collect()}
    assert sorted(out) == ["a", "b", "c", "d"]  # 'a' collapsed
    assert (out["a"].severity, out["b"].severity, out["c"].severity, out["d"].severity) == (
        "MODERATE", "SEVERE", "CRITICAL", None)


def test_filter_dates_selects_only_requested(spark):
    df = route_rows(spark, [rr("2026-01-14 08:00:00"), rr("2026-01-15 08:00:00"), rr("2026-01-16 08:00:00")])
    assert filter_dates(df, "window_date", ["2026-01-15"]).count() == 1


def test_resolve_dates_default_is_yesterday_and_today():
    assert resolve_dates("", datetime(2026, 3, 2)) == ["2026-03-01", "2026-03-02"]
    assert resolve_dates("2026-01-01, 2026-01-02") == ["2026-01-01", "2026-01-02"]


def _write_curated(spark, base):
    route_rows(spark, [
        rr("2026-01-14 08:00:00", "A", obs=4), rr("2026-01-14 08:00:00", "A", obs=4),  # duplicate
        rr("2026-01-15 08:00:00", "A", obs=6), rr("2026-01-15 08:05:00", "B", obs=7),
    ]).write.mode("overwrite").partitionBy("window_date").parquet(f"{base}/curated/route_delay_5m")
    spark.createDataFrame([alert_row("x", 300, "2026-01-14 09:00:00"), alert_row("y", 600, "2026-01-15 09:00:00")],
                          ALERT_SCHEMA).write.mode("overwrite").partitionBy("alert_date").parquet(f"{base}/curated/alerts")


def test_job_is_idempotent_and_only_touches_requested_partitions(spark, tmp_path):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    base = f"file://{tmp_path}"
    _write_curated(spark, base)

    s1 = run(spark, base, ["2026-01-14", "2026-01-15"], allow_empty=False)
    assert s1["route_delay_5m_in"] == 4 and s1["route_delay_5m_out"] == 3  # 1 duplicate removed
    assert s1["alerts_out"] == 2

    # rerun only ONE date: must not duplicate, must not delete the other date's output
    s2 = run(spark, base, ["2026-01-15"], allow_empty=False)
    total = spark.read.parquet(f"{base}/serving/route_delay_5m").count()
    assert total == 3 and s2["route_delay_5m_out"] == 2
    assert sorted(os.listdir(f"{tmp_path}/serving/route_delay_5m")) == ["window_date=2026-01-14", "window_date=2026-01-15"]


def test_job_fails_loudly_on_empty_input_unless_allowed(spark, tmp_path):
    base = f"file://{tmp_path}"
    _write_curated(spark, base)
    with pytest.raises(RuntimeError, match="no input rows"):
        run(spark, base, ["2030-01-01"], allow_empty=False)
    assert run(spark, base, ["2030-01-01"], allow_empty=True)["route_delay_5m_in"] == 0


def test_no_success_marker_when_configured(spark, tmp_path):
    spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    base = f"file://{tmp_path}"
    _write_curated(spark, base)
    run(spark, base, ["2026-01-15"], allow_empty=False)
    names = [f for _, _, files in os.walk(f"{tmp_path}/serving") for f in files]
    assert not any(n == "_SUCCESS" for n in names)


def test_manifest_reports_per_date_counts_and_sanitises_run_id(spark, tmp_path):
    import json
    from glue.curate_job import write_manifest
    from gtfsrt.runid import safe_run_id

    base = f"file://{tmp_path}"
    _write_curated(spark, base)
    dates = ["2026-01-14", "2026-01-15"]
    summary = run(spark, base, dates, allow_empty=False)
    assert summary["by_date"]["route_delay_5m"] == {"2026-01-14": 1, "2026-01-15": 2}  # dup removed on the 14th
    rel = write_manifest(base, "scheduled__2026-01-15T10:00:00+00:00", dates, summary)
    assert ":" not in rel and "+" not in rel
    body = json.loads((tmp_path / rel).read_text())
    assert body["rows"]["alerts"] == {"2026-01-14": 1, "2026-01-15": 1}
    assert safe_run_id("") .startswith("manual_")
