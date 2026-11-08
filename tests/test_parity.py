import pandas as pd
import pyarrow.parquet as pq
import pytest
import sqlglot

from dashboard import parity
from gtfsrt.warehouse import loader

T0 = pd.Timestamp("2026-11-08 17:00", tz="UTC")


def route_df():
    def r(route, minute, obs, late, date="2026-11-08", loaded=0):
        s = T0 + pd.Timedelta(minutes=minute)
        return {"window_start": s, "window_end": s + pd.Timedelta(minutes=5), "route_id": route, "observations": obs,
                "late_observations": late, "late_ratio": late / obs, "avg_delay_seconds": 100.0, "max_delay_seconds": 600,
                "window_date": date, "curated_at": T0 + pd.Timedelta(minutes=loaded)}
    return pd.DataFrame([r("A", 0, 100, 50), r("A", 5, 100, 30), r("B", 0, 200, 20), r("C", 0, 50, 50),   # C is below min_obs
                         r("A", 0, 100, 50, loaded=9),                                                      # replayed duplicate of A@0
                         r("A", 0, 100, 99, date="2026-11-07")])                                            # another day


def test_export_writes_the_layout_the_copy_statement_reads_without_the_partition_column(tmp_path):
    out = parity.export_partitions({"route_delay_5m": route_df()}, tmp_path, "2026-11-08")
    (e,) = out
    assert e.path == tmp_path / "route_delay_5m" / "window_date=2026-11-08" / "part-00000.parquet"
    t = pq.read_table(e.path)
    assert "window_date" not in t.column_names and t.num_rows == e.rows == 5   # the other day's row is not exported


def test_export_skips_empty_inputs_and_rejects_a_bad_date(tmp_path):
    assert parity.export_partitions({"alerts": pd.DataFrame()}, tmp_path, "2026-11-08") == []
    assert parity.export_partitions({"route_delay_5m": route_df()}, tmp_path, "2030-01-01") == []
    with pytest.raises(ValueError):
        parity.export_partitions({"route_delay_5m": route_df()}, tmp_path, "2026-11-08'; DROP TABLE x;--")


def test_expected_route_is_deduplicated_observation_weighted_and_filters_thin_routes():
    e = parity.expected_route(route_df(), "2026-11-08", min_obs=100)
    assert e["rows"] == 4 and e["observations"] == 450 and e["late"] == 150 and e["routes"] == 3   # the replayed duplicate counts once
    assert [t[0] for t in e["top"]] == ["A", "B"]                      # C has only 50 readings
    assert e["top"][0][1] == pytest.approx(80 / 200) and e["top"][0][2] == 200


def test_compare_route_flags_each_kind_of_disagreement():
    e = parity.expected_route(route_df(), "2026-11-08", min_obs=100)
    totals, top = (4, 450, 150, 3), [("A", 0.4, 200), ("B", 0.1, 200)]
    assert all(c.ok for c in parity.compare_route(e, totals, top))
    assert not parity.compare_route(e, (4, 451, 150, 3), top)[1].ok                       # observation total off by one
    assert not parity.compare_route(e, totals, [("B", 0.1, 200), ("A", 0.4, 200)])[-1].ok  # same rows, wrong order
    assert not parity.compare_route(e, totals, [("A", 0.4001, 200), ("B", 0.1, 200)])[-1].ok  # share off by 0.0001
    assert not parity.compare_route(e, totals, top[:1])[-1].ok                              # a route missing


def test_expected_alerts_counts_rows_once_and_collapses_episodes():
    def a(alert_id, typ, minute, date="2026-11-08"):
        return {"alert_id": alert_id, "alert_type": typ, "trip_key": "T1", "episode": 1, "route_id": "R", "tier": 0,
                "delay_minutes": 6.0, "alert_ts": T0 + pd.Timedelta(minutes=minute), "alert_date": date}
    df = pd.DataFrame([a("1", "DELAY_STARTED", 0), a("1", "DELAY_STARTED", 0), a("2", "DELAY_RECOVERED", 10)])
    e = parity.expected_alerts(df, "2026-11-08")
    assert e == {"rows": 2, "episodes": 1}
    assert all(c.ok for c in parity.compare_alerts(e, 2, 1)) and not parity.compare_alerts(e, 3, 1)[0].ok


def test_generated_sql_is_valid_snowflake_and_injection_safe():
    for sql in (parity.route_totals_sql("2026-11-08"), parity.route_top_sql("2026-11-08"), parity.alert_count_sql("2026-11-08"), parity.EPISODES_SQL):
        sqlglot.parse_one(sql, dialect="snowflake")
    with pytest.raises(sqlglot.errors.ParseError):                                    # negative control: the parser really can fail
        sqlglot.parse_one("SELEC FROM WHERE ((", dialect="snowflake")
    with pytest.raises(ValueError):
        parity.route_totals_sql("2026-11-08' OR '1'='1")


def test_copy_sql_accepts_an_internal_stage_and_rejects_a_hostile_one():
    sql = loader.copy_sql(loader.ROUTE_DELAY, "2026-11-08", stage="@GTFSR.RAW.SERVING_STAGE_LOCAL")
    assert "@GTFSR.RAW.SERVING_STAGE_LOCAL/route_delay_5m/window_date=2026-11-08/" in sql and "SERVING_STAGE/" not in sql
    sqlglot.parse_one(sql, dialect="snowflake")
    for bad in ("GTFSR.RAW.STAGE", "@a b", "@x; DROP TABLE y", "@a.b.c.d"):
        with pytest.raises(ValueError):
            loader.copy_sql(loader.ROUTE_DELAY, "2026-11-08", stage=bad)
    assert "@GTFSR.RAW.SERVING_STAGE/" in loader.copy_sql(loader.ROUTE_DELAY, "2026-11-08")   # default unchanged


def test_ties_in_late_share_are_ordered_by_more_evidence_then_name():
    def r(route, obs, late):
        s = T0
        return {"window_start": s, "window_end": s + pd.Timedelta(minutes=5), "route_id": route, "observations": obs,
                "late_observations": late, "late_ratio": late / obs, "avg_delay_seconds": 1.0, "max_delay_seconds": 1,
                "window_date": "2026-11-08", "curated_at": T0}
    df = pd.DataFrame([r("B", 200, 80), r("A", 200, 80), r("C", 400, 160), r("D", 300, 120)])   # all exactly 40% late
    assert [t[0] for t in parity.expected_route(df, "2026-11-08", min_obs=100)["top"]] == ["C", "D", "A", "B"]


def test_a_wrong_reading_count_in_the_top_list_is_a_failure_even_if_route_and_share_match():
    e = parity.expected_route(route_df(), "2026-11-08", min_obs=100)
    wrong_count = [("A", 0.4, 201), ("B", 0.1, 200)]
    assert not parity.compare_route(e, (4, 450, 150, 3), wrong_count)[-1].ok


def test_exported_timestamps_are_microseconds_like_the_real_glue_output(tmp_path):
    import pyarrow as pa

    (e,) = parity.export_partitions({"route_delay_5m": route_df()}, tmp_path, "2026-11-08")
    schema = pq.read_schema(e.path)
    for col in ("window_start", "window_end", "curated_at"):
        assert pa.types.is_timestamp(schema.field(col).type) and schema.field(col).type.unit == "us", col
