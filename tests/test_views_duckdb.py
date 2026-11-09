"""Do the Snowflake VIEWS compute the same numbers as the pandas twins in dashboard/views.py?

This executes the real snowflake/03_views.sql, translated to DuckDB's dialect by sqlglot, on engineered data (replayed duplicates,
rank ties, minute-boundary rounding, escalations, open episodes) and compares every view with its pandas twin.

What it proves: the LOGIC of the views (de-duplication, observation weighting, ranking, episode collapsing, freshness).
What it does NOT prove: that Snowflake accepts the SQL, grants, COPY, stages, clustering. Those need a real account
(docs/SNOWFLAKE_RUNBOOK.md). DuckDB is a second SQL engine, not Snowflake.
"""
import pathlib

import duckdb
import pandas as pd
import pytest
import sqlglot

from dashboard import views

ROOT = pathlib.Path(__file__).resolve().parents[1]
NOW = pd.Timestamp.now(tz="UTC").floor("min").tz_localize(None)  # naive UTC, like Snowflake TIMESTAMP_NTZ


def ts(minutes_ago: float = 0, base: pd.Timestamp = NOW) -> pd.Timestamp:
    return base - pd.Timedelta(minutes=minutes_ago)


# ------------------------------------------------------------------------------------------------ engineered data
def window(route, minutes_ago, obs, late, avg, mx, loaded=0, hour=None):
    s = ts(minutes_ago)
    return {"WINDOW_START": s, "WINDOW_END": s + pd.Timedelta(minutes=5), "ROUTE_ID": route, "OBSERVATIONS": obs, "TRIPS_OBSERVED": 5,
            "AVG_DELAY_SECONDS": float(avg), "MAX_DELAY_SECONDS": mx, "LATE_OBSERVATIONS": late, "LATE_RATIO": late / obs,
            "LOCAL_DATE": s.date(), "LOCAL_HOUR": s.hour if hour is None else hour, "IS_PEAK": s.hour in (8, 17),
            "WINDOW_DATE": s.date(), "_LOADED_AT": ts(-loaded)}


def route_frame():
    rows = [
        window("A", 120, 100, 50, 400, 900), window("A", 115, 300, 30, 100, 700), window("A", 110, 50, 5, 60, 300),
        window("A", 120, 100, 99, 999, 9999, loaded=7),          # REPLAY of A@120 with different numbers: the NEWER load must win
        window("B", 100, 200, 40, 200, 500), window("C", 100, 100, 20, 150, 400),   # B and C tie at 20% late: equal rank
        window("D", 60 * 30, 100, 90, 700, 1500),                # 30 hours ago: outside the 24 h ranking window
        window("E", 90, 100, 0, -30, 60),                        # early running, nothing late
    ]
    return pd.DataFrame(rows)


def alert(alert_id, typ, trip, ep, minutes_ago, tier, delay, route="A", loaded=0, peak=False, severity="MODERATE"):
    t = ts(minutes_ago) + pd.Timedelta(seconds=50)  # 50 s into the minute: exposes DATEDIFF boundary-counting vs exact differences
    return {"ALERT_ID": alert_id, "ALERT_TYPE": typ, "TRIP_KEY": trip, "EPISODE": ep, "ROUTE_ID": route, "TIER": tier,
            "DELAY_MINUTES": float(delay), "ALERT_TS": t, "LOCAL_DATE": t.date(), "IS_PEAK": peak, "SEVERITY": severity,
            "_LOADED_AT": ts(-loaded)}


def alert_frame():
    return pd.DataFrame([
        alert("1", "DELAY_STARTED", "T1", 1, 60, 0, 6.0, peak=True),
        alert("1", "DELAY_STARTED", "T1", 1, 60, 0, 6.0, loaded=3, peak=True),       # redelivered duplicate: must count once
        alert("2", "DELAY_ESCALATED", "T1", 1, 57, 1, 11.0, severity="SEVERE"),
        alert("2", "DELAY_ESCALATED", "T1", 1, 57, 1, 11.0, severity="SEVERE", loaded=4),   # redelivered ESCALATED: would double the escalation count
        alert("3", "DELAY_RECOVERED", "T1", 1, 54, 1, 1.0),
        alert("4", "DELAY_STARTED", "T2", 1, 30, 0, 7.0, route="B"),                  # still open
        alert("5", "DELAY_STARTED", "T3", 1, 40, 0, 8.0, route="B"),
        alert("6", "DELAY_EXPIRED", "T3", 1, 25, 0, 8.0, route="B"),
        alert("7", "DELAY_STARTED", "T1", 2, 20, 0, 5.5),                             # a second episode for the same trip
        alert("8", "DELAY_ESCALATED", "T1", 2, 15, 2, 20.0, severity="CRITICAL"),
    ])


# ------------------------------------------------------------------------------------------------ the engine
@pytest.fixture
def engine():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS GTFSR")
    con.execute("CREATE SCHEMA GTFSR.RAW")
    con.execute("CREATE SCHEMA GTFSR.ANALYTICS")
    rf, af = route_frame(), alert_frame()
    con.register("rf", rf)
    con.register("af", af)
    con.execute("CREATE TABLE GTFSR.RAW.ROUTE_DELAY_5M AS SELECT * FROM rf")
    con.execute("CREATE TABLE GTFSR.RAW.ALERTS AS SELECT * FROM af")
    con.execute("USE GTFSR.ANALYTICS")
    for stmt in sqlglot.parse((ROOT / "snowflake" / "03_views.sql").read_text(), read="snowflake"):
        if isinstance(stmt, sqlglot.exp.Create):  # skip USE / GRANT: roles and grants are Snowflake-only
            con.execute(stmt.sql(dialect="duckdb"))
    return con


def q(con, sql):
    return con.execute(sql).fetchdf()


def twin_route_df():
    d = route_frame().rename(columns=str.lower)
    d["window_start"] = pd.to_datetime(d["window_start"], utc=True)
    d["window_end"] = pd.to_datetime(d["window_end"], utc=True)
    return d


# ------------------------------------------------------------------------------------------------ view by view
def test_all_six_views_were_created(engine):
    names = set(q(engine, "SELECT view_name FROM duckdb_views() WHERE schema_name = 'ANALYTICS'")["view_name"].str.upper())
    assert {"V_ROUTE_DELAY_5M", "V_ROUTE_RELIABILITY_HOURLY", "V_ROUTE_RANKING_24H", "V_ALERT_EPISODES", "V_ALERTS_BY_ROUTE_DAY",
            "V_PIPELINE_FRESHNESS"} <= names


def test_dedup_view_keeps_the_newest_load_of_each_window_like_the_twin(engine):
    got = q(engine, "SELECT ROUTE_ID, WINDOW_START, OBSERVATIONS FROM V_ROUTE_DELAY_5M").sort_values(["ROUTE_ID", "WINDOW_START"])
    want = views.dedupe_route_windows(twin_route_df()).sort_values(["route_id", "window_start"])
    assert len(got) == len(want) == 7                          # 8 rows loaded, the replayed window counts once
    assert list(got["OBSERVATIONS"]) == list(want["observations"])
    a120 = got[(got.ROUTE_ID == "A")].sort_values("WINDOW_START").iloc[0]
    assert a120["OBSERVATIONS"] == 100                         # the replay changed late_obs to 99: that is the one kept...
    assert q(engine, "SELECT LATE_OBSERVATIONS FROM V_ROUTE_DELAY_5M WHERE ROUTE_ID='A' ORDER BY WINDOW_START LIMIT 1").iloc[0, 0] == 99


def test_hourly_view_is_observation_weighted_like_the_twin(engine):
    got = q(engine, "SELECT * FROM V_ROUTE_RELIABILITY_HOURLY").rename(columns=str.lower)
    want = views.hourly_reliability(twin_route_df())
    got["local_date"], want["local_date"] = got["local_date"].astype(str), want["local_date"].astype(str)   # DuckDB: timestamp, twin: date
    m = got.merge(want, on=["route_id", "local_date", "local_hour"], suffixes=("_sf", "_pd"))
    assert len(m) == len(got) == len(want)
    for col in ("observations", "avg_delay_seconds", "max_delay_seconds", "late_ratio"):
        assert m[f"{col}_sf"].astype(float).tolist() == pytest.approx(m[f"{col}_pd"].astype(float).tolist()), col
    # the weighting matters: average of the averages would be 250 here, the weighted figure is not
    a = m[(m.route_id == "A")]
    assert a["avg_delay_seconds_sf"].iloc[0] != pytest.approx(a["avg_delay_seconds_sf"].mean())


def test_ranking_view_matches_the_twin_including_ties_and_the_24h_window(engine):
    got = q(engine, "SELECT * FROM V_ROUTE_RANKING_24H").rename(columns=str.lower).sort_values(["reliability_rank", "route_id"])
    want = views.route_ranking(twin_route_df(), pd.Timestamp(NOW, tz="UTC") + pd.Timedelta(seconds=1), hours=24)
    assert "D" not in set(got.route_id) and set(got.route_id) == set(want.route_id)      # 30 h ago: excluded by both
    m = got.merge(want, on="route_id", suffixes=("_sf", "_pd"))
    assert m["reliability_rank_sf"].tolist() == m["reliability_rank_pd"].tolist()
    for col in ("observations", "avg_delay_seconds", "max_delay_seconds", "late_ratio"):
        assert m[f"{col}_sf"].astype(float).tolist() == pytest.approx(m[f"{col}_pd"].astype(float).tolist()), col
    ranks = dict(zip(m.route_id, m.reliability_rank_sf))
    assert ranks["B"] == ranks["C"]                           # a genuine tie shares a rank in both implementations


def test_episode_view_matches_the_twin_field_by_field(engine):
    got = q(engine, "SELECT * FROM V_ALERT_EPISODES").rename(columns=str.lower).sort_values(["trip_key", "episode"]).reset_index(drop=True)
    twin_in = alert_frame().rename(columns=str.lower)
    want = views.alert_episodes(twin_in).sort_values(["trip_key", "episode"]).reset_index(drop=True)
    assert len(got) == len(want) == 4                         # T1 ep1, T1 ep2, T2, T3: the duplicate STARTED did not make a 5th
    for col in ("trip_key", "episode", "escalations", "max_tier"):
        assert got[col].tolist() == want[col].tolist(), col
    assert got["peak_delay_minutes"].astype(float).tolist() == pytest.approx(want["peak_delay_minutes"].astype(float).tolist())
    # open episodes: both say None/NaN for the end and for how it ended
    assert [None if pd.isna(x) else x for x in got["ended_by"]] == [None if pd.isna(x) else x for x in want["ended_by"]]
    assert pd.isna(got["ended_at"]).tolist() == pd.isna(want["ended_at"]).tolist()
    # DATEDIFF('minute') counts minute BOUNDARIES crossed, and the twin deliberately mirrors that (T1 ep1: 6 min, not 5.x)
    for g, w in zip(got["duration_minutes"], want["duration_minutes"]):
        assert (pd.isna(g) and pd.isna(w)) or int(g) == int(w)


def test_episode_view_flags_peak_if_any_event_of_the_episode_was_in_peak(engine):
    got = q(engine, "SELECT TRIP_KEY, EPISODE, STARTED_IN_PEAK FROM V_ALERT_EPISODES").rename(columns=str.lower)
    flag = {(r.trip_key, r.episode): bool(r.started_in_peak) for r in got.itertuples()}
    assert flag[("T1", 1)] is True and flag[("T2", 1)] is False


def test_alerts_by_route_day_counts_each_alert_once(engine):
    got = q(engine, "SELECT * FROM V_ALERTS_BY_ROUTE_DAY").rename(columns=str.lower)
    a = got[got.route_id == "A"]
    assert int(a["episodes_started"].sum()) == 2              # T1 ep1 + T1 ep2: the redelivered duplicate does not count
    assert int(a["critical_alerts"].sum()) == 1
    assert float(a["worst_delay_minutes"].max()) == 20.0


def test_freshness_view_agrees_with_the_twin_to_within_one_minute(engine):
    got = q(engine, "SELECT * FROM V_PIPELINE_FRESHNESS").iloc[0]
    twin = views.freshness(twin_route_df(), pd.Timestamp.now(tz="UTC"))
    # DATEDIFF counts boundaries, the twin floors elapsed time: they may differ by one minute, never more
    assert abs(int(got["MINUTES_BEHIND"]) - twin["minutes_behind"]) <= 1
    assert pd.Timestamp(got["LATEST_WINDOW_END"]) == pd.Timestamp(twin["latest_window_end"]).tz_localize(None)
