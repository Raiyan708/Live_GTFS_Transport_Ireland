import numpy as np
import pandas as pd
import pytest

from dashboard import views

T = pd.Timestamp


def windows(rows):
    cols = ["window_start", "window_end", "route_id", "observations", "avg_delay_seconds",
            "max_delay_seconds", "late_observations", "local_hour", "local_date", "curated_at"]
    out = []
    for r in rows:
        start = T(r[0]); out.append((start, start + pd.Timedelta(minutes=5), r[1], r[2], r[3], r[4], r[5],
                                     start.hour, start.date(), T("2026-11-06 20:00")))
    return pd.DataFrame(out, columns=cols)


NOW = T("2026-11-06 17:00")


def test_ranking_is_observation_weighted_not_average_of_averages():
    # window A: 1 obs avg 1000s (late) ; window B: 99 obs avg 0s.  True mean = 10s, naive mean-of-means = 500s
    df = windows([("2026-11-06 16:00", "X", 1, 1000, 1000, 1), ("2026-11-06 16:05", "X", 99, 0, 0, 0)])
    r = views.route_ranking(df, NOW).iloc[0]
    assert r["avg_delay_seconds"] == pytest.approx(10.0)
    assert r["late_ratio"] == pytest.approx(0.01)


def test_ranking_orders_by_late_ratio_and_ranks_ties_equally():
    df = windows([("2026-11-06 16:00", "A", 10, 5, 10, 5), ("2026-11-06 16:00", "B", 10, 5, 10, 5),
                  ("2026-11-06 16:00", "C", 10, 5, 10, 1)])
    r = views.route_ranking(df, NOW)
    assert list(r["route_id"]) == ["A", "B", "C"]
    assert list(r["reliability_rank"]) == [1, 1, 3]  # SQL RANK(): ties share a rank, next rank skips


def test_ranking_respects_lookback_window():
    df = windows([("2026-11-05 10:00", "OLD", 10, 5, 10, 5), ("2026-11-06 16:00", "NEW", 10, 5, 10, 1)])
    assert list(views.route_ranking(df, NOW, hours=24)["route_id"]) == ["NEW"]
    assert len(views.route_ranking(df, NOW, hours=48)) == 2


def test_duplicate_windows_are_collapsed_before_aggregating():
    df = windows([("2026-11-06 16:00", "A", 10, 100, 200, 5)] * 2)
    assert views.route_ranking(df, NOW).iloc[0]["observations"] == 10  # not 20


def test_empty_inputs_return_typed_empty_frames():
    empty = windows([]).iloc[0:0]
    assert views.route_ranking(empty, NOW).empty and views.heatmap_frame(empty).empty
    assert views.alert_episodes(pd.DataFrame()).empty
    assert views.freshness(empty, NOW)["minutes_behind"] is None


def test_heatmap_pools_days_per_hour():
    df = windows([("2026-11-05 08:00", "A", 10, 0, 0, 2), ("2026-11-06 08:00", "A", 10, 0, 0, 6)])
    h = views.heatmap_frame(df)
    assert len(h) == 1 and h.iloc[0]["late_ratio"] == pytest.approx(0.4)


def test_freshness_minutes_behind():
    df = windows([("2026-11-06 16:00", "A", 1, 0, 0, 0)])  # window_end 16:05, now 17:00
    assert views.freshness(df, NOW)["minutes_behind"] == 55


def alert(aid, atype, ts, tier=0, delay=6.0, trip="k1", ep=1, route="46A"):
    return {"alert_id": aid, "alert_type": atype, "alert_ts": T(ts), "tier": tier, "delay_minutes": delay,
            "trip_key": trip, "episode": ep, "route_id": route}


def test_episode_collapses_lifecycle_and_matches_sql_datediff_semantics():
    a = pd.DataFrame([
        alert("1", "DELAY_STARTED", "2026-11-06 16:00:50", 0, 6.0),
        alert("2", "DELAY_ESCALATED", "2026-11-06 16:10:00", 1, 11.0),
        alert("3", "DELAY_RECOVERED", "2026-11-06 16:20:10", 1, 1.0),
    ])
    e = views.alert_episodes(a).iloc[0]
    assert e["ended_by"] == "RECOVERED" and e["escalations"] == 1 and e["max_tier"] == 1
    assert e["peak_delay_minutes"] == 11.0  # the RECOVERED row's 1.0 must not count as a peak
    # 16:00:50 -> 16:20:10 crosses 20 minute boundaries (16:00 -> 16:20) in DATEDIFF('minute') terms
    assert e["duration_minutes"] == 20


def test_open_episode_has_no_end_and_duplicate_alert_rows_are_ignored():
    a = pd.DataFrame([alert("1", "DELAY_STARTED", "2026-11-06 16:00:00")] * 2)
    e = views.alert_episodes(a)
    assert len(e) == 1 and e.iloc[0]["ended_by"] is None and np.isnan(e.iloc[0]["duration_minutes"])


def test_episodes_are_separated_per_trip_and_episode_number():
    a = pd.DataFrame([alert("1", "DELAY_STARTED", "2026-11-06 16:00:00", ep=1),
                      alert("2", "DELAY_STARTED", "2026-11-06 16:30:00", ep=2),
                      alert("3", "DELAY_STARTED", "2026-11-06 16:10:00", trip="k2", ep=1)])
    assert len(views.alert_episodes(a)) == 3


def test_severity_uses_shape_and_word_not_colour():
    assert views.severity_label(0) == "◆ MODERATE"
    assert views.severity_label(1) == "▲ SEVERE"
    assert views.severity_label(2) == "● CRITICAL" and views.severity_label(7) == "● CRITICAL"
    assert len({views.severity_label(i).split()[0] for i in (0, 1, 2)}) == 3  # three distinct shapes


def test_active_alert_table_sorted_worst_first_with_age():
    items = [{"tier": 0, "route_id": "A", "trip_id": "t1", "delay_seconds": 360, "event_time": 1000},
             {"tier": 2, "route_id": "B", "trip_id": "t2", "delay_seconds": 960, "event_time": 1000 - 120}]
    t = views.active_alerts_table(items, now_epoch=1060)
    assert list(t["route"]) == ["B", "A"] and list(t["delay_min"]) == [16.0, 6.0]
    assert list(t["since_change_min"]) == [3, 1]
    assert views.active_alerts_table([], 0).empty
