import pandas as pd

from dashboard.analytics import reliability

NOW = pd.Timestamp("2026-11-08 20:00:00", tz="UTC")


def rows(route, start, obs, late, avg=120.0, mx=900, hour=None, trips=10):
    s = pd.Timestamp(start, tz="UTC")
    return {"window_start": s, "window_end": s + pd.Timedelta(minutes=5), "route_id": route, "observations": obs,
            "trips_observed": trips, "late_observations": late, "avg_delay_seconds": avg, "max_delay_seconds": mx,
            "late_ratio": late / obs, "local_hour": s.hour if hour is None else hour, "curated_at": s}


def frame(*r):
    return pd.DataFrame(list(r))


def test_no_data_is_reported_as_no_data_not_as_an_empty_success():
    assert reliability(pd.DataFrame(), NOW)["has_data"] is False
    old = frame(rows("A", "2026-10-01 10:00", 200, 50))
    assert reliability(old, NOW, hours=24)["has_data"] is False  # exists, but outside the window


def test_ranking_is_by_late_share_among_routes_with_enough_evidence():
    f = frame(rows("GOOD", "2026-11-08 17:00", 500, 5), rows("BAD", "2026-11-08 17:00", 500, 250),
              rows("MID", "2026-11-08 17:00", 500, 50), rows("TINY", "2026-11-08 17:00", 3, 3))  # 100% late on 3 readings
    r = reliability(f, NOW, min_obs=100)
    assert [x["route_id"] for x in r["ranking"]] == ["BAD", "MID", "GOOD"]
    assert [x["rank"] for x in r["ranking"]] == [1, 2, 3]
    assert r["coverage"]["routes_seen"] == 4 and r["coverage"]["routes_ranked"] == 3  # TINY is counted as seen, not ranked


def test_observation_weighted_not_average_of_averages():
    # a busy half-hour at 10% late + a nearly empty one at 100% late must come out near 10%, not 55%
    f = frame(rows("A", "2026-11-08 17:00", 1000, 100), rows("A", "2026-11-08 17:05", 10, 10))
    x = reliability(f, NOW, min_obs=1)["ranking"][0]
    assert abs(x["late_ratio"] - 110 / 1010) < 1e-4 and x["observations"] == 1010


def test_ties_are_broken_by_more_evidence_then_name():
    f = frame(rows("B", "2026-11-08 17:00", 200, 40), rows("A", "2026-11-08 17:00", 400, 80), rows("C", "2026-11-08 17:00", 400, 80))
    assert [x["route_id"] for x in reliability(f, NOW, min_obs=1)["ranking"]] == ["A", "C", "B"]


def test_thin_heatmap_cells_are_masked_but_still_reported():
    f = frame(rows("A", "2026-11-08 17:00", 500, 250), rows("A", "2026-11-08 18:00", 5, 5))  # hour 18 has only 5 readings
    cells = {c["hour"]: c for c in reliability(f, NOW, min_obs=1, min_cell_obs=20)["heatmap"]["routes"][0]["cells"]}
    assert cells[17]["late_ratio"] == 0.5 and cells[17]["observations"] == 500
    assert cells[18]["late_ratio"] is None and cells[18]["observations"] == 5


def test_heatmap_covers_all_24_hours_and_only_lists_hours_with_data():
    r = reliability(frame(rows("A", "2026-11-08 17:00", 500, 250)), NOW, min_obs=1)
    assert r["heatmap"]["hours"] == list(range(24)) and [c["hour"] for c in r["heatmap"]["routes"][0]["cells"]] == [17]
    assert r["coverage"]["hours_with_data"] == [17]  # the UI must not pretend the other 23 hours are known


def test_duplicate_windows_from_replays_are_not_double_counted():
    one = rows("A", "2026-11-08 17:00", 500, 250)
    dup = {**one, "curated_at": one["curated_at"] + pd.Timedelta(minutes=1)}
    r = reliability(frame(one, dup), NOW, min_obs=1)
    assert r["ranking"][0]["observations"] == 500
    assert r["coverage"]["observations"] == 500 and r["coverage"]["windows"] == 1  # the headline totals must not double-count either


def test_limit_and_naive_timestamps():
    f = pd.DataFrame([{**rows(f"R{i}", "2026-11-08 17:00", 200, i), "window_start": pd.Timestamp("2026-11-08 17:00"),
                       "window_end": pd.Timestamp("2026-11-08 17:05")} for i in range(30)])  # tz-naive, as Parquet can return
    r = reliability(f, NOW.tz_localize(None), min_obs=1, limit=5)
    assert len(r["ranking"]) == 5 and r["ranking"][0]["route_id"] == "R29" and r["coverage"]["routes_ranked"] == 30


def test_a_route_with_too_few_trips_is_not_ranked_even_with_plenty_of_observations():
    """REAL case: route 261 has 2 trips, one of them a 'ghost' (scheduled trip that never ran; its delay grows 1:1 with the
    clock). That single trip made the route '100% late' and rank 1. Late share needs several trips behind it."""
    f = frame(rows("GHOSTY", "2026-11-08 17:00", 500, 500, trips=2), rows("REAL", "2026-11-08 17:00", 500, 100, trips=12))
    r = reliability(f, NOW, min_obs=1, min_trips=3)
    assert [x["route_id"] for x in r["ranking"]] == ["REAL"]
    assert r["coverage"]["routes_seen"] == 2 and r["coverage"]["routes_ranked"] == 1


def test_trip_support_uses_the_busiest_window_not_a_sum_over_windows():
    # 3 windows with 2 concurrent trips each is still a 2-trip route (summing would claim 6)
    f = frame(*[rows("SMALL", f"2026-11-08 17:{m:02d}", 100, 100, trips=2) for m in (0, 5, 10)])
    assert reliability(f, NOW, min_obs=1, min_trips=3)["ranking"] == []
    f2 = frame(rows("OK", "2026-11-08 17:00", 100, 50, trips=2), rows("OK", "2026-11-08 17:05", 100, 50, trips=3))
    assert reliability(f2, NOW, min_obs=1, min_trips=3)["ranking"][0]["trips_peak"] == 3
