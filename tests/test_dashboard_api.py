import asyncio
import json

from fastapi.testclient import TestClient

from dashboard import api
from dashboard.vehicles import VehicleBoard

NOW = 1_800_000_000


def make_board():
    b = VehicleBoard()
    b.apply_vehicle({"vehicle_id": "V1", "latitude": 53.35, "longitude": -6.26, "event_time": NOW,
                     "trip_id": "T1", "start_date": "20261108", "route_id": "R1", "bearing": 10})
    b.apply_trip_update({"trip_id": "T1", "start_date": "20261108", "current_delay_seconds": 600, "event_time": NOW})
    return b


def client(board=None):
    return TestClient(api.create_app(board or make_board(), start_tail=False))


def test_vehicles_endpoint_returns_snapshot_and_stats():
    r = client().get("/api/vehicles")
    body = r.json()
    assert r.status_code == 200 and body["version"] > 0
    assert body["stats"]["vehicles"] == 1 and body["vehicles"][0]["route_id"] == "R1"


def test_health_reports_ok_without_a_kafka_tail():
    r = client().get("/api/health").json()
    assert r["status"] == "ok" and r["error"] is None and r["seconds_since_last_event"] is not None


def test_health_surfaces_rejected_coordinates():
    b = make_board()
    b.apply_vehicle({"vehicle_id": "BAD", "latitude": 0.00004, "longitude": -0.00002, "event_time": NOW})
    assert client(b).get("/api/health").json()["rejected_bad_coordinates"] == 1


def test_active_alerts_degrades_gracefully_when_dynamodb_is_unreachable(monkeypatch):
    from dashboard import sources

    def boom():
        raise ConnectionError("no emulator")

    monkeypatch.setattr(sources, "active_alerts", boom)
    body = client().get("/api/alerts/active").json()
    assert body["available"] is False and body["reason"] == "ConnectionError" and body["items"] == []
    assert body["summary"]["active"] == 0  # the UI can render the same shape either way


def test_active_alerts_are_shaped_and_joined_to_the_vehicle_board(monkeypatch):
    from dashboard import sources

    # make_board() holds vehicle V1 on trip T1 (service day 20261108) at event time NOW; alert for T1 + one with no vehicle
    monkeypatch.setattr(api.time, "time", lambda: NOW + 60)
    monkeypatch.setattr(sources, "active_alerts", lambda: [
        {"trip_key": "20261108|T1", "tier": 1, "route_id": "R1", "trip_id": "T1", "delay_seconds": 700, "event_time": NOW - 120, "episode": 1},
        {"trip_key": "20261108|T9", "tier": 0, "route_id": "R1", "trip_id": "T9", "delay_seconds": 400, "event_time": NOW - 60, "episode": 1}])
    body = client().get("/api/alerts/active").json()
    assert body["available"] and body["summary"] == {"active": 2, "by_tier": {"0": 1, "1": 1, "2": 0}, "with_vehicle": 1, "unconfirmed": 1, "stalled": 0}
    first = body["items"][0]
    assert first["trip_id"] == "T1" and first["severity"].endswith("SEVERE") and first["changed_min"] == 3
    assert first["vehicle"]["id"] == "V1" and body["items"][1]["vehicle"] is None


ROUTES = {"attribution": "x (CC BY 4.0)", "feed": {"end": "20271107"},
          "routes": {"1 120 c a": {"short": "120", "long": "A - B", "agency": "Dublin Bus", "mode": "bus"}}}


def test_routes_endpoint_serves_names_and_is_cacheable():
    r = TestClient(api.create_app(make_board(), start_tail=False, routes=ROUTES)).get("/api/routes")
    assert r.json()["available"] is True and r.json()["routes"]["1 120 c a"]["short"] == "120"
    assert "max-age" in r.headers["cache-control"]


def test_routes_endpoint_reports_unavailable_when_the_file_is_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("ROUTES_JSON", str(tmp_path / "nope.json"))
    body = TestClient(api.create_app(make_board(), start_tail=False)).get("/api/routes").json()
    assert body["available"] is False and body["routes"] == {}


def test_corrupt_routes_file_is_treated_as_missing(tmp_path):
    (tmp_path / "r.json").write_text("{not json")
    assert api.load_routes_file(tmp_path / "r.json") is None
    (tmp_path / "w.json").write_text('{"routes": []}')  # wrong shape
    assert api.load_routes_file(tmp_path / "w.json") is None


def test_many_alert_polls_cost_one_store_query(monkeypatch):
    """The incident: every open tab polled the store itself, and a slow store snowballed."""
    from dashboard import sources

    calls = []
    monkeypatch.setattr(sources, "active_alerts", lambda: calls.append(1) or [])
    c = client()
    for _ in range(20):
        assert c.get("/api/alerts/active").json()["available"] is True
    assert len(calls) == 1


def test_a_store_failure_after_a_success_serves_the_previous_data_marked_stale(monkeypatch):
    from dashboard import api as api_mod, sources

    state = {"fail": False}

    def flaky():
        if state["fail"]:
            raise TimeoutError("store slow")
        return [{"trip_key": "20261108|T1", "tier": 0, "route_id": "R1", "trip_id": "T1", "delay_seconds": 400, "event_time": NOW, "episode": 1}]

    monkeypatch.setattr(sources, "active_alerts", flaky)
    monkeypatch.setattr(api_mod, "ALERTS_CACHE_SECONDS", 0.0)  # every request refreshes
    c = TestClient(api_mod.create_app(make_board(), start_tail=False))
    first = c.get("/api/alerts/active").json()
    assert first["available"] and first["stale"] is False and first["summary"]["active"] == 1
    state["fail"] = True
    second = c.get("/api/alerts/active").json()
    assert second["available"] and second["stale"] is True and second["summary"]["active"] == 1  # data kept, honestly flagged


def windows_df():
    import pandas as pd

    now = pd.Timestamp.now(tz="UTC").floor("5min") - pd.Timedelta(minutes=30)
    return pd.DataFrame([{"window_start": now, "window_end": now + pd.Timedelta(minutes=5), "route_id": rid, "observations": 500, "trips_observed": 8,
                          "late_observations": late, "late_ratio": late / 500, "avg_delay_seconds": 200.0, "max_delay_seconds": 900,
                          "local_hour": now.hour, "curated_at": now} for rid, late in (("A", 50), ("B", 300))])


def analytics_client(loader):
    return TestClient(api.create_app(make_board(), start_tail=False, windows_loader=loader))


def test_reliability_endpoint_ranks_routes_and_reports_coverage():
    body = analytics_client(windows_df).get("/api/analytics/reliability?min_obs=100").json()
    assert body["available"] and body["has_data"]
    assert [r["route_id"] for r in body["ranking"]] == ["B", "A"] and body["ranking"][0]["late_ratio"] == 0.6
    assert body["coverage"]["routes_ranked"] == 2 and len(body["heatmap"]["hours"]) == 24


def test_reliability_endpoint_degrades_to_unavailable_when_the_lake_is_down():
    def boom():
        raise ConnectionError("lake down")

    body = analytics_client(boom).get("/api/analytics/reliability").json()
    assert body["available"] is False and body["reason"] == "ConnectionError" and body["ranking"] == []


def test_reliability_endpoint_rejects_nonsense_parameters():
    c = analytics_client(windows_df)
    assert c.get("/api/analytics/reliability?hours=0").status_code == 422
    assert c.get("/api/analytics/reliability?limit=9999").status_code == 422
    assert c.get("/api/analytics/reliability?min_obs=abc").status_code == 422


def test_reliability_reads_are_cached_between_requests():
    calls = []

    def loader():
        calls.append(1)
        return windows_df()

    c = analytics_client(loader)
    c.get("/api/analytics/reliability"); c.get("/api/analytics/reliability?limit=5")
    assert len(calls) == 1


class FakeRequest:
    """Reports 'connected' for `n` checks, then disconnects: lets us drive the endless SSE loop deterministically."""

    def __init__(self, n):
        self.n = n

    async def is_disconnected(self):
        self.n -= 1
        return self.n < 0


def collect_sse(board, checks, monkeypatch):
    monkeypatch.setattr(api, "PUSH_CHECK_SECONDS", 0)
    app = api.create_app(board, start_tail=False)
    route = next(r for r in app.routes if getattr(r, "path", "") == "/api/stream")

    async def run():
        resp = await route.endpoint(FakeRequest(checks))
        return [chunk async for chunk in resp.body_iterator]

    return asyncio.run(run())


def test_sse_sends_a_snapshot_only_when_the_board_changed(monkeypatch):
    chunks = collect_sse(make_board(), checks=5, monkeypatch=monkeypatch)
    snaps = [c for c in chunks if c.startswith("event: snapshot")]
    assert len(snaps) == 1  # five loop iterations, one change: no repeated identical pushes
    data = json.loads(snaps[0].split("data: ", 1)[1])
    assert data["stats"]["vehicles"] == 1 and data["vehicles"][0]["delay_s"] == 600


def test_sse_stream_ends_when_the_client_disconnects(monkeypatch):
    assert collect_sse(VehicleBoard(), checks=0, monkeypatch=monkeypatch) == []  # returned, did not hang
