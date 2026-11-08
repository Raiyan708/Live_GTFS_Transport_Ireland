from decimal import Decimal

from dashboard.alerts import build_active_alerts
from dashboard.vehicles import VehicleBoard

NOW = 1_800_000_000


def item(trip="T1", date="20261108", tier=0, delay=420, et=NOW - 600, route="1 120 c a", episode=1):
    # boto3 returns DynamoDB numbers as Decimal: the shaper must cope
    return {"trip_key": f"{date}|{trip}", "trip_id": trip, "route_id": route, "tier": Decimal(tier),
            "delay_seconds": Decimal(delay), "event_time": Decimal(et), "episode": Decimal(episode)}


def test_alerts_are_sorted_worst_first_with_stable_ties():
    r = build_active_alerts([item("B", delay=400), item("A", delay=900, tier=2), item("C", delay=400)], {}, NOW)
    assert [a["trip_id"] for a in r["items"]] == ["A", "B", "C"]
    assert r["items"][0]["severity"].endswith("CRITICAL")


def test_changed_min_means_time_since_the_alert_changed_not_since_the_trip_reported():
    a = build_active_alerts([item(et=NOW - 11 * 60 - 20)], {}, NOW)["items"][0]
    assert a["changed_min"] == 11 and a["changed_at"] == NOW - 680
    assert "since_update_min" not in a  # the misleading name must not come back


def test_alert_is_joined_to_its_vehicle_by_trip_key_and_service_day():
    b = VehicleBoard()
    b.apply_vehicle({"vehicle_id": "V9", "latitude": 53.35, "longitude": -6.26, "event_time": NOW - 30,
                     "trip_id": "T1", "start_date": "20261108", "route_id": "R"})
    b.apply_vehicle({"vehicle_id": "V8", "latitude": 53.40, "longitude": -6.30, "event_time": NOW - 30,
                     "trip_id": "T2", "start_date": "20261107", "route_id": "R"})  # same trip id, other day
    pos = b.positions_by_trip(NOW)
    r = build_active_alerts([item("T1"), item("T2"), item("T3")], pos, NOW)
    by = {a["trip_id"]: a for a in r["items"]}
    assert by["T1"]["vehicle"] == {"id": "V9", "lat": 53.35, "lon": -6.26, "age_s": 30}
    assert by["T2"]["vehicle"] is None and by["T3"]["vehicle"] is None  # wrong service day / no vehicle: not faked
    assert r["summary"] == {"active": 3, "by_tier": {0: 3, 1: 0, 2: 0}, "with_vehicle": 1, "unconfirmed": 0, "stalled": 0}


def test_current_delay_overrides_the_stale_delay_stored_when_the_alert_last_changed():
    """REAL inconsistency seen on the live feed: a critical alert said 104 min while its bus was 196 min late. A top-tier
    alert gets no further events, so the delay stored on the row is the delay at the moment it last CHANGED."""
    a = build_active_alerts([item("T1", delay=6240, tier=2)], {}, NOW, current_delays={"20261108|T1": 11760})["items"][0]
    assert a["delay_s"] == 11760 and a["delay_at_alert_s"] == 6240 and a["delay_is_current"] is True


def test_falls_back_to_the_alert_delay_when_the_trip_has_no_current_update():
    a = build_active_alerts([item("T1", delay=700)], {}, NOW, current_delays={})["items"][0]
    assert a["delay_s"] == 700 and a["delay_is_current"] is False


def test_sort_uses_the_current_delay():
    r = build_active_alerts([item("A", delay=900), item("B", delay=400)], {}, NOW, current_delays={"20261108|B": 5000})
    assert [x["trip_id"] for x in r["items"]] == ["B", "A"]


def test_board_exposes_current_delays_within_the_ttl_only():
    b = VehicleBoard()
    b.apply_trip_update({"trip_id": "T1", "start_date": "20261108", "current_delay_seconds": 600, "event_time": NOW})
    b.apply_trip_update({"trip_id": "T2", "start_date": "20261108", "current_delay_seconds": None, "event_time": NOW})  # canceled
    assert b.current_delays(NOW + 60) == {"20261108|T1": 600}
    assert b.current_delays(NOW + 15 * 60 + 1) == {}


def test_malformed_rows_are_skipped_not_fatal_and_unknown_route_becomes_none():
    good = item("T1", route="unknown")
    r = build_active_alerts([{"trip_key": "x"}, {"tier": "oops", "trip_key": "y", "delay_seconds": 1, "event_time": 1}, good], {}, NOW)
    assert len(r["items"]) == 1 and r["items"][0]["route_id"] is None


def test_empty_input():
    assert build_active_alerts([], {}, NOW) == {"summary": {"active": 0, "by_tier": {0: 0, 1: 0, 2: 0}, "with_vehicle": 0, "unconfirmed": 0, "stalled": 0}, "items": []}


def test_summary_counts_alerts_the_live_feed_no_longer_confirms():
    """A stopped alert pipeline leaves rows ACTIVE forever. Reconcile against the live feed so the UI can say so:
    unconfirmed = the trip is no longer in the feed, or its current delay is back under the 'off' threshold."""
    items = [item("A", delay=900), item("B", delay=900), item("C", delay=900), item("D", delay=900)]
    cur = {"20261108|A": 1000,   # still late: confirmed
           "20261108|B": 60,     # recovered in the feed (alert row not yet closed): unconfirmed
           "20261108|C": 130}    # inside the hysteresis band (>= off threshold): still legitimately active -> confirmed
    #  D has no current update at all: the trip left the feed -> unconfirmed
    s = build_active_alerts(items, {}, NOW, current_delays=cur)["summary"]
    assert s["active"] == 4 and s["unconfirmed"] == 2


def test_with_no_live_feed_information_nothing_is_called_unconfirmed():
    # if the delay table is empty (feed not loaded yet) we cannot judge: claiming 100% disagreement would be a false alarm
    assert build_active_alerts([item("A")], {}, NOW, current_delays=None)["summary"]["unconfirmed"] == 0
    assert build_active_alerts([item("A")], {}, NOW, current_delays={})["summary"]["unconfirmed"] == 0


def test_alerts_carry_stalled_and_upcoming_flags_and_the_summary_counts_stalled():
    items = [item("A", delay=3000), item("B", delay=900), item("C", delay=900)]
    r = build_active_alerts(items, {}, NOW, current_delays={"20261108|A": 3000, "20261108|B": 900, "20261108|C": 900},
                            stalled={"20261108|A"}, upcoming={"20261108|C"})
    by = {a["trip_id"]: a for a in r["items"]}
    assert by["A"]["stalled"] and not by["A"]["upcoming"] and by["C"]["upcoming"] and not by["B"]["stalled"]
    assert r["summary"]["stalled"] == 1
