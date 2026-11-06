from google.transit import gtfs_realtime_pb2 as pb

from gtfsrt.decode import current_delay_seconds, parse_feed, trip_key, trip_update_events
from gtfsrt.producer import build_events
from gtfsrt.simulator import Simulator

NOW = 1_760_000_000  # fixed epoch for determinism


def _feed(ts=NOW) -> pb.FeedMessage:
    f = pb.FeedMessage()
    f.header.gtfs_realtime_version = "2.0"
    f.header.timestamp = ts
    return f


def _trip(feed, trip_id="T1", route="46A", start_date="20260101"):
    e = feed.entity.add()
    e.id = f"e-{trip_id}"
    tu = e.trip_update
    tu.trip.trip_id = trip_id
    tu.trip.route_id = route
    tu.trip.start_date = start_date
    return tu


def test_delay_uses_first_stop_with_delay_arrival_preferred():
    tu = _trip(_feed())
    s1 = tu.stop_time_update.add(); s1.stop_sequence = 1  # no delay info at all
    s2 = tu.stop_time_update.add(); s2.stop_sequence = 2
    s2.arrival.delay = 360
    s2.departure.delay = 400
    assert current_delay_seconds(tu) == 360  # skips s1, prefers arrival over departure


def test_delay_falls_back_to_departure():
    tu = _trip(_feed())
    s = tu.stop_time_update.add(); s.departure.delay = 90
    assert current_delay_seconds(tu) == 90


def test_trip_level_delay_overrides_stop_level():
    tu = _trip(_feed())
    tu.delay = 600
    s = tu.stop_time_update.add(); s.arrival.delay = 30
    assert current_delay_seconds(tu) == 600


def test_zero_delay_is_zero_not_none():
    tu = _trip(_feed())
    s = tu.stop_time_update.add(); s.arrival.delay = 0
    assert current_delay_seconds(tu) == 0  # on time is a real value


def test_canceled_trip_has_no_delay():
    tu = _trip(_feed())
    tu.trip.schedule_relationship = pb.TripDescriptor.CANCELED
    s = tu.stop_time_update.add(); s.arrival.delay = 999
    assert current_delay_seconds(tu) is None


def test_no_delay_info_is_none():
    assert current_delay_seconds(_trip(_feed())) is None


def test_events_have_contract_fields_and_drop_tripless_entities():
    feed = _feed()
    tu = _trip(feed, "T1")
    s = tu.stop_time_update.add(); s.stop_sequence = 7; s.stop_id = "S7"; s.arrival.delay = 310
    bad = feed.entity.add(); bad.id = "no-trip"; bad.trip_update.timestamp = NOW  # trip_id missing
    events = list(trip_update_events(feed, ingest_time_ms=NOW * 1000))
    assert len(events) == 1
    ev = events[0]
    assert ev["trip_id"] == "T1" and ev["route_id"] == "46A"
    assert ev["current_delay_seconds"] == 310
    assert ev["next_stop_id"] == "S7" and ev["next_stop_sequence"] == 7
    assert ev["event_time"] == NOW  # falls back to header timestamp
    assert ev["stop_time_updates"][0]["arrival_delay"] == 310


def test_start_date_falls_back_to_event_time_date():
    feed = _feed()
    tu = _trip(feed, start_date="")
    s = tu.stop_time_update.add(); s.arrival.delay = 1
    ev = next(iter(trip_update_events(feed)))
    assert len(ev["start_date"]) == 8 and ev["start_date"].isdigit()


def test_trip_key_includes_date():
    assert trip_key("20260101", "T1") == "20260101|T1"
    assert trip_key("20260101", "T1") != trip_key("20260102", "T1")


def test_build_events_skips_unchanged_feed_timestamp():
    payload = Simulator(seed=1, n_trips=3).trip_updates_feed(NOW)
    first = build_events(payload, None, last_header_ts=None, ingest_time_ms=NOW * 1000)
    assert len(first.trip_events) == 3 and not first.skipped_duplicate
    again = build_events(payload, None, last_header_ts=first.header_ts, ingest_time_ms=NOW * 1000)
    assert again.skipped_duplicate and again.trip_events == []


def test_build_events_survives_garbage_bytes():
    res = build_events(b"\x00\xff not protobuf \x01", None, last_header_ts=5, ingest_time_ms=1)
    assert res.decode_error and res.header_ts == 5  # last good ts preserved


def test_simulator_is_deterministic_and_produces_late_trips():
    a = Simulator(seed=7, n_trips=30)
    b = Simulator(seed=7, n_trips=30)
    worst = 0
    for i in range(30):
        pa = a.trip_updates_feed(NOW + 30 * i)
        pb_ = b.trip_updates_feed(NOW + 30 * i)
        assert pa == pb_
        for ev in trip_update_events(parse_feed(pa)):
            worst = max(worst, ev["current_delay_seconds"] or 0)
    assert worst >= 300, "simulator should produce at least one trip beyond the 5-minute alert threshold"


# ---- live-mode contract found by running against the real NTA API (see DECISIONS.md) -------------------------------
def test_config_reads_the_existing_env_variable_names(monkeypatch):
    from gtfsrt.config import FeedConfig

    monkeypatch.setenv("FEED_MODE", "live")
    monkeypatch.setenv("NTA_API_KEY", "k")
    monkeypatch.setenv("NTA_API_KEY_HEADER", "Ocp-Apim-Subscription-Key")
    monkeypatch.setenv("NTA_VEHICLE_POSITIONS_URL", "https://example/vehicles")
    monkeypatch.setenv("NTA_TRIP_UPDATES_URL", "https://example/trips")
    cfg = FeedConfig.from_env()
    assert (cfg.api_key_header, cfg.vehicles_url, cfg.trip_updates_url) == (
        "Ocp-Apim-Subscription-Key", "https://example/vehicles", "https://example/trips")


def test_live_call_interval_can_never_drop_below_fair_usage_minimum():
    from gtfsrt.config import FeedConfig

    assert FeedConfig(mode="live", poll_interval_seconds=5).call_interval_seconds == 60
    assert FeedConfig(mode="live", poll_interval_seconds=90).call_interval_seconds == 90
    assert FeedConfig(mode="simulator", poll_interval_seconds=2).call_interval_seconds == 2  # sim is not rate-limited


def test_live_source_makes_exactly_one_call_per_cycle_alternating_feeds_and_sends_configured_header():
    from unittest import mock

    from gtfsrt.config import FeedConfig
    from gtfsrt.producer import LiveSource

    cfg = FeedConfig(mode="live", api_key="secret", api_key_header="X-Custom-Key",
                     trip_updates_url="https://t", vehicles_url="https://v")
    src = LiveSource(cfg)
    assert src.session.headers["X-Custom-Key"] == "secret" and "x-api-key" not in {k.lower() for k in src.session.headers if k != "X-Custom-Key"}
    ok = mock.Mock(status_code=200, content=b"payload")
    with mock.patch.object(src.session, "get", return_value=ok) as get:
        results = [src.fetch_cycle(i) for i in range(4)]
    assert get.call_count == 4  # one call per cycle, never two
    assert [c.args[0] for c in get.call_args_list] == ["https://t", "https://v", "https://t", "https://v"]
    assert results[0] == (b"payload", None) and results[1] == (None, b"payload")


def test_throttled_response_yields_nothing_and_backs_off():
    from unittest import mock

    from gtfsrt.config import FeedConfig
    from gtfsrt.producer import LiveSource

    src = LiveSource(FeedConfig(mode="live", api_key="k"))
    with mock.patch.object(src.session, "get", return_value=mock.Mock(status_code=429, content=b"")), \
            mock.patch("gtfsrt.producer.time.sleep"):
        assert src.fetch_cycle(0) == (None, None)
    assert src._backoff > 0


def test_vehicle_only_cycle_is_decoded_without_touching_trip_state():
    veh = Simulator(seed=3, n_trips=4).vehicles_feed(NOW)
    res = build_events(None, veh, last_header_ts=123, ingest_time_ms=NOW * 1000)
    assert len(res.vehicle_events) == 4 and res.trip_events == [] and res.header_ts is None and not res.skipped_duplicate
