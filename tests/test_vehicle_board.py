import threading

from dashboard.vehicles import DELAY_TTL_SECONDS, VEHICLE_MAX_AGE_SECONDS, VehicleBoard, summarize

NOW = 1_800_000_000


def veh(vid="V1", lat=53.35, lon=-6.26, et=NOW, trip="T1", date="20261108", route="R1", bearing=90):
    return {"vehicle_id": vid, "latitude": lat, "longitude": lon, "event_time": et, "trip_id": trip,
            "start_date": date, "route_id": route, "bearing": bearing, "speed_mps": 5.0}


def tu(trip="T1", date="20261108", delay=600, et=NOW):
    return {"trip_id": trip, "start_date": date, "current_delay_seconds": delay, "event_time": et}


def test_vehicle_appears_in_snapshot_with_rounded_coordinates():
    b = VehicleBoard()
    assert b.apply_vehicle(veh(lat=53.3498123456, lon=-6.2602987654))
    (v,) = b.snapshot(NOW + 30)
    assert (v["lat"], v["lon"], v["age_s"], v["route_id"]) == (53.34981, -6.2603, 30, "R1")


def test_unusable_vehicle_records_are_rejected():
    b = VehicleBoard()
    bad = [veh(vid=""), veh(lat=None), veh(et=0)]
    assert not any(b.apply_vehicle(x) for x in bad)
    assert b.snapshot(NOW) == [] and b.version == 0 and b.rejected_out_of_bounds == 0  # not a coordinate problem


def test_coordinates_outside_ireland_are_rejected_and_counted():
    b = VehicleBoard()
    junk = [veh(lat=0, lon=0),                   # protobuf "unset" default
            veh(lat=0.00004, lon=-0.00002),      # REAL values seen on the live feed: near zero, not exactly zero
            veh(lat=51.5, lon=-0.12),            # London
            veh(lat=53.35, lon=6.26)]            # sign flipped: Dublin's longitude without the minus
    assert not any(b.apply_vehicle(x) for x in junk)
    assert b.snapshot(NOW) == [] and b.rejected_out_of_bounds == 4


def test_the_whole_island_is_accepted_including_the_corners():
    b = VehicleBoard()
    places = {"Dublin": (53.35, -6.26), "Cork": (51.90, -8.47), "Galway": (53.27, -9.05), "Belfast": (54.60, -5.93),
              "Malin Head": (55.38, -7.37), "Valentia": (51.94, -10.30)}
    assert all(b.apply_vehicle(veh(vid=n, lat=la, lon=lo)) for n, (la, lo) in places.items())
    assert b.rejected_out_of_bounds == 0


def test_duplicates_and_out_of_order_events_never_move_a_bus_backwards():
    b = VehicleBoard()
    b.apply_vehicle(veh(lat=53.40, et=NOW))
    v = b.version
    assert not b.apply_vehicle(veh(lat=53.10, et=NOW - 60))   # older
    assert not b.apply_vehicle(veh(lat=53.10, et=NOW))        # replayed duplicate
    assert b.version == v and b.snapshot(NOW)[0]["lat"] == 53.40
    assert b.apply_vehicle(veh(lat=53.41, et=NOW + 60)) and b.snapshot(NOW + 60)[0]["lat"] == 53.41


def test_delay_is_joined_through_the_trip_key():
    b = VehicleBoard()
    b.apply_vehicle(veh(trip="T1", date="20261108"))
    b.apply_vehicle(veh(vid="V2", trip="T1", date="20261107"))  # same trip_id, a different service day
    b.apply_trip_update(tu(trip="T1", date="20261108", delay=420))
    by_id = {v["id"]: v for v in b.snapshot(NOW)}
    assert by_id["V1"]["delay_s"] == 420 and by_id["V2"]["delay_s"] is None


def test_a_stale_delay_is_unknown_not_current():
    b = VehicleBoard()
    b.apply_vehicle(veh(et=NOW + DELAY_TTL_SECONDS + 60))
    b.apply_trip_update(tu(et=NOW))
    assert b.snapshot(NOW + DELAY_TTL_SECONDS + 60)[0]["delay_s"] is None
    assert b.snapshot(NOW + DELAY_TTL_SECONDS)[0]["delay_s"] == 600  # exactly at the limit still counts


def test_canceled_trip_and_idle_vehicle_have_no_delay():
    b = VehicleBoard()
    b.apply_vehicle(veh(trip="T1"))
    b.apply_vehicle(veh(vid="V2", trip="", date=""))  # vehicle not on a trip
    b.apply_trip_update(tu(trip="T1", delay=None))
    assert [v["delay_s"] for v in b.snapshot(NOW)] == [None, None]


def test_older_trip_update_does_not_overwrite_a_newer_one():
    b = VehicleBoard()
    b.apply_vehicle(veh())
    b.apply_trip_update(tu(delay=900, et=NOW + 100))
    assert not b.apply_trip_update(tu(delay=60, et=NOW))
    assert b.snapshot(NOW + 100)[0]["delay_s"] == 900


def test_prune_drops_vehicles_that_stopped_reporting():
    b = VehicleBoard()
    b.apply_vehicle(veh(vid="OLD", et=NOW))
    b.apply_vehicle(veh(vid="NEW", et=NOW + VEHICLE_MAX_AGE_SECONDS))
    b.prune(NOW + VEHICLE_MAX_AGE_SECONDS + 1)
    assert [v["id"] for v in b.snapshot(NOW + VEHICLE_MAX_AGE_SECONDS + 1)] == ["NEW"]


def test_summary_only_counts_known_delays():
    vs = [{"delay_s": d} for d in (None, None, 0, 299, 300, 900)]
    s = summarize(vs)
    assert s == {"vehicles": 6, "with_delay": 4, "late": 2, "median_delay_s": 300, "stalled": 0}
    assert summarize([{"delay_s": None}])["median_delay_s"] is None and summarize([])["vehicles"] == 0


def test_concurrent_writers_and_readers_do_not_corrupt_the_board():
    b = VehicleBoard()
    errors = []

    def writer(offset):
        try:
            for i in range(500):
                b.apply_vehicle(veh(vid=f"V{offset}-{i}", et=NOW + i))
                b.apply_trip_update(tu(trip=f"T{i}", et=NOW + i))
        except Exception as e:  # pragma: no cover
            errors.append(e)

    def reader():
        try:
            for _ in range(200):
                b.snapshot(NOW + 1000)
        except Exception as e:  # pragma: no cover
            errors.append(e)

    ts = [threading.Thread(target=writer, args=(k,)) for k in range(3)] + [threading.Thread(target=reader)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not errors and len(b.snapshot(NOW + 1000)) == 1500


# ----------------------------------------------------------------------------------------------- stalled ("ghost") trips
def feed(b, delays, start=NOW, step=120, trip="T1", date="20261108", start_time="08:00:00"):
    for i, d in enumerate(delays):
        b.apply_trip_update({"trip_id": trip, "start_date": date, "current_delay_seconds": d, "event_time": start + i * step,
                             "start_time": start_time})
    return start + (len(delays) - 1) * step


KEY = "20261108|T1"


def test_a_delay_growing_one_for_one_with_the_clock_is_flagged_as_not_progressing():
    """REAL pattern on the live feed (10 of 18 trips with a 90+ min delay): the delay rises exactly as fast as the clock,
    i.e. the trip is not moving through its timetable (never started, or a stuck vehicle)."""
    b = VehicleBoard()
    t = feed(b, [900, 1020, 1140, 1260, 1380, 1500])            # +120 s every 120 s
    assert KEY in b.stalled_trips(t)


def test_a_steady_or_recovering_delay_is_not_flagged():
    for delays in ([1800] * 6, [1800, 1700, 1500, 1300, 1100, 900], [600, 640, 700, 700, 690, 720]):
        b = VehicleBoard()
        t = feed(b, delays)
        assert b.stalled_trips(t) == set(), delays


def test_too_little_history_is_not_enough_to_accuse_a_trip():
    b = VehicleBoard()
    assert b.stalled_trips(feed(b, [900, 1020, 1140])) == set()   # 3 readings / 4 min: could be anything


def test_growth_that_is_not_late_enough_to_matter_is_ignored():
    b = VehicleBoard()
    assert b.stalled_trips(feed(b, [0, 30, 60, 90, 120, 150])) == set()   # slow drift, never reaches 5 min late


def test_stalled_flag_expires_when_the_history_goes_stale():
    b = VehicleBoard()
    t = feed(b, [900, 1020, 1140, 1260, 1380, 1500])
    assert b.stalled_trips(t + 30 * 60) == set()                    # last reading 30 min ago: say nothing


def test_upcoming_means_the_scheduled_start_is_still_in_the_future():
    b = VehicleBoard()
    b.apply_trip_update({"trip_id": "T1", "start_date": "20261108", "current_delay_seconds": 0, "event_time": NOW, "start_time": "21:35:00"})
    from gtfsrt.trip_start import scheduled_start_epoch

    sched = scheduled_start_epoch("20261108", "21:35:00")
    assert b.upcoming_trips(sched - 600) == {KEY} and b.upcoming_trips(sched + 1) == set()
    b.apply_trip_update({"trip_id": "T2", "start_date": "20261108", "current_delay_seconds": 0, "event_time": NOW})   # no start_time
    assert "20261108|T2" not in b.upcoming_trips(sched - 600)      # unknown start: never claimed to be upcoming


def test_summary_counts_stalled_vehicles():
    from dashboard.vehicles import summarize

    assert summarize([{"delay_s": 900, "stalled": 1}, {"delay_s": 900}, {"delay_s": None}])["stalled"] == 1


def test_three_readings_are_never_enough_even_if_they_span_enough_time():
    b = VehicleBoard()
    t = feed(b, [900, 1200, 1500], step=300)             # 10 minutes, perfect 1:1 growth, but only 3 readings
    assert b.stalled_trips(t) == set()


def test_growth_one_for_one_is_ignored_while_the_trip_is_still_under_five_minutes_late():
    b = VehicleBoard()
    t = feed(b, [-300, -180, -60, 60, 180, 280])         # an early trip drifting to slightly late: 1:1 growth, ends at 280 s
    assert b.stalled_trips(t) == set()
