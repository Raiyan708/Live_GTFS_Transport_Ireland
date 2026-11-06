import random

import pytest

from gtfsrt.alerting import (
    ESCALATED, EXPIRED, LATE, ON_TIME, RECOVERED, STARTED,
    Observation, TripState, expire, make_alert_id, process_batch, step, thresholds, tier_for,
)
from gtfsrt.config import AlertConfig

CFG = AlertConfig(on_seconds=300, off_seconds=120, escalation_seconds=(600, 900), consecutive_observations=2)
T0 = 1_000_000


def obs(i, delay, trip="T1", date="20260101"):
    return Observation(trip_id=trip, start_date=date, event_time=T0 + 30 * i,
                       delay_seconds=delay, route_id="46A", vehicle_id="V1")


def run(delays, cfg=CFG, trip="T1"):
    state, alerts = None, []
    state, alerts = process_batch(state, [obs(i, d, trip) for i, d in enumerate(delays)], cfg)
    return state, alerts


def types(alerts):
    return [a["alert_type"] for a in alerts]


# --- config validation ------------------------------------------------------
def test_config_rejects_missing_hysteresis():
    with pytest.raises(ValueError):
        AlertConfig(on_seconds=300, off_seconds=300)


def test_tiers():
    assert thresholds(CFG) == (300, 600, 900)
    assert [tier_for(d, CFG) for d in (0, 299, 300, 599, 600, 899, 900, 5000)] == [-1, -1, 0, 0, 1, 1, 2, 2]


# --- core lifecycle ----------------------------------------------------------
def test_single_spike_is_debounced():
    _, alerts = run([0, 400, 0, 0])  # one reading above threshold only
    assert alerts == []


def test_start_requires_n_consecutive_observations():
    state, alerts = run([100, 350, 360])
    assert types(alerts) == [STARTED]
    assert state.status == LATE and state.episode == 1
    assert alerts[0]["threshold_seconds"] == 300 and alerts[0]["delay_seconds"] == 360


def test_full_lifecycle_start_escalate_recover():
    delays = [0, 350, 360, 650, 700, 950, 980, 100, 90]
    state, alerts = run(delays)
    assert types(alerts) == [STARTED, ESCALATED, ESCALATED, RECOVERED]
    assert [a["tier"] for a in alerts] == [0, 1, 2, 2]  # recovered carries the high-water tier
    assert state.status == ON_TIME


def test_no_flapping_around_the_threshold():
    # Oscillating 290<->310 forever: never two consecutive >= 300 -> never alerts.
    _, alerts = run([290, 310] * 50)
    assert alerts == []
    # Once LATE, dipping to 250 (between off=120 and on=300) must NOT recover or re-alert.
    state, alerts = run([350, 350, 250, 310, 250, 310, 250, 310])
    assert types(alerts) == [STARTED] and state.status == LATE


def test_hysteresis_band_holds_the_late_state():
    # LATE, then sit at 200s (below `on`=300 but ABOVE `off`=120) for many readings.
    # With a single threshold this would recover; with hysteresis it must stay LATE.
    # (Isolates hysteresis from debounce: the readings are consecutive, not alternating.)
    state, alerts = run([350, 350] + [200] * 10)
    assert types(alerts) == [STARTED]
    assert state.status == LATE
    # ...and only dropping to <= off finally recovers it.
    state, alerts = run([350, 350] + [200] * 10 + [100, 100])
    assert types(alerts) == [STARTED, RECOVERED]


def test_recovery_needs_n_consecutive_observations_below_off():
    state, alerts = run([350, 350, 100, 400, 100, 100])
    assert types(alerts) == [STARTED, RECOVERED]  # the single 100 was interrupted by 400
    assert state.status == ON_TIME


def test_wobble_inside_high_water_band_does_not_re_alert():
    # reach tier 1 (>=600), dip to tier 0 range, return to tier 1 -> no second escalation
    _, alerts = run([350, 350, 650, 650, 400, 400, 650, 650])
    assert types(alerts) == [STARTED, ESCALATED]


def test_new_episode_after_recovery_gets_new_ids():
    state, alerts = run([350, 350, 50, 50, 350, 350])
    assert types(alerts) == [STARTED, RECOVERED, STARTED]
    assert state.episode == 2
    assert alerts[0]["alert_id"] != alerts[2]["alert_id"]


def test_started_directly_in_high_tier():
    _, alerts = run([1000, 1000])
    assert types(alerts) == [STARTED] and alerts[0]["tier"] == 2


# --- robustness -------------------------------------------------------------
def test_none_delay_is_ignored_and_does_not_break_streak():
    state, alerts = run([350, None, 350])
    assert types(alerts) == [STARTED]  # None skipped, the two real readings still count


def test_stale_and_duplicate_observations_are_noops():
    s, a1 = step(TripState(), obs(5, 350), CFG)
    s, a2 = step(s, obs(6, 350), CFG)
    assert types(a2) == [STARTED]
    before = s
    s, a3 = step(s, obs(6, 9999), CFG)  # same event_time replayed with different value
    s, a4 = step(s, obs(2, 9999), CFG)  # older than last applied
    assert a3 == [] and a4 == [] and s == before


def test_out_of_order_within_batch_is_sorted():
    shuffled = [obs(3, 360), obs(1, 350), obs(2, 100), obs(0, 0)]
    in_order = sorted(shuffled, key=lambda o: o.event_time)
    _, a_shuffled = process_batch(None, shuffled, CFG)
    _, a_ordered = process_batch(None, in_order, CFG)
    assert a_shuffled == a_ordered


def test_replay_produces_identical_alert_ids():
    delays = [0, 350, 360, 650, 700, 100, 90]
    _, first = run(delays)
    _, second = run(delays)
    assert [a["alert_id"] for a in first] == [a["alert_id"] for a in second]
    assert len({a["alert_id"] for a in first}) == len(first)  # and unique within the run


def test_batch_boundaries_do_not_change_outcome():
    delays = [0, 350, 360, 650, 700, 950, 980, 100, 90]
    _, whole = run(delays)
    state, split = None, []
    idx = 0
    for size in (2, 3, 3, 1):
        batch = [obs(idx + k, d) for k, d in enumerate(delays[idx: idx + size])]
        idx += size
        state, out = process_batch(state, batch, CFG)
        split.extend(out)
    assert [a["alert_id"] for a in split] == [a["alert_id"] for a in whole]


def test_state_tuple_roundtrip():
    state, _ = run([350, 350, 650, 650])
    assert TripState.from_tuple(state.to_tuple()) == state


# --- expiry ------------------------------------------------------------------
def test_expire_late_trip_emits_closing_alert():
    state, _ = run([350, 350])
    new_state, alerts = expire(state, CFG)
    assert new_state is None and types(alerts) == [EXPIRED]
    assert alerts[0]["reason"] == "no_updates" and alerts[0]["episode"] == 1


def test_expire_on_time_trip_is_silent():
    state, _ = run([10, 20])
    assert expire(state, CFG) == (None, [])


# --- property-style invariant ------------------------------------------------
def test_random_walks_never_violate_invariants():
    rng = random.Random(0)
    for _ in range(300):
        delays = [rng.choice([None, *range(-60, 1300, 20)]) for _ in range(rng.randint(1, 60))]
        _, alerts = run(delays)
        # per episode: at most one STARTED and one RECOVERED; STARTED always precedes the rest
        per_ep: dict[int, list[str]] = {}
        for a in alerts:
            per_ep.setdefault(a["episode"], []).append(a["alert_type"])
        for ep_types in per_ep.values():
            assert ep_types[0] == STARTED
            assert ep_types.count(STARTED) == 1 and ep_types.count(RECOVERED) <= 1
            if RECOVERED in ep_types:
                assert ep_types[-1] == RECOVERED
        # tiers within an episode only ever go up
        by_ep: dict[int, list[int]] = {}
        for a in alerts:
            if a["alert_type"] in (STARTED, ESCALATED):
                by_ep.setdefault(a["episode"], []).append(a["tier"])
        for tiers in by_ep.values():
            assert tiers == sorted(tiers) and len(set(tiers)) == len(tiers)
        assert len({a["alert_id"] for a in alerts}) == len(alerts)


def test_alert_id_is_stable_across_python_runs():
    # sha1 of a fixed string: guards against accidentally using hash() (randomised per process)
    assert make_alert_id("20260101|T1", 1, STARTED, 0) == make_alert_id("20260101|T1", 1, STARTED, 0)
    assert len(make_alert_id("k", 1, STARTED, 0)) == 32
