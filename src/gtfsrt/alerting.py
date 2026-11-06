"""Per-trip delay alert state machine  -  PURE PYTHON, no Spark, no I/O.

Spark's `applyInPandasWithState` only *supplies* (state, ordered observations)
per trip key and persists the state we return. All business rules live here so
they can be unit-tested in milliseconds.

Lifecycle of one trip:

    ON_TIME --(delay >= on, N times in a row)--> LATE            emit DELAY_STARTED
    LATE    --(delay above a NEW higher tier, N times)--> LATE   emit DELAY_ESCALATED
    LATE    --(delay <= off, N times in a row)--> ON_TIME        emit DELAY_RECOVERED
    LATE    --(no updates for `expiry` seconds)--> (state dropped) emit DELAY_EXPIRED

Why these rules? See docs/DECISION_LOG.md  D0.2 / D0.3 / D0.5.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from typing import Iterable

from .config import AlertConfig

ON_TIME = "ON_TIME"
LATE = "LATE"

STARTED = "DELAY_STARTED"
ESCALATED = "DELAY_ESCALATED"
RECOVERED = "DELAY_RECOVERED"
EXPIRED = "DELAY_EXPIRED"


@dataclass(frozen=True)
class Observation:
    """One trip-update reading, as the state machine sees it."""

    trip_id: str
    start_date: str
    event_time: int  # epoch seconds
    delay_seconds: int | None
    route_id: str | None = None
    vehicle_id: str | None = None
    next_stop_id: str | None = None

    @property
    def key(self) -> str:
        return f"{self.start_date}|{self.trip_id}"


@dataclass(frozen=True)
class TripState:
    status: str = ON_TIME
    tier: int = 0  # high-water-mark tier reached in the current episode
    episode: int = 0  # increments every time the trip becomes LATE
    on_streak: int = 0  # consecutive obs >= on   (while ON_TIME)
    up_streak: int = 0  # consecutive obs above current tier (while LATE)
    clear_streak: int = 0  # consecutive obs <= off (while LATE)
    last_event_time: int = 0
    last_delay: int = 0
    # carried so that an expiry alert can still describe the trip
    trip_id: str = ""
    start_date: str = ""
    route_id: str = ""
    vehicle_id: str = ""

    # Spark persists state as a flat tuple; keep conversion next to the type.
    FIELDS = (
        "status", "tier", "episode", "on_streak", "up_streak", "clear_streak",
        "last_event_time", "last_delay", "trip_id", "start_date", "route_id", "vehicle_id",
    )
    SPARK_SCHEMA = (
        "status string, tier int, episode int, on_streak int, up_streak int, clear_streak int, "
        "last_event_time long, last_delay int, trip_id string, start_date string, "
        "route_id string, vehicle_id string"
    )

    def to_tuple(self) -> tuple:
        return tuple(getattr(self, f) for f in self.FIELDS)

    @classmethod
    def from_tuple(cls, t: Iterable) -> "TripState":
        return cls(**dict(zip(cls.FIELDS, t)))


def thresholds(cfg: AlertConfig) -> tuple[int, ...]:
    """[on, tier1, tier2, ...] ascending. Index == tier number."""
    return (cfg.on_seconds, *sorted(cfg.escalation_seconds))


def tier_for(delay: int, cfg: AlertConfig) -> int:
    """Highest tier whose threshold <= delay; -1 if below `on`."""
    tier = -1
    for i, thr in enumerate(thresholds(cfg)):
        if delay >= thr:
            tier = i
    return tier


def make_alert_id(trip_key: str, episode: int, alert_type: str, tier: int) -> str:
    """Deterministic: replaying the same input yields the SAME id (idempotent consumers)."""
    raw = f"{trip_key}|{episode}|{alert_type}|{tier}"
    return hashlib.sha1(raw.encode()).hexdigest()[:32]


def _alert(
    state: TripState, cfg: AlertConfig, alert_type: str, delay: int, event_time: int,
    next_stop_id: str | None = None, reason: str | None = None,
) -> dict:
    key = f"{state.start_date}|{state.trip_id}"
    thr = thresholds(cfg)
    return {
        "alert_id": make_alert_id(key, state.episode, alert_type, state.tier),
        "alert_type": alert_type,
        "trip_key": key,
        "trip_id": state.trip_id,
        "start_date": state.start_date,
        "route_id": state.route_id or None,
        "vehicle_id": state.vehicle_id or None,
        "delay_seconds": delay,
        "tier": state.tier,
        "threshold_seconds": thr[state.tier] if 0 <= state.tier < len(thr) else None,
        "episode": state.episode,
        "event_time": event_time,
        "next_stop_id": next_stop_id,
        "reason": reason,
    }


def step(state: TripState, obs: Observation, cfg: AlertConfig) -> tuple[TripState, list[dict]]:
    """Apply ONE observation. Returns (new_state, alerts_to_emit)."""
    # Identity is carried in state so expiry alerts can describe the trip.
    s = replace(
        state,
        trip_id=obs.trip_id,
        start_date=obs.start_date,
        route_id=obs.route_id or state.route_id,
        vehicle_id=obs.vehicle_id or state.vehicle_id,
    )

    # 1. Duplicate / stale / replayed observation -> no-op (idempotence).
    if obs.event_time <= s.last_event_time:
        return state, []

    # 2. No delay information (e.g. cancelled trip): record time, change nothing else.
    if obs.delay_seconds is None:
        return replace(s, last_event_time=obs.event_time), []

    delay = obs.delay_seconds
    s = replace(s, last_event_time=obs.event_time, last_delay=delay)
    n = cfg.consecutive_observations
    alerts: list[dict] = []

    if s.status == ON_TIME:
        streak = s.on_streak + 1 if delay >= cfg.on_seconds else 0
        if streak >= n:
            s = replace(
                s, status=LATE, episode=s.episode + 1, tier=max(0, tier_for(delay, cfg)),
                on_streak=0, up_streak=0, clear_streak=0,
            )
            alerts.append(_alert(s, cfg, STARTED, delay, obs.event_time, obs.next_stop_id))
        else:
            s = replace(s, on_streak=streak)
        return s, alerts

    # status == LATE ---------------------------------------------------------
    # Recovery check first: dropping to <= off clears the episode.
    if delay <= cfg.off_seconds:
        clear = s.clear_streak + 1
        if clear >= n:
            alerts.append(_alert(s, cfg, RECOVERED, delay, obs.event_time, obs.next_stop_id))
            s = replace(s, status=ON_TIME, tier=0, on_streak=0, up_streak=0, clear_streak=0)
        else:
            s = replace(s, clear_streak=clear, up_streak=0)
        return s, alerts

    # Still late. Escalate only past the episode's high-water mark.
    candidate = tier_for(delay, cfg)
    if candidate > s.tier:
        up = s.up_streak + 1
        if up >= n:
            s = replace(s, tier=candidate, up_streak=0, clear_streak=0)
            alerts.append(_alert(s, cfg, ESCALATED, delay, obs.event_time, obs.next_stop_id))
        else:
            s = replace(s, up_streak=up, clear_streak=0)
    else:
        s = replace(s, up_streak=0, clear_streak=0)  # wobble inside the band: stay quiet
    return s, alerts


def expire(state: TripState, cfg: AlertConfig) -> tuple[None, list[dict]]:
    """Trip went silent. If it was LATE, close the episode so nothing stays 'active' forever."""
    if state.status != LATE:
        return None, []
    return None, [
        _alert(state, cfg, EXPIRED, state.last_delay, state.last_event_time, reason="no_updates")
    ]


def process_batch(
    state: TripState | None, observations: Iterable[Observation], cfg: AlertConfig
) -> tuple[TripState, list[dict]]:
    """Process one trip's observations from a micro-batch.

    Spark does NOT guarantee row order inside a micro-batch, so we sort by
    event_time here. (Per-partition order from Kafka helps across batches, not within one.)
    """
    s = state or TripState()
    out: list[dict] = []
    for obs in sorted(observations, key=lambda o: o.event_time):
        s, alerts = step(s, obs, cfg)
        out.extend(alerts)
    return s, out
