"""Central configuration.

One place that reads the environment, so thresholds / topic names are never
scattered through the code. Everything has a safe local default so tests and
the simulator work with zero setup.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _csv_ints(value: str) -> tuple[int, ...]:
    return tuple(int(v) for v in value.split(",") if v.strip())


@dataclass(frozen=True)
class AlertConfig:
    """Thresholds for the per-trip delay state machine (all in seconds)."""

    on_seconds: int = 300  # enter LATE at/above this
    off_seconds: int = 120  # leave LATE at/below this (hysteresis: off < on)
    escalation_seconds: tuple[int, ...] = (600, 900)  # extra tiers above `on`
    consecutive_observations: int = 2  # debounce: need N in a row to change state

    def __post_init__(self) -> None:
        if self.off_seconds >= self.on_seconds:
            raise ValueError("off_seconds must be < on_seconds, otherwise there is no hysteresis")
        if self.consecutive_observations < 1:
            raise ValueError("consecutive_observations must be >= 1")
        if any(t <= self.on_seconds for t in self.escalation_seconds):
            raise ValueError("escalation tiers must be above on_seconds")

    @classmethod
    def from_env(cls) -> "AlertConfig":
        return cls(
            on_seconds=int(os.getenv("ALERT_ON_SECONDS", "300")),
            off_seconds=int(os.getenv("ALERT_OFF_SECONDS", "120")),
            escalation_seconds=_csv_ints(os.getenv("ALERT_ESCALATION_SECONDS", "600,900")),
            consecutive_observations=int(os.getenv("ALERT_CONSECUTIVE_OBSERVATIONS", "2")),
        )


@dataclass(frozen=True)
class Topics:
    trip_updates: str = "gtfsr.trip_updates"
    vehicle_positions: str = "gtfsr.vehicle_positions"
    delay_alerts: str = "gtfsr.delay_alerts"

    @classmethod
    def from_env(cls) -> "Topics":
        return cls(
            trip_updates=os.getenv("TOPIC_TRIP_UPDATES", cls.trip_updates),
            vehicle_positions=os.getenv("TOPIC_VEHICLE_POSITIONS", cls.vehicle_positions),
            delay_alerts=os.getenv("TOPIC_DELAY_ALERTS", cls.delay_alerts),
        )


# NTA fair-usage policy (found by running against the real API; see DECISIONS.md): ONE call per token per 60 s,
# shared across ALL endpoints. Calling both feeds every cycle gets throttled (HTTP 429).
MIN_LIVE_CALL_INTERVAL_SECONDS = 60


@dataclass(frozen=True)
class FeedConfig:
    mode: str = "simulator"  # "live" | "simulator"
    api_key: str = ""
    api_key_header: str = "x-api-key"
    trip_updates_url: str = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"
    vehicles_url: str = "https://api.nationaltransport.ie/gtfsr/v2/Vehicles"
    poll_interval_seconds: int = 30

    @property
    def call_interval_seconds(self) -> int:
        """Seconds between API CALLS. In live mode never below the fair-usage minimum, whatever the env says."""
        if self.mode == "live":
            return max(self.poll_interval_seconds, MIN_LIVE_CALL_INTERVAL_SECONDS)
        return self.poll_interval_seconds

    @classmethod
    def from_env(cls) -> "FeedConfig":
        # Variable names match the project's existing .env: NTA_API_KEY, NTA_API_KEY_HEADER,
        # NTA_VEHICLE_POSITIONS_URL, NTA_TRIP_UPDATES_URL. (NTA_VEHICLES_URL kept as a fallback alias.)
        mode = os.getenv("FEED_MODE", "simulator")
        return cls(
            mode=mode,
            api_key=os.getenv("NTA_API_KEY", ""),
            api_key_header=os.getenv("NTA_API_KEY_HEADER", cls.api_key_header),
            trip_updates_url=os.getenv("NTA_TRIP_UPDATES_URL", cls.trip_updates_url),
            vehicles_url=os.getenv("NTA_VEHICLE_POSITIONS_URL") or os.getenv("NTA_VEHICLES_URL", cls.vehicles_url),
            poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "60" if mode == "live" else "30")),
        )
