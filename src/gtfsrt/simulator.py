"""Synthetic GTFS-RT feed generator that emits *real protobuf bytes*.

Purpose: run the whole pipeline without an NTA API key, and give the alert
state machine realistic episodes (start / escalate / recover) to detect.

Each simulated trip has a delay profile:
  punctual  - noise around 0
  degrading - drifts steadily later
  incident  - sudden jump of several minutes, then slow recovery
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timezone

from google.transit import gtfs_realtime_pb2 as pb

ROUTES = ["46A", "145", "15", "39A", "16", "41", "C1", "747", "Luas-Green", "DART"]
PROFILES = ["punctual", "punctual", "punctual", "degrading", "incident"]


@dataclass
class _SimTrip:
    trip_id: str
    route_id: str
    vehicle_id: str
    profile: str
    delay: float = 0.0
    stop_seq: int = 1
    lat: float = 53.3498
    lon: float = -6.2603
    incident_at: int = 0
    n_stops: int = 40


@dataclass
class Simulator:
    seed: int = 42
    n_trips: int = 30
    _rng: random.Random = field(init=False, repr=False)
    _trips: list[_SimTrip] = field(init=False, repr=False)
    _tick: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        self._rng = random.Random(self.seed)
        self._trips = []
        for i in range(self.n_trips):
            route = ROUTES[i % len(ROUTES)]
            profile = self._rng.choice(PROFILES)
            self._trips.append(
                _SimTrip(
                    trip_id=f"SIM_{route}_{i:03d}",
                    route_id=route,
                    vehicle_id=f"VEH{1000 + i}",
                    profile=profile,
                    stop_seq=self._rng.randint(1, 10),
                    lat=53.30 + self._rng.random() * 0.1,
                    lon=-6.35 + self._rng.random() * 0.2,
                    incident_at=self._rng.randint(3, 15),
                )
            )

    # -- delay dynamics ---------------------------------------------------
    def _advance(self) -> None:
        self._tick += 1
        for t in self._trips:
            noise = self._rng.gauss(0, 15)
            if t.profile == "punctual":
                t.delay = 0.7 * t.delay + noise
            elif t.profile == "degrading":
                t.delay += 25 + noise  # ~+25s per tick
            elif t.profile == "incident":
                if self._tick == t.incident_at:
                    t.delay += self._rng.randint(420, 900)  # 7-15 min jump
                elif self._tick > t.incident_at:
                    t.delay = max(0.0, t.delay * 0.93 + noise)  # slow recovery
                else:
                    t.delay = 0.7 * t.delay + noise
            t.stop_seq = min(t.n_stops, t.stop_seq + (1 if self._tick % 2 == 0 else 0))
            t.lat += self._rng.gauss(0, 0.0004)
            t.lon += self._rng.gauss(0, 0.0004)

    # -- feed builders ----------------------------------------------------
    def _header(self, feed: pb.FeedMessage, now: int) -> None:
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.incrementality = pb.FeedHeader.FULL_DATASET
        feed.header.timestamp = now

    def trip_updates_feed(self, now: int) -> bytes:
        """Advance the world one tick and return a TripUpdates FeedMessage."""
        self._advance()
        feed = pb.FeedMessage()
        self._header(feed, now)
        start_date = datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y%m%d")
        for t in self._trips:
            e = feed.entity.add()
            e.id = f"tu-{t.trip_id}"
            tu = e.trip_update
            tu.trip.trip_id = t.trip_id
            tu.trip.route_id = t.route_id
            tu.trip.start_date = start_date
            tu.trip.schedule_relationship = pb.TripDescriptor.SCHEDULED
            tu.vehicle.id = t.vehicle_id
            tu.timestamp = now
            # two upcoming stops, both carrying the current delay
            for k in range(2):
                stu = tu.stop_time_update.add()
                stu.stop_sequence = t.stop_seq + k
                stu.stop_id = f"8220DB{t.stop_seq + k:06d}"
                stu.arrival.delay = int(t.delay)
                stu.departure.delay = int(t.delay)
        return feed.SerializeToString()

    def vehicles_feed(self, now: int) -> bytes:
        """Vehicle positions for the *current* world state (does not advance time)."""
        feed = pb.FeedMessage()
        self._header(feed, now)
        start_date = datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y%m%d")
        for t in self._trips:
            e = feed.entity.add()
            e.id = f"vp-{t.vehicle_id}"
            v = e.vehicle
            v.trip.trip_id = t.trip_id
            v.trip.route_id = t.route_id
            v.trip.start_date = start_date
            v.vehicle.id = t.vehicle_id
            v.position.latitude = t.lat
            v.position.longitude = t.lon
            v.current_stop_sequence = t.stop_seq
            v.timestamp = now
        return feed.SerializeToString()
