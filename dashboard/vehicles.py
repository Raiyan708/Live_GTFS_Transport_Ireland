"""Live vehicle board: the latest known position of every vehicle, joined to its trip's current delay.

Feeds the map in web/. Two parts:
  * VehicleBoard : pure, thread-safe state (no Kafka, no web)  -> unit-tested
  * KafkaTail    : a background thread that reads gtfsr.vehicle_positions + gtfsr.trip_updates into the board

Local vs AWS: here the board is rebuilt from the last few minutes of Kafka on start-up. In AWS the same
VehicleBoard interface would be backed by a DynamoDB "latest position" table written by a streaming job
(see docs/DECISION_LOG.md), so the API layer would not change.
"""
from __future__ import annotations

import json
import threading
import time
from collections import deque
from typing import Any

from gtfsrt.decode import trip_key
from gtfsrt.trip_start import scheduled_start_epoch

LATE_SECONDS = 300          # same threshold as the alert state machine's "late" band
DELAY_TTL_SECONDS = 15 * 60  # a trip delay older than this is shown as "unknown", never as current
VEHICLE_MAX_AGE_SECONDS = 30 * 60
# (south, north, west, east) with margin around the island of Ireland incl. Northern Ireland. Live data showed
# vehicles at lat 0.00004 / lon -0.00002: "almost zero" that an exact `== 0` check does not catch.
IRELAND_BOUNDS = (51.0, 55.6, -10.8, -5.2)

# "Not progressing" (a stalled or never-started trip): over >= 8 min and >= 4 readings the delay grew by >= 80% of the elapsed
# time, and the trip is >= 5 min late. A trip that is moving through its timetable does not do this. Measured on the live
# feed: 10 of the 18 trips with a 90+ min delay had a fitted delay-vs-clock slope of ~1.0 (route 261: 0.998).
STALL_MIN_SPAN_S = 8 * 60
STALL_MIN_POINTS = 4
STALL_MIN_SLOPE = 0.8
STALL_MIN_DELAY_S = 300
HISTORY_KEEP_S = 20 * 60


def in_ireland(lat: float, lon: float) -> bool:
    south, north, west, east = IRELAND_BOUNDS
    return south <= lat <= north and west <= lon <= east


class VehicleBoard:
    """Latest position per vehicle + latest delay per trip. Thread-safe; `version` bumps on every real change."""

    def __init__(self) -> None:
        self._vehicles: dict[str, dict[str, Any]] = {}
        self._delays: dict[str, tuple[int, int | None]] = {}  # trip_key -> (event_time, delay_seconds | None)
        self._history: dict[str, deque[tuple[int, int]]] = {}  # trip_key -> recent (event_time, delay), oldest first
        self._scheduled: dict[str, int] = {}  # trip_key -> scheduled start (epoch); absent = unknown
        self._lock = threading.Lock()
        self.version = 0
        self.last_update_epoch: float | None = None  # wall-clock time of the last accepted event
        self.rejected_out_of_bounds = 0  # surfaced on /api/health: bad coordinates are counted, never silent

    # ------------------------------------------------------------------ writes
    def apply_vehicle(self, ev: dict[str, Any]) -> bool:
        vid, lat, lon = ev.get("vehicle_id"), ev.get("latitude"), ev.get("longitude")
        et = int(ev.get("event_time") or 0)
        if not vid or lat is None or lon is None or not et:
            return False
        # Protobuf's unset default is 0/0 and some vehicles report near-zero junk: never plot them.
        if not in_ireland(float(lat), float(lon)):
            with self._lock:
                self.rejected_out_of_bounds += 1
            return False
        with self._lock:
            old = self._vehicles.get(vid)
            if old is not None and et <= old["event_time"]:
                return False  # duplicate or out-of-order: a replay must never move a bus backwards
            self._vehicles[vid] = {
                "id": vid, "lat": float(lat), "lon": float(lon), "bearing": ev.get("bearing"),
                "speed_mps": ev.get("speed_mps"), "route_id": ev.get("route_id") or None,
                "trip_id": ev.get("trip_id") or None, "start_date": ev.get("start_date") or None,
                "event_time": et,
            }
            self._touch()
            return True

    def apply_trip_update(self, ev: dict[str, Any]) -> bool:
        tid, sd = ev.get("trip_id"), ev.get("start_date")
        et = int(ev.get("event_time") or 0)
        if not tid or not sd or not et:
            return False
        key = trip_key(sd, tid)
        with self._lock:
            old = self._delays.get(key)
            if old is not None and et <= old[0]:
                return False
            delay = ev.get("current_delay_seconds")
            self._delays[key] = (et, delay)  # None = canceled / no delay info
            if delay is not None:
                h = self._history.setdefault(key, deque(maxlen=16))
                h.append((et, int(delay)))
                while h and et - h[0][0] > HISTORY_KEEP_S:
                    h.popleft()
            start = scheduled_start_epoch(sd, ev.get("start_time"))
            if start is not None:
                self._scheduled[key] = start
            self._touch()
            return True

    def _touch(self) -> None:
        self.version += 1
        self.last_update_epoch = time.time()

    def prune(self, now_epoch: float) -> None:
        with self._lock:
            self._vehicles = {k: v for k, v in self._vehicles.items()
                              if now_epoch - v["event_time"] <= VEHICLE_MAX_AGE_SECONDS}
            self._delays = {k: v for k, v in self._delays.items() if now_epoch - v[0] <= DELAY_TTL_SECONDS * 2}
            self._history = {k: h for k, h in self._history.items() if k in self._delays}
            self._scheduled = {k: v for k, v in self._scheduled.items() if k in self._delays}

    # ------------------------------------------------------------------ reads
    def current_delays(self, now_epoch: float) -> dict[str, int]:
        """trip_key -> latest known delay (seconds) for EVERY trip in the trip-update feed, not just those with a vehicle.
        Entries older than the TTL are omitted: an old delay is unknown, never 'current'."""
        with self._lock:
            return {k: d for k, (et, d) in self._delays.items() if d is not None and now_epoch - et <= DELAY_TTL_SECONDS}

    def stalled_trips(self, now_epoch: float) -> set[str]:
        """Trips whose delay is growing as fast as the clock: not progressing (stalled, stuck or never started)."""
        out = set()
        with self._lock:
            for key, h in self._history.items():
                if len(h) < STALL_MIN_POINTS or now_epoch - h[-1][0] > DELAY_TTL_SECONDS:
                    continue
                span = h[-1][0] - h[0][0]
                grew = h[-1][1] - h[0][1]
                if span >= STALL_MIN_SPAN_S and h[-1][1] >= STALL_MIN_DELAY_S and grew >= STALL_MIN_SLOPE * span:
                    out.add(key)
        return out

    def upcoming_trips(self, now_epoch: float) -> set[str]:
        """Trips whose scheduled start is still in the future (their 'delay' is a prediction, often a timetable placeholder)."""
        with self._lock:
            return {k for k, start in self._scheduled.items() if start > now_epoch}

    def positions_by_trip(self, now_epoch: float) -> dict[str, dict[str, Any]]:
        """trip_key -> where its vehicle is. Lets an alert say 'on the map here' or 'no vehicle reporting yet'."""
        with self._lock:
            return {
                trip_key(v["start_date"], v["trip_id"]): {
                    "id": v["id"], "lat": round(v["lat"], 5), "lon": round(v["lon"], 5),
                    "age_s": max(0, int(now_epoch - v["event_time"])),
                }
                for v in self._vehicles.values() if v["trip_id"] and v["start_date"]
            }

    def snapshot(self, now_epoch: float) -> list[dict[str, Any]]:
        stalled = self.stalled_trips(now_epoch)
        with self._lock:
            out = []
            for v in self._vehicles.values():
                delay = None
                if v["trip_id"] and v["start_date"]:
                    d = self._delays.get(trip_key(v["start_date"], v["trip_id"]))
                    if d is not None and now_epoch - d[0] <= DELAY_TTL_SECONDS:
                        delay = d[1]
                item = {
                    "id": v["id"], "lat": round(v["lat"], 5), "lon": round(v["lon"], 5),
                    "bearing": v["bearing"], "route_id": v["route_id"], "trip_id": v["trip_id"],
                    "delay_s": delay, "age_s": max(0, int(now_epoch - v["event_time"])),
                }
                if v["trip_id"] and v["start_date"] and trip_key(v["start_date"], v["trip_id"]) in stalled:
                    item["stalled"] = 1  # only present when true: keeps the payload small
                out.append(item)
        out.sort(key=lambda r: r["id"])
        return out


def summarize(vehicles: list[dict[str, Any]]) -> dict[str, Any]:
    """Headline numbers for the top bar. Delay stats only count vehicles whose delay is actually known."""
    known = [v["delay_s"] for v in vehicles if v["delay_s"] is not None]
    return {
        "vehicles": len(vehicles),
        "with_delay": len(known),
        "late": sum(d >= LATE_SECONDS for d in known),
        "median_delay_s": sorted(known)[len(known) // 2] if known else None,
        "stalled": sum(1 for v in vehicles if v.get("stalled")),
    }


# ---------------------------------------------------------------------------- Kafka I/O
class KafkaTail(threading.Thread):  # pragma: no cover - needs a broker
    """Read-only consumer: starts `lookback_s` in the past (bounded: not 24 h of retention), never commits."""

    def __init__(self, board: VehicleBoard, bootstrap: str, vehicle_topic: str, trip_topic: str,
                 lookback_s: int = 6 * 60):
        super().__init__(daemon=True, name="kafka-tail")
        self.board, self.bootstrap, self.lookback_s = board, bootstrap, lookback_s
        self.topics = {vehicle_topic: board.apply_vehicle, trip_topic: board.apply_trip_update}
        self.stop_event = threading.Event()
        self.error: str | None = None

    def run(self) -> None:
        from confluent_kafka import Consumer, TopicPartition

        try:
            c = Consumer({"bootstrap.servers": self.bootstrap, "group.id": "dashboard-vehicles",
                          "enable.auto.commit": False, "auto.offset.reset": "latest"})
            since_ms = int((time.time() - self.lookback_s) * 1000)
            parts = []
            for topic in self.topics:
                meta = c.list_topics(topic, timeout=10).topics[topic]
                parts += [TopicPartition(topic, p, since_ms) for p in meta.partitions]
            c.assign(c.offsets_for_times(parts, timeout=10))  # -1 offsets mean "no data since then": start at end
            last_prune = time.time()
            while not self.stop_event.is_set():
                for m in c.consume(500, timeout=1.0):
                    if m.error() is not None:
                        continue
                    try:
                        self.topics[m.topic()](json.loads(m.value()))
                    except (ValueError, KeyError, TypeError):
                        continue  # one bad message must not stop the board
                if time.time() - last_prune > 60:
                    self.board.prune(time.time())
                    last_prune = time.time()
            c.close()
        except Exception as exc:  # surfaced on /api/health instead of dying silently
            self.error = f"{type(exc).__name__}: {exc}"
