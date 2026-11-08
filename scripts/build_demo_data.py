#!/usr/bin/env python3
"""Record a replay for the public demo from REAL data sitting in Kafka.

    PYTHONPATH=src:. S3_ENDPOINT_URL=http://localhost:5555 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
        ./.venv/bin/python scripts/build_demo_data.py --from 2026-11-08T17:00:00Z --to 2026-11-08T18:20:00Z

Why it exists: a recruiter cannot run Kafka + Spark, so the dashboard has a demo mode that replays a recording. To keep the
recording honest it is NOT synthetic and NOT a lookalike: every frame is produced by replaying the real trip-update and
vehicle-position events, in time order, through the SAME code the live system uses
(VehicleBoard, the alert state machine in gtfsrt.alerting, build_active_alerts, analytics.reliability).

Output (web/public/demo/): frames.json, routes.json, reliability.json, meta.json
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import time
from pathlib import Path

from confluent_kafka import Consumer

from dashboard import analytics, sources
from dashboard.alerts import build_active_alerts
from dashboard.vehicles import VehicleBoard, summarize
from gtfsrt.alerting import LATE, Observation, TripState, expire, step
from gtfsrt.config import AlertConfig

EXPIRY_S = 600  # same default as streaming/delay_alerts.py ALERT_EXPIRY_SECONDS
RESOLVING = {"DELAY_RECOVERED", "DELAY_EXPIRED"}


def parse_ts(s: str) -> int:
    return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())


def read_topic(topic: str, bootstrap: str) -> list[dict]:
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"demo-builder-{topic}-{int(time.time())}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    out, idle = [], 0
    while idle < 3:
        ms = c.consume(5000, timeout=3.0)
        if not ms:
            idle += 1
            continue
        idle = 0
        out += [json.loads(m.value()) for m in ms if not m.error()]
    c.close()
    return sorted(out, key=lambda r: r.get("event_time") or 0)


def pick_frames(vehicle_events: list[dict], t0: int, t1: int, max_frames: int) -> list[int]:
    stamps = sorted({r["feed_timestamp"] for r in vehicle_events if t0 <= r["feed_timestamp"] <= t1})
    if len(stamps) <= max_frames:
        return stamps
    step_ = (len(stamps) - 1) / (max_frames - 1)
    return [stamps[round(i * step_)] for i in range(max_frames)]


def compact_vehicle(v: dict) -> dict:
    out = {"id": v["id"], "lat": round(v["lat"], 4), "lon": round(v["lon"], 4), "route_id": v["route_id"],
           "trip_id": v["trip_id"], "delay_s": v["delay_s"], "age_s": v["age_s"], "bearing": None}
    if v.get("stalled"):
        out["stalled"] = 1
    return out


def compact_alert(a: dict) -> dict:
    keep = {k: a[k] for k in ("key", "trip_id", "route_id", "tier", "severity", "delay_s", "delay_at_alert_s", "delay_is_current",
                              "stalled", "upcoming", "episode", "changed_at", "changed_min")}
    v = a["vehicle"]
    keep["vehicle"] = None if v is None else {"id": v["id"], "lat": round(v["lat"], 4), "lon": round(v["lon"], 4), "age_s": v["age_s"]}
    return keep


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="t_from", required=True)
    ap.add_argument("--to", dest="t_to", required=True)
    ap.add_argument("--max-frames", type=int, default=36)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--out", default="web/public/demo")
    ap.add_argument("--routes", default="data/gtfs_static/routes.json")
    a = ap.parse_args()
    t0, t1 = parse_ts(a.t_from), parse_ts(a.t_to)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)

    print("reading Kafka ...")
    trips = [r for r in read_topic("gtfsr.trip_updates", a.bootstrap) if r.get("event_time") and r["event_time"] <= t1]
    vehicles = [r for r in read_topic("gtfsr.vehicle_positions", a.bootstrap) if r.get("event_time") and r["event_time"] <= t1]
    frame_times = pick_frames(vehicles, t0, t1, a.max_frames)
    if len(frame_times) < 2:
        raise SystemExit(f"fewer than 2 vehicle snapshots between {a.t_from} and {a.t_to}: pick a window inside the recorded data")
    print(f"{len(trips):,} trip updates, {len(vehicles):,} vehicle events -> {len(frame_times)} frames")

    cfg = AlertConfig()
    board = VehicleBoard()
    states: dict[str, TripState] = {}
    rows: dict[str, dict] = {}  # trip_key -> latest non-resolving alert (what DynamoDB's active table would hold)
    ti = vi = 0
    frames = []
    for T in frame_times:
        while vi < len(vehicles) and vehicles[vi]["event_time"] <= T:
            board.apply_vehicle(vehicles[vi])
            vi += 1
        while ti < len(trips) and trips[ti]["event_time"] <= T:
            r = trips[ti]
            ti += 1
            board.apply_trip_update(r)
            if r.get("schedule_relationship") != "SCHEDULED" or not r.get("trip_id"):
                continue
            obs = Observation(trip_id=r["trip_id"], start_date=r["start_date"], event_time=r["event_time"],
                              delay_seconds=r.get("current_delay_seconds"), route_id=r.get("route_id"),
                              vehicle_id=r.get("vehicle_id"), next_stop_id=r.get("next_stop_id"))
            states[obs.key], emitted = step(states.get(obs.key, TripState()), obs, cfg)
            for al in emitted:
                if al["alert_type"] in RESOLVING:
                    rows.pop(al["trip_key"], None)
                else:
                    rows[al["trip_key"]] = al
        for key, st in list(states.items()):  # silent trips are closed out, exactly like the Spark timeout
            if st.status == LATE and T - st.last_event_time > EXPIRY_S:
                _, closing = expire(st, cfg)
                rows.pop(key, None)
                states[key] = TripState()
        items = [{"trip_key": k, "tier": al["tier"], "delay_seconds": al["delay_seconds"], "event_time": al["event_time"],
                  "route_id": al.get("route_id"), "trip_id": al.get("trip_id"), "episode": al.get("episode", 0)}
                 for k, al in rows.items()]
        veh = board.snapshot(T)
        alerts = build_active_alerts(items, board.positions_by_trip(T), T, board.current_delays(T),
                                     board.stalled_trips(T), board.upcoming_trips(T))
        frames.append({"t": T, "stats": summarize(veh), "vehicles": [compact_vehicle(v) for v in veh],
                       "alerts": {"summary": alerts["summary"], "items": [compact_alert(x) for x in alerts["items"]]}})
        print(f"  frame {len(frames):>2}/{len(frame_times)} {dt.datetime.fromtimestamp(T, dt.timezone.utc):%H:%M}Z "
              f"vehicles={len(veh)} alerts={alerts['summary']['active']} stalled={alerts['summary']['stalled']}")

    (out / "frames.json").write_text(json.dumps(frames, separators=(",", ":")), encoding="utf-8")

    routes = json.loads(Path(a.routes).read_text(encoding="utf-8"))
    (out / "routes.json").write_text(json.dumps({"available": True, **routes}, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")

    df = sources.route_windows()
    rel = analytics.reliability(df, now=t1, hours=24, min_obs=100, min_trips=3, limit=15)
    (out / "reliability.json").write_text(json.dumps({"available": True, "stale": False, **rel}, separators=(",", ":")), encoding="utf-8")

    gaps = sorted(b - a_ for a_, b in zip(frame_times, frame_times[1:]))
    meta = {"from": frame_times[0], "to": frame_times[-1], "frames": len(frames), "median_gap_s": gaps[len(gaps) // 2],
            "generated_at": int(time.time()), "source": "National Transport Authority (NTA) GTFS-Realtime, recorded from the live API",
            "attribution": "Contains data from the National Transport Authority / Transport for Ireland, licensed CC BY 4.0",
            "trip_updates_replayed": len(trips), "vehicle_events_replayed": len(vehicles)}
    (out / "meta.json").write_text(json.dumps(meta, indent=1), encoding="utf-8")
    total = sum(p.stat().st_size for p in out.glob("*.json"))
    print(f"wrote {out} ({total/1e6:.1f} MB uncompressed): " + ", ".join(f"{p.name} {p.stat().st_size/1e3:.0f} KB" for p in sorted(out.glob('*.json'))))


if __name__ == "__main__":
    main()
