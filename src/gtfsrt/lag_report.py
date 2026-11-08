"""Profile REAL feed data sitting in Kafka, to answer the questions the simulator cannot.

    PYTHONPATH=src python -m gtfsrt.lag_report [--topic gtfsr.trip_updates] [--max 20000]

Reports:
  * event-time lag = feed_timestamp - event_time   -> sizes the Spark WATERMARK (route windows drop rows later than it)
  * how many records carry a delay at all, how many have empty trip_id / start_date
  * what route_id values really look like (the NTA ids may be internal variants, not "46A")
Works with either producer's records (`event_time`, or `trip_update_timestamp` from ingestion/transform.py).
Prints only aggregate numbers and a few example route ids: no API key or credentials are involved.
"""
from __future__ import annotations

import argparse
import json
import math
import os
from collections import Counter
from typing import Iterable

WATERMARK_SAFETY_SECONDS = 120  # one feed cycle: the watermark is computed from the PREVIOUS batch's max event time


def percentile(sorted_values: list[int], p: float) -> int:
    """Nearest-rank percentile on a pre-sorted list (no interpolation: reports a value that was really observed)."""
    if not sorted_values:
        raise ValueError("no values")
    rank = max(1, math.ceil(p / 100 * len(sorted_values)))
    return sorted_values[rank - 1]


def event_time_of(rec: dict) -> int | None:
    for k in ("event_time", "trip_update_timestamp"):
        v = rec.get(k)
        if v:  # 0 means "field not set" in protobuf, so it is treated as missing
            return int(v)
    return None


def summarize(records: Iterable[dict]) -> dict:
    lags, n, no_event_time, no_trip, no_date = [], 0, 0, 0, 0
    with_delay = 0
    routes: Counter = Counter()
    for r in records:
        n += 1
        et, ft = event_time_of(r), r.get("feed_timestamp")
        if et is None or not ft:
            no_event_time += 1
        else:
            lags.append(int(ft) - et)
        no_trip += not r.get("trip_id")
        no_date += not r.get("start_date")
        if r.get("current_delay_seconds") is not None:
            with_delay += 1
        if r.get("route_id"):
            routes[r["route_id"]] += 1
    lags.sort()
    out = {"records": n, "records_without_event_time": no_event_time, "empty_trip_id": no_trip,
           "empty_start_date": no_date, "with_delay_value": with_delay,
           "distinct_routes": len(routes), "route_id_examples": [r for r, _ in routes.most_common(8)],
           "lag_seconds": None, "suggested_watermark_seconds": None}
    if lags:
        p99 = percentile(lags, 99)
        out["lag_seconds"] = {
            "min": lags[0], "p50": percentile(lags, 50), "p90": percentile(lags, 90),
            "p99": p99, "max": lags[-1],
            "share_over_120s": round(sum(x > 120 for x in lags) / len(lags), 4),
            "share_over_300s": round(sum(x > 300 for x in lags) / len(lags), 4),
            "negative": sum(x < 0 for x in lags),  # event time AFTER the feed header: clock skew, worth knowing
        }
        out["suggested_watermark_seconds"] = max(0, p99) + WATERMARK_SAFETY_SECONDS
    return out


def render(s: dict) -> str:
    lines = [f"records read                 : {s['records']}",
             f"without usable event time    : {s['records_without_event_time']}",
             f"empty trip_id / start_date   : {s['empty_trip_id']} / {s['empty_start_date']}",
             f"with a delay value           : {s['with_delay_value']}",
             f"distinct route_ids           : {s['distinct_routes']}   e.g. {s['route_id_examples']}"]
    if s["lag_seconds"]:
        lg = s["lag_seconds"]
        lines += [f"event-time lag (feed_ts - event_time), seconds:",
                  f"    min {lg['min']}  p50 {lg['p50']}  p90 {lg['p90']}  p99 {lg['p99']}  max {lg['max']}",
                  f"    share > 120s: {lg['share_over_120s']:.1%}   share > 300s: {lg['share_over_300s']:.1%}   negative: {lg['negative']}",
                  f"suggested WATERMARK_DELAY    : {s['suggested_watermark_seconds']} seconds "
                  f"(= p99 lag + {WATERMARK_SAFETY_SECONDS}s safety; a starting point, not a proof)"]
    else:
        lines.append("no lag could be computed (no records with both timestamps)")
    return "\n".join(lines)


def _consume(topic: str, max_records: int, bootstrap: str) -> list[dict]:  # pragma: no cover - needs a broker
    from confluent_kafka import Consumer

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "lag-report", "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe([topic])
    out, idle = [], 0
    try:
        while len(out) < max_records and idle < 4:
            msgs = c.consume(1000, timeout=3.0)
            if not msgs:
                idle += 1
                continue
            idle = 0
            for m in msgs:
                if m.error() is None:
                    try:
                        out.append(json.loads(m.value()))
                    except ValueError:
                        pass  # unparseable payloads are the raw zone's job; skip here
    finally:
        c.close()
    return out[:max_records]


def main() -> None:  # pragma: no cover
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default=os.getenv("TOPIC_TRIP_UPDATES", "gtfsr.trip_updates"))
    ap.add_argument("--max", type=int, default=20000)
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    a = ap.parse_args()
    print(render(summarize(_consume(a.topic, a.max, a.bootstrap))))


if __name__ == "__main__":
    main()
