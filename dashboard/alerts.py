"""Shape the hot-path alerts (DynamoDB `active_alerts`) for the control room.

An alert is raised by the stateful engine (debounced, with hysteresis), so it is more stable than the raw 'delayed now'
list. Two facts the UI must not blur:
  * `changed_min` is when the ALERT last changed state (started / escalated), NOT when the trip last reported.
    (Reading it as 'last update' made a healthy long-running alert look stale.)
  * An alert is per TRIP. A trip that has not started yet can already be predicted late, so many alerts have no vehicle
    on the map: those say so instead of pretending to have a position.
"""
from __future__ import annotations

from typing import Any

from gtfsrt.config import AlertConfig

from .views import severity_label

OFF_SECONDS = AlertConfig().off_seconds  # below this an alert would have been closed by the engine


def build_active_alerts(
    items: list[dict[str, Any]],
    positions: dict[str, dict[str, Any]],
    now_epoch: float,
    current_delays: dict[str, int] | None = None,
    stalled: set[str] | None = None,
    upcoming: set[str] | None = None,
) -> dict[str, Any]:
    """`current_delays` overlays the LIVE delay. The delay stored on an alert row is the delay when the alert last changed;
    a top-tier alert gets no more events, so a bus getting later and later would keep showing its old number."""
    current_delays = current_delays or {}
    stalled, upcoming = stalled or set(), upcoming or set()
    out = []
    for it in items:
        try:
            tier, delay = int(it["tier"]), int(float(it["delay_seconds"]))
            changed = int(it["event_time"])
            trip_key = str(it["trip_key"])
        except (KeyError, TypeError, ValueError):
            continue  # one malformed row must not blank the panel
        out.append({
            "key": trip_key, "trip_id": it.get("trip_id") or None,
            "route_id": None if it.get("route_id") in (None, "", "unknown") else it["route_id"],
            "tier": tier, "severity": severity_label(tier), "episode": int(it.get("episode") or 0),
            "delay_s": int(current_delays.get(trip_key, delay)), "delay_at_alert_s": delay,
            "delay_is_current": trip_key in current_delays,
            "stalled": trip_key in stalled,    # delay growing 1:1 with the clock: not progressing (stuck / never started)
            "upcoming": trip_key in upcoming,  # scheduled start still in the future: the delay is a prediction
            "changed_at": changed, "changed_min": max(0, round((now_epoch - changed) / 60)),
            "vehicle": positions.get(trip_key),
        })
    out.sort(key=lambda a: (-a["delay_s"], a["key"]))
    by_tier = {t: sum(a["tier"] == t for a in out) for t in (0, 1, 2)}
    # Reconcile with the live feed: an ACTIVE alert whose trip left the feed, or whose delay is back under the 'off' threshold,
    # should already have been closed. A healthy pipeline leaves ~0.4% of these (measured: 2 of 463); a stopped one leaves
    # hundreds. Only judged when we actually hold live delays: an empty table means 'feed not loaded', not 'all wrong'.
    unconfirmed = 0
    if current_delays:
        unconfirmed = sum(1 for a in out if not a["delay_is_current"] or a["delay_s"] < OFF_SECONDS)
    return {"summary": {"active": len(out), "by_tier": by_tier, "with_vehicle": sum(a["vehicle"] is not None for a in out),
                        "unconfirmed": unconfirmed, "stalled": sum(a["stalled"] for a in out)},
            "items": out}
