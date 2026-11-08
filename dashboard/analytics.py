"""Route-reliability analytics for the control room (cold path: S3/Snowflake serving data).

Pure pandas over the `route_delay_5m` table; reuses views.py (the Python twins of the Snowflake views) so the numbers
match the warehouse. Two rules keep it honest:
  * A route with few observations is NOT ranked (3 readings of '100% late' is noise, not a finding): `min_obs`.
  * A route needs several TRIPS behind it (`min_trips`, judged by its busiest 5-minute window). Found on the live feed:
    a 2-trip route where one 'ghost' trip (scheduled but never ran: its delay grows 1:1 with the clock) made the whole
    route '100% late' and rank 1. Late share is only meaningful when more than one or two trips support it.
  * A heatmap cell with few observations is masked, not drawn as a confident colour: `min_cell_obs`.
Also reported: how much data there actually is, so a screen built on 3 hours of data does not look like a week.
"""
from __future__ import annotations

from typing import Any

import pandas as pd

from . import views

HOURS = list(range(24))


def _utc(ts: Any) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    out = views.dedupe_route_windows(df)
    for c in ("window_start", "window_end"):
        out[c] = pd.to_datetime(out[c], utc=True)
    return out


def reliability(
    windows: pd.DataFrame,
    now: Any,
    hours: int = 24,
    min_obs: int = 100,
    min_cell_obs: int = 20,
    min_trips: int = 3,
    limit: int = 15,
    heat_routes: int = 12,
) -> dict[str, Any]:
    if windows is None or windows.empty:
        return {"has_data": False, "coverage": None, "ranking": [], "heatmap": {"hours": HOURS, "routes": []}}
    now = _utc(now)
    d = _clean(windows)
    d = d[d["window_start"] >= now - pd.Timedelta(hours=hours)]
    if d.empty:
        return {"has_data": False, "coverage": None, "ranking": [], "heatmap": {"hours": HOURS, "routes": []}}

    # ---- ranking: worst by share of late observations, among routes with enough evidence
    ranked = views.route_ranking(d, now, hours)
    trips_peak = d.groupby("route_id")["trips_observed"].max()  # busiest window, NOT a sum: windows overlap the same trips
    ranked = ranked.assign(trips_peak=ranked["route_id"].map(trips_peak).fillna(0).astype(int))
    eligible = ranked[(ranked["observations"] >= min_obs) & (ranked["trips_peak"] >= min_trips)].copy()
    eligible = eligible.sort_values(["late_ratio", "observations", "route_id"], ascending=[False, False, True]).reset_index(drop=True)
    eligible["reliability_rank"] = range(1, len(eligible) + 1)
    top = eligible.head(limit)
    ranking = [
        {"route_id": r.route_id, "rank": int(r.reliability_rank), "late_ratio": round(float(r.late_ratio), 4),
         "avg_delay_s": int(round(r.avg_delay_seconds)), "max_delay_s": int(r.max_delay_seconds), "observations": int(r.observations),
         "trips_peak": int(r.trips_peak)}
        for r in top.itertuples()
    ]

    # ---- heatmap: the worst `heat_routes` routes x local hour of day, thin cells masked
    cells = views.heatmap_frame(d)
    heat = []
    for rid in list(top["route_id"])[:heat_routes]:
        rc = cells[cells["route_id"] == rid].set_index("local_hour")
        row = []
        for h in HOURS:
            if h in rc.index and rc.loc[h, "observations"] > 0:
                obs = int(rc.loc[h, "observations"])
                row.append({"hour": h, "observations": obs,
                            "late_ratio": round(float(rc.loc[h, "late_ratio"]), 4) if obs >= min_cell_obs else None})
        heat.append({"route_id": rid, "cells": row})

    hours_with_data = sorted({int(h) for h in cells["local_hour"]})
    coverage = {
        "first_window": d["window_start"].min().isoformat(), "last_window": d["window_end"].max().isoformat(),
        "windows": int(d["window_start"].nunique()), "hours_with_data": hours_with_data,
        "routes_seen": int(d["route_id"].nunique()), "routes_ranked": int(len(eligible)),
        "observations": int(d["observations"].sum()), "min_obs": min_obs, "min_trips": min_trips, "min_cell_obs": min_cell_obs,
        "span_hours": round((d["window_end"].max() - d["window_start"].min()).total_seconds() / 3600, 1),
    }
    return {"has_data": True, "coverage": coverage, "ranking": ranking, "heatmap": {"hours": HOURS, "routes": heat}}
