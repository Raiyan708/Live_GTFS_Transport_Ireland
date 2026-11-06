"""Pure-pandas equivalents of the Snowflake ANALYTICS views (snowflake/03_views.sql).

Used by the `local` data source so the dashboard runs without a Snowflake account, and unit-tested so the
two implementations stay in step. No Streamlit, no I/O here.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def dedupe_route_windows(df: pd.DataFrame) -> pd.DataFrame:
    """V_ROUTE_DELAY_5M: one row per (window_start, route_id); latest load wins if _loaded_at exists."""
    if df.empty:
        return df
    order = "_loaded_at" if "_loaded_at" in df.columns else "curated_at"
    return (df.sort_values(order).drop_duplicates(["window_start", "route_id"], keep="last")
            .reset_index(drop=True))


def _weighted(df: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    g = df.assign(_w=df["avg_delay_seconds"] * df["observations"]).groupby(by, as_index=False).agg(
        observations=("observations", "sum"), _w=("_w", "sum"),
        max_delay_seconds=("max_delay_seconds", "max"), late_observations=("late_observations", "sum"),
    )
    # Observation-weighted: an average of per-window averages would over-weight sparse windows.
    g["avg_delay_seconds"] = g["_w"] / g["observations"].replace(0, np.nan)
    g["late_ratio"] = g["late_observations"] / g["observations"].replace(0, np.nan)
    return g.drop(columns="_w")


def route_ranking(df: pd.DataFrame, now: pd.Timestamp, hours: int = 24) -> pd.DataFrame:
    """V_ROUTE_RANKING_24H: worst routes by share of late observations."""
    d = dedupe_route_windows(df)
    d = d[d["window_start"] >= now - pd.Timedelta(hours=hours)]
    if d.empty:
        return pd.DataFrame(columns=["route_id", "observations", "avg_delay_seconds", "max_delay_seconds",
                                     "late_ratio", "reliability_rank"])
    g = _weighted(d, ["route_id"])
    g["reliability_rank"] = g["late_ratio"].rank(method="min", ascending=False).astype(int)
    return g.sort_values(["late_ratio", "route_id"], ascending=[False, True]).reset_index(drop=True)


def hourly_reliability(df: pd.DataFrame) -> pd.DataFrame:
    """V_ROUTE_RELIABILITY_HOURLY: route x local date x local hour."""
    d = dedupe_route_windows(df)
    if d.empty:
        return pd.DataFrame(columns=["route_id", "local_date", "local_hour", "observations",
                                     "avg_delay_seconds", "max_delay_seconds", "late_ratio"])
    return _weighted(d, ["route_id", "local_date", "local_hour"])


def heatmap_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Route x local-hour grid of late_ratio, pooled across days (what the heatmap draws)."""
    d = dedupe_route_windows(df)
    if d.empty:
        return pd.DataFrame(columns=["route_id", "local_hour", "late_ratio", "observations"])
    return _weighted(d, ["route_id", "local_hour"])[["route_id", "local_hour", "late_ratio", "observations"]]


def alert_episodes(alerts: pd.DataFrame) -> pd.DataFrame:
    """V_ALERT_EPISODES: collapse STARTED/ESCALATED/RECOVERED/EXPIRED rows to one row per trip episode."""
    cols = ["trip_key", "episode", "route_id", "started_at", "ended_at", "ended_by", "escalations",
            "max_tier", "peak_delay_minutes", "duration_minutes"]
    if alerts.empty:
        return pd.DataFrame(columns=cols)
    a = alerts.drop_duplicates("alert_id")
    rows = []
    for (trip_key, episode), g in a.groupby(["trip_key", "episode"]):
        started = g.loc[g["alert_type"] == "DELAY_STARTED", "alert_ts"]
        ended = g.loc[g["alert_type"].isin(["DELAY_RECOVERED", "DELAY_EXPIRED"])]
        raised = g.loc[g["alert_type"].isin(["DELAY_STARTED", "DELAY_ESCALATED"]), "delay_minutes"]
        ended_by = None
        if (g["alert_type"] == "DELAY_RECOVERED").any():
            ended_by = "RECOVERED"
        elif (g["alert_type"] == "DELAY_EXPIRED").any():
            ended_by = "EXPIRED"
        s = started.min() if len(started) else pd.NaT
        e = ended["alert_ts"].max() if len(ended) else pd.NaT
        rows.append({
            "trip_key": trip_key, "episode": episode, "route_id": g["route_id"].iloc[0],
            "started_at": s, "ended_at": e, "ended_by": ended_by,
            "escalations": int((g["alert_type"] == "DELAY_ESCALATED").sum()),
            "max_tier": int(g["tier"].max()),
            "peak_delay_minutes": float(raised.max()) if len(raised) else np.nan,
            # Snowflake DATEDIFF('minute') counts minute-boundaries crossed, so mirror that: truncate first.
            "duration_minutes": (int((e.floor("min") - s.floor("min")).total_seconds() // 60)
                                 if pd.notna(s) and pd.notna(e) else np.nan),
        })
    return pd.DataFrame(rows, columns=cols).sort_values("started_at", ascending=False).reset_index(drop=True)


def freshness(df: pd.DataFrame, now: pd.Timestamp) -> dict:
    """V_PIPELINE_FRESHNESS."""
    if df.empty:
        return {"latest_window_end": None, "minutes_behind": None}
    latest = df["window_end"].max()
    return {"latest_window_end": latest, "minutes_behind": int((now - latest).total_seconds() // 60)}


SEVERITY_BY_TIER = {0: ("◆", "MODERATE"), 1: ("▲", "SEVERE"), 2: ("●", "CRITICAL")}


def severity_label(tier: int) -> str:
    """Status is conveyed by SHAPE + WORD, never colour alone (status colours are also reserved)."""
    icon, word = SEVERITY_BY_TIER.get(min(int(tier), 2), ("◆", "MODERATE"))
    return f"{icon} {word}"


def active_alerts_table(items: list[dict], now_epoch: int) -> pd.DataFrame:
    """Shape DynamoDB active_alerts items for display, worst first, with data age."""
    cols = ["severity", "route", "trip", "delay_min", "since_update_min"]
    if not items:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(items)
    df = df.assign(
        severity=df["tier"].astype(int).map(severity_label),
        route=df["route_id"], trip=df["trip_id"],
        delay_min=(df["delay_seconds"].astype(float) / 60).round(1),
        since_update_min=((now_epoch - df["event_time"].astype(float)) / 60).round(0).astype(int),
    )
    return df.sort_values("delay_min", ascending=False)[cols].reset_index(drop=True)
