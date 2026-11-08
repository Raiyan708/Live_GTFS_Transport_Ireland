"""Streamlit dashboard.   streamlit run dashboard/app.py

Two sections with deliberately different freshness contracts:
  RIGHT NOW  (hot path)  DynamoDB active alerts, refreshed every 5 s
  HISTORY    (cold path) Snowflake / serving zone, cached 60 s, 5-minute window granularity

Visual rules (dataviz skill): form chosen by job; ONE hue for nominal bars; sequential single-hue ramp for the
heatmap (anchor flips in dark); thin marks; hairline solid grid; direct label only on the extreme; status by
shape+word never colour alone; a table-view twin for every chart.
"""
from __future__ import annotations

import time

import altair as alt
import pandas as pd
import streamlit as st

from dashboard import sources, views

st.set_page_config(page_title="Irish transport delays", page_icon="🚌", layout="wide")

# ---- palette: validated with scripts/validate_palette.js (slot 1 + sequential blue ramp) ----------------
PALETTE = {
    "light": dict(surface="#fcfcfb", ink="#0b0b0b", ink2="#52514e", muted="#898781", grid="#e1e0d9",
                  axis="#c3c2b7", series1="#2a78d6"),
    "dark": dict(surface="#1a1a19", ink="#f0efec", ink2="#c3c2b7", muted="#898781", grid="#2c2c2a",
                 axis="#383835", series1="#3987e5"),
}
BLUE_RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]  # steps 100..700


def mode() -> str:
    try:
        return "dark" if st.context.theme.type == "dark" else "light"
    except Exception:
        return "light"


C = PALETTE[mode()]
RAMP = BLUE_RAMP[::-1] if mode() == "dark" else BLUE_RAMP  # dark: low values recede into the surface


def style(chart: alt.Chart) -> alt.Chart:
    return (chart.configure(background="transparent")
            .configure_view(stroke=None)
            .configure_axis(gridColor=C["grid"], gridWidth=1, domainColor=C["axis"], tickColor=C["axis"],
                            labelColor=C["muted"], titleColor=C["ink2"], labelFontSize=12, titleFontSize=12,
                            titleFontWeight="normal")
            .configure_legend(labelColor=C["ink2"], titleColor=C["ink2"], titleFontWeight="normal"))


# ---- cached loaders (cold path) -------------------------------------------------------------------------
@st.cache_data(ttl=60, show_spinner=False)
def load_cold() -> tuple[pd.DataFrame, pd.DataFrame]:
    return sources.route_windows(), sources.alerts()


# ---- header + the single filter row ------------------------------------------------------------------------
st.title("Irish public transport · delay monitor")
st.caption("Live GTFS-Realtime feed → Kafka → Spark → (hot) DynamoDB alerts · (cold) S3 → Glue → Snowflake")

windows, alerts_df = load_cold()
all_routes = sorted(windows["route_id"].unique()) if not windows.empty else []

f1, f2, _ = st.columns([1, 2, 3])
lookback = f1.selectbox("Lookback", [6, 24, 72, 168], index=1, format_func=lambda h: f"{h} h" if h < 72 else f"{h // 24} days")
picked = f2.multiselect("Routes", all_routes, placeholder="All routes")

now = pd.Timestamp.now(tz="UTC").tz_localize(None)
scope = windows[windows["route_id"].isin(picked)] if picked else windows


# ---- RIGHT NOW (hot path) -----------------------------------------------------------------------------------
@st.fragment(run_every="5s")
def right_now():
    st.subheader("Right now")
    st.caption("Hot path · refreshes every 5 s · one row per trip currently running late")
    try:
        items = sources.active_alerts()
    except Exception as exc:  # DynamoDB down must not blank the whole page
        st.error(f"⚠ Active alerts unavailable: {type(exc).__name__}")
        return
    if picked:
        items = [i for i in items if i.get("route_id") in picked]
    table = views.active_alerts_table(items, int(time.time()))

    k1, k2, k3 = st.columns(3)
    k1.metric("Trips running late", len(table))
    k2.metric("Longest delay", f"{table['delay_min'].max():.0f} min" if len(table) else "–")
    k3.metric("Critical (≥ 15 min)", int((table["severity"].str.contains("CRITICAL")).sum()) if len(table) else 0)

    if table.empty:
        st.success("✓ No trips are currently flagged as late.")
    else:
        st.dataframe(table, hide_index=True, width="stretch", column_config={
            "severity": "Severity", "route": "Route", "trip": "Trip",
            "delay_min": st.column_config.NumberColumn("Delay (min)", format="%.1f"),
            "since_change_min": st.column_config.NumberColumn("Alert changed (min ago)", format="%d",
                                                               help="Minutes since this alert last started or escalated. It does not mean the trip stopped reporting."),
        })


right_now()

# ---- HISTORY (cold path) ------------------------------------------------------------------------------------
st.divider()
st.subheader("History")
fresh = views.freshness(windows, now)
if fresh["minutes_behind"] is None:
    st.info("No curated data yet - start the streaming jobs and the Glue job.")
    st.stop()
behind = fresh["minutes_behind"]
st.caption(f"Cold path · latest 5-minute window ended {fresh['latest_window_end']:%Y-%m-%d %H:%M} UTC "
           f"({behind} min ago) · source: {sources.DATA_SOURCE}")
if behind > 45:
    st.warning(f"⚠ Warehouse data is {behind} min behind - the pipeline may be stalled.")

rank = views.route_ranking(scope, now, hours=lookback)
if rank.empty:
    st.info("No data in this lookback window.")
    st.stop()

t1, t2, t3, t4 = st.columns(4)
t1.metric("Routes reporting", len(rank))
worst = rank.iloc[0]
# No `delta`: st.metric would draw an up-arrow, which reads as "rose by X" - this is a level, not a change.
t2.metric("Least reliable route", f"{worst['route_id']} · {worst['late_ratio']:.0%}", help="Share of its delay readings that were 5+ minutes late")
t3.metric("Fleet late share", f"{(rank['late_ratio'] * rank['observations']).sum() / rank['observations'].sum():.1%}")
t4.metric("Observations", f"{int(rank['observations'].sum()):,}")

# -- ranked bars: nominal categories => ONE hue, bar length carries the value ------------------------------
st.markdown("#### Least reliable routes")
st.caption("Share of delay readings at or above 5 minutes late · observation-weighted")
top = rank.head(10).copy()
top["share"] = top["late_ratio"]
top["avg_min"] = (top["avg_delay_seconds"] / 60).round(1)
top["max_min"] = (top["max_delay_seconds"] / 60).round(1)
order = list(top["route_id"])
base = alt.Chart(top).encode(
    y=alt.Y("route_id:N", sort=order, title=None, axis=alt.Axis(domain=False, ticks=False, labelColor=C["ink2"])),
    x=alt.X("share:Q", title="Late share", axis=alt.Axis(format="%", grid=True, tickCount=5), scale=alt.Scale(domain=[0, max(0.05, float(top["share"].max()) * 1.15)])),
    tooltip=[alt.Tooltip("route_id:N", title="Route"), alt.Tooltip("share:Q", title="Late share", format=".1%"),
             alt.Tooltip("avg_min:Q", title="Avg delay (min)"), alt.Tooltip("max_min:Q", title="Max delay (min)"),
             alt.Tooltip("observations:Q", title="Readings", format=",")],
)
bars = base.mark_bar(size=14, cornerRadiusEnd=4, color=C["series1"])
label = (base.transform_filter(alt.datum.route_id == order[0]).mark_text(align="left", dx=6, color=C["ink"], fontSize=12)
         .encode(text=alt.Text("share:Q", format=".0%")))  # direct label on the extreme only
st.altair_chart(style(bars + label).properties(height=28 * len(top) + 40), width="stretch")
with st.expander("Table view"):
    st.dataframe(rank.assign(late_ratio=rank["late_ratio"].round(4), avg_delay_seconds=rank["avg_delay_seconds"].round(1)),
                 hide_index=True, width="stretch")

# -- heatmap: route x local hour, sequential single hue ---------------------------------------------------------
st.markdown("#### When are routes late?")
st.caption("Late share by route and local hour (Europe/Dublin) · darker = more late readings" if mode() == "light"
           else "Late share by route and local hour (Europe/Dublin) · lighter = more late readings")
heat = views.heatmap_frame(scope[scope["window_start"] >= now - pd.Timedelta(hours=lookback)])
if heat.empty:
    st.info("No windows in range.")
else:
    heat["late_ratio"] = heat["late_ratio"].fillna(0)
    route_order = [r for r in rank["route_id"] if r in set(heat["route_id"])]
    cells = alt.Chart(heat).mark_rect(stroke=C["surface"], strokeWidth=2, cornerRadius=2).encode(
        # Fixed 0-23 domain: with sparse data a single hour must draw as ONE column, not stretch across the plot
        # (a stretched cell would imply the whole day looks the same).
        x=alt.X("local_hour:O", title="Local hour", scale=alt.Scale(domain=list(range(24))),
                axis=alt.Axis(labelAngle=0, grid=False, labelOverlap=True)),
        y=alt.Y("route_id:N", sort=route_order, title=None, axis=alt.Axis(domain=False, ticks=False, grid=False)),
        color=alt.Color("late_ratio:Q", title="Late share", scale=alt.Scale(domain=[0, max(0.01, float(heat["late_ratio"].max()))], range=RAMP),
                        legend=alt.Legend(format="%", gradientLength=160, orient="right")),
        tooltip=[alt.Tooltip("route_id:N", title="Route"), alt.Tooltip("local_hour:O", title="Hour"),
                 alt.Tooltip("late_ratio:Q", title="Late share", format=".1%"), alt.Tooltip("observations:Q", title="Readings", format=",")],
    )
    st.altair_chart(style(cells).properties(height=26 * len(route_order) + 50), width="stretch")
    with st.expander("Table view"):
        st.dataframe(heat.pivot(index="route_id", columns="local_hour", values="late_ratio").round(3), width="stretch")

# -- episodes ----------------------------------------------------------------------------------------------------------
st.markdown("#### Delay episodes")
st.caption("One row per period a trip ran ≥ 5 min late (collapsed from start/escalate/recover alerts)")
episodes = views.alert_episodes(alerts_df)
if picked and not episodes.empty:
    episodes = episodes[episodes["route_id"].isin(picked)]
if episodes.empty:
    st.info("No delay episodes recorded in this period.")
else:
    shown = episodes.assign(status=episodes["ended_by"].fillna("OPEN").map(
        {"RECOVERED": "✓ recovered", "EXPIRED": "– expired", "OPEN": "● open"}))
    st.dataframe(shown[["route_id", "status", "started_at", "ended_at", "duration_minutes", "peak_delay_minutes", "escalations"]],
                 hide_index=True, width="stretch", column_config={
                     "route_id": "Route", "status": "Status", "started_at": "Started (UTC)", "ended_at": "Ended (UTC)",
                     "duration_minutes": st.column_config.NumberColumn("Duration (min)", format="%d"),
                     "peak_delay_minutes": st.column_config.NumberColumn("Peak delay (min)", format="%.1f"),
                     "escalations": "Escalations"})
