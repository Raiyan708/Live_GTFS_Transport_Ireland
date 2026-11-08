"""Parity: do the Snowflake views compute the same numbers as the pandas twins in views.py?

Used by scripts/snowflake_verify.py. Everything here is pure (no Snowflake connection) so it is unit-tested:
  * export_partitions : write the serving Parquet in the folder layout the COPY statement expects
  * expected_*        : what the numbers SHOULD be, computed with pandas from the same files
  * *_sql             : the queries that fetch the same numbers from Snowflake
  * compare_*         : turn (expected, actual) into named PASS/FAIL checks
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from gtfsrt.warehouse.loader import validate_date

from . import views

PARTITION = {"route_delay_5m": "window_date", "alerts": "alert_date"}


@dataclass(frozen=True)
class Exported:
    table: str
    date: str
    path: Path
    rows: int


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    expected: Any
    actual: Any


def export_partitions(frames: dict[str, pd.DataFrame], out_dir: Path, date: str) -> list[Exported]:
    """serving/<table>/<part>=<date>/part-00000.parquet, WITHOUT the partition column (Spark strips it too: the COPY
    recovers it from the path, so a file that still contained it would not be a faithful copy of the real layout)."""
    validate_date(date)
    out = []
    for table, df in frames.items():
        part = PARTITION[table]
        if df is None or df.empty or part not in df.columns:
            continue
        sub = df[df[part].astype(str) == date].drop(columns=[part])
        for c in sub.columns:
            if str(sub[c].dtype) == "category":
                sub[c] = sub[c].astype(str)
        if sub.empty:
            continue
        folder = Path(out_dir) / table / f"{part}={date}"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / "part-00000.parquet"
        # microseconds, exactly like the real Glue output (TIMESTAMP_MICROS); pandas defaults to nanoseconds, which Snowflake reads badly
        sub.to_parquet(path, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
        out.append(Exported(table, date, path, len(sub)))
    return out


# ------------------------------------------------------------------------------------------------ expected (pandas)
def expected_route(df: pd.DataFrame, date: str, min_obs: int = 100, top: int = 10) -> dict[str, Any]:
    validate_date(date)
    d = views.dedupe_route_windows(df)
    d = d[d["window_date"].astype(str) == date]
    g = d.groupby("route_id", as_index=False).agg(obs=("observations", "sum"), late=("late_observations", "sum"))
    g["ratio"] = g["late"] / g["obs"]
    ranked = g[g["obs"] >= min_obs].sort_values(["ratio", "obs", "route_id"], ascending=[False, False, True]).head(top)
    return {"rows": int(len(d)), "observations": int(d["observations"].sum()), "late": int(d["late_observations"].sum()),
            "routes": int(d["route_id"].nunique()),
            "top": [(r.route_id, float(r.ratio), int(r.obs)) for r in ranked.itertuples()]}


def expected_alerts(df: pd.DataFrame, date: str) -> dict[str, Any]:
    validate_date(date)
    d = df[df["alert_date"].astype(str) == date].drop_duplicates("alert_id")
    return {"rows": int(len(d)), "episodes": int(len(views.alert_episodes(d)))}


# ------------------------------------------------------------------------------------------------ the same numbers from Snowflake
def route_totals_sql(date: str) -> str:
    validate_date(date)
    return ("SELECT COUNT(*), COALESCE(SUM(OBSERVATIONS), 0), COALESCE(SUM(LATE_OBSERVATIONS), 0), COUNT(DISTINCT ROUTE_ID) "
            f"FROM GTFSR.ANALYTICS.V_ROUTE_DELAY_5M WHERE WINDOW_DATE = '{date}'")


def route_top_sql(date: str, min_obs: int = 100, top: int = 10) -> str:
    validate_date(date)
    return ("SELECT ROUTE_ID, SUM(LATE_OBSERVATIONS) / NULLIF(SUM(OBSERVATIONS), 0) AS RATIO, SUM(OBSERVATIONS) AS OBS "
            f"FROM GTFSR.ANALYTICS.V_ROUTE_DELAY_5M WHERE WINDOW_DATE = '{date}' GROUP BY ROUTE_ID "
            f"HAVING SUM(OBSERVATIONS) >= {int(min_obs)} ORDER BY RATIO DESC, OBS DESC, ROUTE_ID LIMIT {int(top)}")


def alert_count_sql(date: str) -> str:
    validate_date(date)
    return ("SELECT COUNT(*) FROM (SELECT ALERT_ID FROM GTFSR.RAW.ALERTS "
            f"WHERE ALERT_DATE = '{date}' QUALIFY ROW_NUMBER() OVER (PARTITION BY ALERT_ID ORDER BY _LOADED_AT DESC) = 1)")


EPISODES_SQL = "SELECT COUNT(*) FROM GTFSR.ANALYTICS.V_ALERT_EPISODES"


# ------------------------------------------------------------------------------------------------ comparison
def _close(a: float, b: float, rel: float = 1e-9) -> bool:
    return math.isclose(float(a), float(b), rel_tol=rel, abs_tol=1e-12)


def compare_route(expected: dict[str, Any], totals: Sequence[Any], top_rows: Sequence[Sequence[Any]]) -> list[Check]:
    rows, obs, late, routes = (int(x) for x in totals)
    checks = [Check("route rows (after de-duplication)", rows == expected["rows"], expected["rows"], rows),
              Check("route observations (sum)", obs == expected["observations"], expected["observations"], obs),
              Check("route late observations (sum)", late == expected["late"], expected["late"], late),
              Check("distinct routes", routes == expected["routes"], expected["routes"], routes)]
    got = [(str(r[0]), float(r[1]), int(r[2])) for r in top_rows]
    exp = expected["top"]
    same = len(got) == len(exp) and all(g[0] == e[0] and _close(g[1], e[1]) and g[2] == e[2] for g, e in zip(got, exp))
    checks.append(Check(f"top-{len(exp)} routes by late share (order, share, readings)", same, [e[0] for e in exp], [g[0] for g in got]))
    return checks


def compare_alerts(expected: dict[str, Any], count: int, episodes: int) -> list[Check]:
    return [Check("alert rows (after de-duplication)", int(count) == expected["rows"], expected["rows"], int(count)),
            Check("alert episodes (V_ALERT_EPISODES)", int(episodes) == expected["episodes"], expected["episodes"], int(episodes))]
