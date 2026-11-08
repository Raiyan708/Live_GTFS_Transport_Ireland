"""Route names from the GTFS *static* feed, so the dashboard can show "120 · Dublin - Edenderry" instead of "03C 120 e a".

    PYTHONPATH=src python -m gtfsrt.static_gtfs [--zip data/gtfs_static/GTFS_Realtime.zip] [--out data/gtfs_static/routes.json]

Why GTFS_Realtime.zip: it is the schedule for exactly the operators that appear in the realtime feed. Checked on the live
feed (2026-11-08): all 224 route_ids seen in the vehicle feed matched a static route_id EXACTLY, so this is a plain lookup.
The zip is untrusted input from the internet: only three small members are read (never extracted to disk), each size-capped.

Source: Transport for Ireland / National Transport Authority, licensed CC BY 4.0 (attribution is shown in the UI).
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import zipfile
from pathlib import Path
from typing import Any

MAX_MEMBER_BYTES = 5 * 1024 * 1024  # routes.txt is ~20 KB; anything near this size is not the file we expect
MODES = {"0": "tram", "1": "metro", "2": "rail", "3": "bus", "4": "ferry"}
ATTRIBUTION = "Route data: Transport for Ireland / National Transport Authority (CC BY 4.0)"


def _read_csv(z: zipfile.ZipFile, name: str) -> list[dict[str, str]]:
    info = z.getinfo(name)  # KeyError if the member is missing: a different feed layout should fail loudly
    if info.file_size > MAX_MEMBER_BYTES:
        raise ValueError(f"{name} is {info.file_size:,} bytes: refusing to parse (cap {MAX_MEMBER_BYTES:,})")
    with z.open(info) as f:
        return list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig", newline="")))


def parse_routes(routes: list[dict[str, str]], agencies: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    """route_id -> {short, long, agency, mode}. Rows without a route_id are skipped; blanks become None."""
    agency_name = {a.get("agency_id", ""): (a.get("agency_name") or "").strip() for a in agencies}
    out: dict[str, dict[str, Any]] = {}
    for r in routes:
        rid = (r.get("route_id") or "").strip()
        if not rid:
            continue
        out[rid] = {
            "short": (r.get("route_short_name") or "").strip() or None,
            "long": (r.get("route_long_name") or "").strip() or None,
            "agency": agency_name.get((r.get("agency_id") or "").strip()) or None,
            "mode": MODES.get((r.get("route_type") or "").strip(), "other"),
        }
    return out


def load(zip_path: str | Path) -> dict[str, Any]:
    with zipfile.ZipFile(zip_path) as z:
        routes = parse_routes(_read_csv(z, "routes.txt"), _read_csv(z, "agency.txt"))
        feed = _read_csv(z, "feed_info.txt") if "feed_info.txt" in z.namelist() else []
    info = feed[0] if feed else {}
    return {
        "attribution": ATTRIBUTION,
        "feed": {"version": info.get("feed_version"), "start": info.get("feed_start_date"), "end": info.get("feed_end_date")},
        "routes": routes,
    }


def main() -> None:  # pragma: no cover - thin CLI
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", default="data/gtfs_static/GTFS_Realtime.zip")
    ap.add_argument("--out", default="data/gtfs_static/routes.json")
    a = ap.parse_args()
    data = load(a.zip)
    Path(a.out).write_text(json.dumps(data, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    print(f"wrote {len(data['routes'])} routes to {a.out} (feed {data['feed']})")


if __name__ == "__main__":
    main()
