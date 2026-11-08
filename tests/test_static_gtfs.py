import io
import zipfile

import pytest

from gtfsrt import static_gtfs

ROUTES = (
    "route_id,agency_id,route_short_name,route_long_name,route_type\n"
    "1 120 c a,1,120,Dublin - Edenderry,3\n"
    "10000 RED g a,10000,Red,The Point - Tallaght,0\n"
    "DUB-CORK-O,IR,InterCity,Dublin - Cork,2\n"
    ",1,X,row without an id,3\n"
    "1 BLANK a,1,,,3\n"
)
AGENCY = "agency_id,agency_name\n1,Bus Átha Cliath – Dublin Bus\n10000,LUAS\nIR,Iarnród Éireann / Irish Rail\n"
FEED = "feed_publisher_name,feed_version,feed_start_date,feed_end_date\nNTA,V1,20261101,20270131\n"


def make_zip(path, members=None, bom=False):
    members = members or {"routes.txt": ROUTES, "agency.txt": AGENCY, "feed_info.txt": FEED}
    with zipfile.ZipFile(path, "w") as z:
        for name, text in members.items():
            z.writestr(name, ("﻿" if bom else "") + text)
    return path


def test_lookup_joins_agency_and_maps_mode(tmp_path):
    r = static_gtfs.load(make_zip(tmp_path / "g.zip"))["routes"]
    assert r["1 120 c a"] == {"short": "120", "long": "Dublin - Edenderry", "agency": "Bus Átha Cliath – Dublin Bus", "mode": "bus"}
    assert r["10000 RED g a"]["mode"] == "tram" and r["10000 RED g a"]["agency"] == "LUAS"
    assert r["DUB-CORK-O"]["mode"] == "rail"


def test_rows_without_an_id_are_skipped_and_blank_names_become_none(tmp_path):
    r = static_gtfs.load(make_zip(tmp_path / "g.zip"))["routes"]
    assert len(r) == 4 and "" not in r
    assert r["1 BLANK a"]["short"] is None and r["1 BLANK a"]["long"] is None


def test_unknown_route_type_and_unknown_agency_degrade_gracefully():
    out = static_gtfs.parse_routes([{"route_id": "Z", "agency_id": "nope", "route_type": "99"}], [])
    assert out["Z"]["mode"] == "other" and out["Z"]["agency"] is None


def test_feed_validity_window_is_exposed_so_staleness_can_be_shown(tmp_path):
    d = static_gtfs.load(make_zip(tmp_path / "g.zip"))
    assert d["feed"] == {"version": "V1", "start": "20261101", "end": "20270131"} and "CC BY 4.0" in d["attribution"]


def test_utf8_bom_in_the_header_does_not_break_the_first_column(tmp_path):
    assert "1 120 c a" in static_gtfs.load(make_zip(tmp_path / "g.zip", bom=True))["routes"]


def test_missing_feed_info_is_fine_but_missing_routes_is_loud(tmp_path):
    ok = static_gtfs.load(make_zip(tmp_path / "a.zip", {"routes.txt": ROUTES, "agency.txt": AGENCY}))
    assert ok["feed"]["version"] is None
    with pytest.raises(KeyError):
        static_gtfs.load(make_zip(tmp_path / "b.zip", {"agency.txt": AGENCY}))


def test_oversized_member_is_refused_not_parsed(tmp_path):
    big = "route_id\n" + ("x\n" * (static_gtfs.MAX_MEMBER_BYTES // 2 + 10))
    p = make_zip(tmp_path / "g.zip", {"routes.txt": big, "agency.txt": AGENCY})
    with pytest.raises(ValueError, match="refusing to parse"):
        static_gtfs.load(p)


def test_real_file_if_present_covers_every_live_route_shape():
    """Runs only on a machine that downloaded the feed. Guards the parser against the REAL file, not just my fixtures."""
    from pathlib import Path

    p = Path(__file__).resolve().parents[1] / "data" / "gtfs_static" / "GTFS_Realtime.zip"
    if not p.exists():
        pytest.skip("GTFS_Realtime.zip not downloaded")
    r = static_gtfs.load(p)["routes"]
    assert len(r) > 300 and all(v["short"] and v["mode"] != "other" for v in r.values())
    assert r["10000 RED g a"]["agency"] == "LUAS"
