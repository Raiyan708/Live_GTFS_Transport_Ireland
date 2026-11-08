"""Static checks on the Snowflake setup that a real account would otherwise reveal as a failed first run."""
import pathlib
import re

SQL = (pathlib.Path(__file__).resolve().parents[1] / "snowflake" / "00_setup.sql").read_text()


def grants(role: str) -> set[tuple[str, str]]:
    out = set()
    for m in re.finditer(r"GRANT\s+(.+?)\s+ON\s+(?:ALL\s+VIEWS\s+IN\s+|FUTURE\s+VIEWS\s+IN\s+)?(SCHEMA|DATABASE|WAREHOUSE)\s+(\S+)\s+TO\s+ROLE\s+" + role, SQL, re.I):
        out.add((m.group(2).upper() + " " + m.group(3).upper().rstrip(";"), m.group(1).upper()))
    return out


def test_loader_can_use_the_analytics_schema_before_creating_views_in_it():
    g = grants("GTFSR_LOADER")
    assert ("SCHEMA GTFSR.ANALYTICS", "USAGE") in g, "CREATE VIEW alone is not enough: the schema needs USAGE too"
    assert ("SCHEMA GTFSR.ANALYTICS", "CREATE VIEW") in g


def test_loader_has_what_it_needs_in_raw_and_a_warehouse():
    g = grants("GTFSR_LOADER")
    assert ("SCHEMA GTFSR.RAW", "ALL") in g and ("WAREHOUSE GTFSR_LOAD_WH", "USAGE") in g and ("DATABASE GTFSR", "USAGE") in g


def test_analysts_can_read_the_views_but_get_no_write_privileges():
    g = grants("GTFSR_ANALYST")
    assert ("SCHEMA GTFSR.ANALYTICS", "USAGE") in g
    assert not any(priv in ("ALL", "INSERT", "CREATE VIEW") for _, priv in g)
