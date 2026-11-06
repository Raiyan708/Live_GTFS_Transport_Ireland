import pathlib

import pytest
import sqlglot
from sqlglot import exp

from gtfsrt.warehouse.loader import ALERTS, ROUTE_DELAY, TABLES, copy_sql, delete_sql, load_partition, validate_date

SQL_DIR = pathlib.Path(__file__).parent.parent / "snowflake"


# --- injection defence ------------------------------------------------------
@pytest.mark.parametrize("bad", ["2026-01-01'; DROP TABLE x;--", "2026-1-1", "", "20260101", "2026-01-01 ", "../../etc"])
def test_validate_date_rejects_anything_but_iso_dates(bad):
    with pytest.raises(ValueError):
        validate_date(bad)


def test_every_sql_builder_validates():
    for fn in (delete_sql, copy_sql):
        with pytest.raises(ValueError):
            fn(ROUTE_DELAY, "x'; --")


# --- statement shape --------------------------------------------------------
def test_copy_targets_match_selected_columns():
    for t in (ROUTE_DELAY, ALERTS):
        sql = copy_sql(t, "2026-11-06")
        target_part = sql.split("(", 1)[1].split(")", 1)[0]
        n_targets = len([c for c in target_part.split(",") if c.strip()])
        n_selects = len(t.columns) + 3  # + partition date, filename, row number
        assert n_targets == len(t.columns) + 3
        select_body = sql.split("(SELECT ", 1)[1].split(" FROM @", 1)[0]
        assert select_body.count("$1:") == len(t.columns)
        assert n_selects == n_targets


def test_copy_reads_partition_from_path_and_is_forced_and_strict():
    sql = copy_sql(ROUTE_DELAY, "2026-11-06")
    assert "window_date=2026-11-06/" in sql and "METADATA$FILENAME" in sql
    assert "FORCE = TRUE" in sql and "ON_ERROR = ABORT_STATEMENT" in sql


def test_columns_line_up_with_ddl():
    ddl = (SQL_DIR / "02_tables.sql").read_text().upper()
    for t in TABLES.values():
        body = ddl.split(f"CREATE TABLE IF NOT EXISTS {t.table} (", 1)[1].split("\n)", 1)[0]
        for col, _ in t.columns:
            assert f"\n  {col} " in "\n" + body, f"{t.table}.{col} missing from DDL"
        assert t.part_col in body


# --- transaction behaviour (fake DB-API connection) --------------------------
class FakeCursor:
    def __init__(self, fail_on=None, count=42):
        self.log, self.fail_on, self.count = [], fail_on, count

    def execute(self, sql, params=None):
        self.log.append(sql.split()[0] + (" " + sql.split()[1] if sql.startswith(("DELETE", "INSERT")) else ""))
        if self.fail_on and sql.startswith(self.fail_on):
            raise RuntimeError("boom")

    def fetchone(self):
        return (self.count,)

    def close(self):
        self.log.append("close")


class FakeConn:
    def __init__(self, **kw):
        self.cur = FakeCursor(**kw)

    def cursor(self):
        return self.cur


def test_load_partition_happy_path_order():
    conn = FakeConn(count=120)
    rows = load_partition(conn, ROUTE_DELAY, "2026-11-06", "run1")
    assert rows == 120
    assert conn.cur.log == ["BEGIN", "DELETE FROM", "COPY", "SELECT", "INSERT INTO", "COMMIT", "close"]


def test_failed_copy_rolls_back_and_never_commits():
    conn = FakeConn(fail_on="COPY")
    with pytest.raises(RuntimeError):
        load_partition(conn, ALERTS, "2026-11-06", "run1")
    assert "ROLLBACK" in conn.cur.log and "COMMIT" not in conn.cur.log
    assert conn.cur.log.index("DELETE FROM") < conn.cur.log.index("ROLLBACK")  # delete was undone, not committed


# --- syntax check of the real SQL files (sqlglot, Snowflake dialect) ---------
def _statements(path):
    text = path.read_text()
    return [s for s in sqlglot.parse(text, read="snowflake") if s is not None]


@pytest.mark.parametrize("name", ["02_tables.sql", "03_views.sql"])
def test_ddl_files_parse_in_snowflake_dialect(name):
    stmts = _statements(SQL_DIR / name)
    assert stmts, name
    # sqlglot only has partial support for Snowflake permission statements (GRANT ... ON ALL VIEWS IN SCHEMA),
    # so those may arrive as raw Commands. Every CREATE TABLE / CREATE VIEW / query must parse fully.
    allowed_raw = {"USE", "GRANT", "DESC"}
    assert not [s for s in stmts if isinstance(s, exp.Command) and s.this.upper() not in allowed_raw], \
        "sqlglot could not fully parse a statement (it fell back to a raw Command)"
    kinds = {type(s).__name__ for s in stmts}
    assert "Create" in kinds, "expected CREATE statements to parse into real Create nodes"


def test_delete_statement_parses():
    stmt = sqlglot.parse_one(delete_sql(ROUTE_DELAY, "2026-11-06"), read="snowflake")
    assert isinstance(stmt, exp.Delete)
