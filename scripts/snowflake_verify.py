#!/usr/bin/env python3
"""Run the REAL Snowflake path end to end with only a Snowflake account (no AWS), and check it against pandas.

    # 1. see exactly what it will do, without connecting (safe, needs nothing):
    PYTHONPATH=src:. S3_ENDPOINT_URL=http://localhost:5555 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
        ./.venv/bin/python scripts/snowflake_verify.py --dry-run

    # 2. the real run (credentials come from YOUR environment; this script never prints or stores them):
    export SNOWFLAKE_ACCOUNT=<orgname-accountname>  SNOWFLAKE_USER=<user>
    export SNOWFLAKE_PRIVATE_KEY_FILE=~/.snowflake/rsa_key.p8       # recommended (see docs/SNOWFLAKE_RUNBOOK.md)
    #   or: export SNOWFLAKE_PASSWORD=...                           # works for a trial account without MFA
    PYTHONPATH=src:. ... ./.venv/bin/python scripts/snowflake_verify.py

What it does: create the roles/warehouses/database/tables/views (snowflake/00..03 + 01_storage_internal) -> export the serving
Parquet from the local lake -> PUT to an INTERNAL stage -> run the production loader's COPY (partition-replace, in a
transaction) -> run the data-quality SQL -> compare the Snowflake VIEWS with the pandas twins on the same data.
STATUS: written and unit-tested (SQL syntax, layout, comparison logic) but, until you run it, NEVER EXECUTED on Snowflake.
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import os
import re
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STAGE = "@GTFSR.RAW.SERVING_STAGE_LOCAL"
SECRET_ENV = ("SNOWFLAKE_PASSWORD", "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
_IDENT = re.compile(r"^[A-Za-z0-9_.@-]+$")


def scrub(text: str) -> str:
    """Remove anything secret-looking from an error message before it is printed."""
    for k in (*SECRET_ENV, "SNOWFLAKE_PRIVATE_KEY_FILE"):
        v = os.environ.get(k)
        if v:
            text = text.replace(v, "***")
    return text


def connect():
    import snowflake.connector

    for k in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER"):
        if not os.environ.get(k):
            raise SystemExit(f"missing environment variable {k} (see docs/SNOWFLAKE_RUNBOOK.md)")
    kw = dict(account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
              role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"), session_parameters={"QUERY_TAG": "gtfsr-verify"})
    key_file = os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE")
    if key_file:
        from cryptography.hazmat.primitives import serialization

        pw = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        key = serialization.load_pem_private_key(Path(key_file).expanduser().read_bytes(), password=pw.encode() if pw else None)
        kw["private_key"] = key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    elif os.environ.get("SNOWFLAKE_PASSWORD"):
        kw["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    else:
        raise SystemExit("set SNOWFLAKE_PRIVATE_KEY_FILE (recommended) or SNOWFLAKE_PASSWORD")
    return snowflake.connector.connect(**kw)


def run_file(conn, name: str) -> int:
    sql = (ROOT / "snowflake" / name).read_text(encoding="utf-8")
    n = 0
    for cur in conn.execute_stream(io.StringIO(sql), remove_comments=True):
        cur.close()
        n += 1
    return n


def one(conn, sql: str):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchone()
    finally:
        cur.close()


def many(conn, sql: str):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall()
    finally:
        cur.close()


def load_local_frames():
    from dashboard import sources

    return {"route_delay_5m": sources.route_windows(), "alerts": sources.alerts()}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="UTC partition date YYYY-MM-DD (default: the newest date in the local serving data)")
    ap.add_argument("--dry-run", action="store_true", help="export + compute expected numbers + show the plan; do NOT connect")
    ap.add_argument("--skip-setup", action="store_true", help="objects already exist (re-run only the load and checks)")
    ap.add_argument("--report", default=str(ROOT / "docs" / "SNOWFLAKE_VERIFY_REPORT.md"))
    a = ap.parse_args()

    from dashboard import parity
    from gtfsrt.warehouse import dq, loader

    frames = load_local_frames()
    rd, al = frames["route_delay_5m"], frames["alerts"]
    if rd.empty:
        raise SystemExit("no local serving data: run the Glue step first (scripts/run_curate_local.sh)")
    date = a.date or max(str(x) for x in rd["window_date"].unique())
    loader.validate_date(date)
    exp_route = parity.expected_route(rd, date)
    exp_alerts = parity.expected_alerts(al, date) if not al.empty else {"rows": 0, "episodes": 0}
    work = Path(tempfile.mkdtemp(prefix="gtfsr-sf-"))
    exported = parity.export_partitions(frames, work, date)
    print(f"date {date} | exported {[(e.table, e.rows) for e in exported]} -> {work}")
    print(f"expected route: {exp_route['rows']} windows, {exp_route['observations']:,} readings, {exp_route['routes']} routes | "
          f"expected alerts: {exp_alerts['rows']} rows, {exp_alerts['episodes']} episodes")
    steps = ["00_setup.sql", "01_storage_internal.sql", "02_tables.sql", "03_views.sql"]
    if a.dry_run:
        print("\nDRY RUN: would execute", ", ".join(steps), "then PUT", len(exported), "file(s) to", STAGE, "and run:")
        for t in (loader.ROUTE_DELAY, loader.ALERTS):
            print("  ", loader.copy_sql(t, date, STAGE)[:160], "...")
        print("  ", parity.route_totals_sql(date)[:140], "...")
        return 0

    checks: list = []
    t0 = time.time()
    conn = connect()
    try:
        if not a.skip_setup:
            run_file(conn, "00_setup.sql")
            user = one(conn, "SELECT CURRENT_USER()")[0]
            if not _IDENT.match(str(user)):
                raise SystemExit("unexpected user name format; refusing to build a GRANT statement")
            conn.cursor().execute(f'GRANT ROLE GTFSR_LOADER TO USER "{user}"').close()
            run_file(conn, "01_storage_internal.sql")
            conn.cursor().execute("USE ROLE GTFSR_LOADER").close()
            conn.cursor().execute("USE WAREHOUSE GTFSR_LOAD_WH").close()
            for f in steps[2:]:
                run_file(conn, f)
            print("setup OK: roles, warehouses, database, tables, views")
        else:
            conn.cursor().execute("USE ROLE GTFSR_LOADER").close()
            conn.cursor().execute("USE WAREHOUSE GTFSR_LOAD_WH").close()

        for e in exported:  # upload
            if not _IDENT.match(e.path.name):
                raise SystemExit(f"unexpected file name {e.path.name!r}")
            part = parity.PARTITION[e.table]
            conn.cursor().execute(
                f"PUT 'file://{e.path}' {STAGE}/{e.table}/{part}={e.date}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE").close()
        print(f"uploaded {len(exported)} file(s) to {STAGE}")

        run_id = f"verify-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}"
        loaded = {}
        for t in (loader.ROUTE_DELAY, loader.ALERTS):
            if any(e.table == t.table.lower() for e in exported):
                loaded[t.table] = loader.load_partition(conn, t, date, run_id, stage=STAGE)
        print("loaded rows:", loaded)
        checks.append(parity.Check("loaded rows == exported rows", all(loaded.get(e.table.upper()) == e.rows for e in exported),
                                   {e.table: e.rows for e in exported}, loaded))

        cur = conn.cursor()
        for r in dq.run_checks(cur, {"route_delay": "GTFSR.RAW.ROUTE_DELAY_5M", "alerts": "GTFSR.RAW.ALERTS"}, date):
            checks.append(parity.Check(f"DQ {r.name} [{r.severity}]", r.passed or r.severity != "error", 0, r.violations))
        cur.close()

        totals = one(conn, parity.route_totals_sql(date))
        top = many(conn, parity.route_top_sql(date))
        checks += parity.compare_route(exp_route, totals, top)
        if exp_alerts["rows"]:
            checks += parity.compare_alerts(exp_alerts, one(conn, parity.alert_count_sql(date))[0], one(conn, parity.EPISODES_SQL)[0])
    finally:
        conn.close()

    ok = all(c.ok for c in checks)
    lines = [f"# Snowflake verification report", "", f"- date verified: `{date}` (UTC partition)", f"- run: `{run_id}`, took {time.time()-t0:.0f}s",
             f"- result: **{'ALL CHECKS PASSED' if ok else 'FAILURES BELOW'}**", "", "| check | expected | actual | |", "|---|---|---|---|"]
    lines += [f"| {c.name} | {c.expected} | {c.actual} | {'PASS' if c.ok else '**FAIL**'} |" for c in checks]
    Path(a.report).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(f"{'PASS' if c.ok else 'FAIL'}  {c.name}  (expected {c.expected}, got {c.actual})" for c in checks))
    print(f"\nreport written to {a.report}")
    return 0 if ok else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception as exc:  # never leak a credential through an error message
        print(f"FAILED: {type(exc).__name__}: {scrub(str(exc))}", file=sys.stderr)
        sys.exit(2)
