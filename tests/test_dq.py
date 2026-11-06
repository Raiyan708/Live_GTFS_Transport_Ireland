"""Every DQ check is proven against SQLite twice: clean data passes, a targeted corruption fails."""
import sqlite3

import pytest

from gtfsrt.warehouse import dq

D = "2026-11-06"


@pytest.fixture
def db():
    c = sqlite3.connect(":memory:")
    c.execute("""CREATE TABLE route_delay (window_start TEXT, route_id TEXT, observations INT, late_ratio REAL,
                 max_delay_seconds INT, window_date TEXT)""")
    c.execute("""CREATE TABLE alerts (alert_id TEXT, alert_type TEXT, trip_key TEXT, delay_seconds INT,
                 threshold_seconds INT, alert_date TEXT)""")
    c.executemany("INSERT INTO route_delay VALUES (?,?,?,?,?,?)", [
        ("2026-11-06 16:40:00", "46A", 10, 0.1, 400, D), ("2026-11-06 16:40:00", "15", 8, 0.0, 30, D),
        ("2026-11-06 16:45:00", "46A", 12, 0.5, 900, D)])
    c.executemany("INSERT INTO alerts VALUES (?,?,?,?,?,?)", [
        ("a1", "DELAY_STARTED", "k1", 360, 300, D), ("a2", "DELAY_ESCALATED", "k1", 650, 600, D),
        ("a3", "DELAY_RECOVERED", "k1", 60, 300, D)])
    return c


TABLES = {"route_delay": "route_delay", "alerts": "alerts"}


def results(db):
    return {r.name: r for r in dq.run_checks(db.cursor(), TABLES, D)}


def test_clean_data_passes_every_check(db):
    res = results(db)
    assert len(res) == len(dq.ALL_CHECKS)
    assert all(r.passed for r in res.values()), [r for r in res.values() if not r.passed]
    assert dq.failures(list(res.values())) == []


@pytest.mark.parametrize("name,corrupt", [
    ("route_delay.not_empty", "DELETE FROM route_delay"),
    ("route_delay.unique_key", "INSERT INTO route_delay VALUES ('2026-11-06 16:40:00','46A',10,0.1,400,'2026-11-06')"),
    ("route_delay.no_null_keys", "UPDATE route_delay SET route_id = NULL WHERE route_id = '15'"),
    ("route_delay.late_ratio_in_range", "UPDATE route_delay SET late_ratio = 1.5 WHERE route_id = '15'"),
    ("route_delay.positive_observations", "UPDATE route_delay SET observations = 0 WHERE route_id = '15'"),
    ("alerts.unique_alert_id", "INSERT INTO alerts VALUES ('a1','DELAY_STARTED','k1',360,300,'2026-11-06')"),
    ("alerts.no_null_keys", "UPDATE alerts SET trip_key = NULL WHERE alert_id = 'a1'"),
    ("alerts.valid_type", "UPDATE alerts SET alert_type = 'MYSTERY' WHERE alert_id = 'a1'"),
    ("alerts.started_has_threshold", "UPDATE alerts SET delay_seconds = 100 WHERE alert_id = 'a1'"),
])
def test_each_check_fails_on_its_own_corruption(db, name, corrupt):
    db.execute(corrupt)
    res = results(db)
    assert not res[name].passed, f"{name} did not detect: {corrupt}"
    # and the corruption is attributed to THIS check family, not everything failing at once
    assert sum(1 for r in res.values() if not r.passed) <= 3


def test_warn_severity_does_not_block(db):
    db.execute("UPDATE route_delay SET max_delay_seconds = 999999 WHERE route_id = '15'")
    res = results(db)
    assert not res["route_delay.plausible_max_delay"].passed
    assert dq.failures(list(res.values())) == []  # reported, not blocking


def test_checks_are_scoped_to_the_requested_date(db):
    db.execute("INSERT INTO route_delay VALUES ('2026-11-05 10:00:00','X',0,2.0,1,'2026-11-05')")  # bad row, other day
    assert dq.failures(dq.run_checks(db.cursor(), TABLES, D)) == []


def test_reconcile_and_freshness():
    assert dq.reconcile(30, 30, "route_delay").passed
    r = dq.reconcile(30, 28, "route_delay")
    assert not r.passed and r.violations == 2
    assert dq.freshness(10, 30).passed
    assert not dq.freshness(95, 30).passed and dq.freshness(95, 30).violations == 65
    assert not dq.freshness(None, 30).passed


def test_date_injection_is_rejected(db):
    with pytest.raises(ValueError):
        dq.run_checks(db.cursor(), TABLES, "2026-11-06'; DROP TABLE alerts;--")
