"""Data-quality checks as plain ANSI SQL returning a VIOLATION COUNT (0 == pass).

Why plain SQL returning a count?
  * portable: the same text runs on Snowflake in production and on SQLite in unit tests,
    so every check is proven against deliberately bad data (not eyeballed).
  * uniform: the runner needs no per-check logic.

Table names are placeholders ({table}) filled from a fixed allow-list, never from user input.
Severity: "error" fails the DAG (blocks publishing); "warn" is reported but does not block.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .loader import validate_date


@dataclass(frozen=True)
class DQCheck:
    name: str
    table_key: str  # logical table: "route_delay" | "alerts"
    sql: str  # may contain {table} and {date}; must return ONE number: violations
    severity: str = "error"
    description: str = ""


@dataclass(frozen=True)
class DQResult:
    name: str
    severity: str
    violations: int
    passed: bool
    description: str


ROUTE_DELAY_CHECKS = [
    DQCheck("route_delay.not_empty", "route_delay",
            "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM {table} WHERE window_date = '{date}'",
            description="a loaded date must contain rows"),
    DQCheck("route_delay.unique_key", "route_delay",
            "SELECT COUNT(*) - COUNT(DISTINCT window_start || '|' || route_id) FROM {table} WHERE window_date = '{date}'",
            description="(window_start, route_id) must be unique - catches duplicate loads"),
    DQCheck("route_delay.no_null_keys", "route_delay",
            "SELECT COUNT(*) FROM {table} WHERE window_date = '{date}' AND "
            "(window_start IS NULL OR route_id IS NULL OR observations IS NULL)",
            description="key and count columns must be populated"),
    DQCheck("route_delay.late_ratio_in_range", "route_delay",
            "SELECT COUNT(*) FROM {table} WHERE window_date = '{date}' AND (late_ratio < 0 OR late_ratio > 1)",
            description="a ratio outside [0,1] means broken aggregation"),
    DQCheck("route_delay.positive_observations", "route_delay",
            "SELECT COUNT(*) FROM {table} WHERE window_date = '{date}' AND observations <= 0",
            description="a window row with no observations should not exist"),
    DQCheck("route_delay.plausible_max_delay", "route_delay",
            "SELECT COUNT(*) FROM {table} WHERE window_date = '{date}' AND ABS(max_delay_seconds) > 86400",
            severity="warn", description="a >24h delay is almost certainly a feed/clock error (warn, don't block)"),
]

ALERT_CHECKS = [
    DQCheck("alerts.unique_alert_id", "alerts",
            "SELECT COUNT(*) - COUNT(DISTINCT alert_id) FROM {table} WHERE alert_date = '{date}'",
            description="alert_id is the idempotency key - duplicates mean a double load"),
    DQCheck("alerts.no_null_keys", "alerts",
            "SELECT COUNT(*) FROM {table} WHERE alert_date = '{date}' AND "
            "(alert_id IS NULL OR alert_type IS NULL OR trip_key IS NULL)"),
    DQCheck("alerts.valid_type", "alerts",
            "SELECT COUNT(*) FROM {table} WHERE alert_date = '{date}' AND alert_type NOT IN "
            "('DELAY_STARTED','DELAY_ESCALATED','DELAY_RECOVERED','DELAY_EXPIRED')",
            description="unknown alert types mean the producer/consumer contract drifted"),
    DQCheck("alerts.started_has_threshold", "alerts",
            "SELECT COUNT(*) FROM {table} WHERE alert_date = '{date}' AND alert_type = 'DELAY_STARTED' "
            "AND (threshold_seconds IS NULL OR delay_seconds < threshold_seconds)",
            description="a STARTED alert below its own threshold is a state-machine bug"),
]

ALL_CHECKS = ROUTE_DELAY_CHECKS + ALERT_CHECKS


def run_check(cursor: Any, check: DQCheck, table: str, date: str) -> DQResult:
    validate_date(date)
    cursor.execute(check.sql.format(table=table, date=date))
    violations = int(cursor.fetchone()[0] or 0)
    return DQResult(check.name, check.severity, violations, violations == 0, check.description)


def run_checks(cursor: Any, tables: dict[str, str], date: str, checks: list[DQCheck] = ALL_CHECKS) -> list[DQResult]:
    return [run_check(cursor, c, tables[c.table_key], date) for c in checks if c.table_key in tables]


def reconcile(expected_rows: int, loaded_rows: int, name: str) -> DQResult:
    """Glue manifest says N rows were written for the date; Snowflake must now hold exactly N."""
    diff = abs(expected_rows - loaded_rows)
    return DQResult(f"{name}.reconcile_with_glue_manifest", "error", diff, diff == 0,
                    f"manifest={expected_rows} snowflake={loaded_rows}")


def freshness(minutes_behind: float | None, max_minutes: int) -> DQResult:
    if minutes_behind is None:
        return DQResult("freshness", "error", 1, False, "no data loaded yet")
    late = max(0, int(minutes_behind) - max_minutes)
    return DQResult("freshness", "error", late, late == 0,
                    f"warehouse is {int(minutes_behind)} min behind (limit {max_minutes})")


def failures(results: list[DQResult]) -> list[DQResult]:
    """Blocking failures only."""
    return [r for r in results if not r.passed and r.severity == "error"]
