"""Control plane for the COLD path:  curated/ -> (Glue) -> serving/ -> (Snowflake) -> RAW -> DQ gate.

Airflow moves NO data. It decides whether it is safe to proceed and tells a human when it is not.
The HOT path (Kafka -> Spark -> Lambda -> DynamoDB) does not involve Airflow at all, by design:
a scheduler tick is the wrong latency model for alerts.

    freshness_gate >> glue_curate >> [load_route_delay, load_alerts] >> data_quality >> publish_status

Design choices (see docs/DECISION_LOG.md, Phase 5):
  * schedule */15, max_active_runs=1  - overlapping runs would fight over the same date partitions
  * catchup=False                     - the load is a partition REPLACE over [yesterday, today]; a backlog
                                        of historical runs adds nothing (use a manual backfill instead)
  * every task is idempotent          - retries and reruns are always safe
  * DQ failure -> DAG failure         - bad data must not be silently "published"
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from gtfsrt.runid import safe_run_id
from gtfsrt.warehouse import dq
from gtfsrt.warehouse.loader import ALERTS, ROUTE_DELAY, load_partition

BUCKET = os.getenv("GTFSR_BUCKET", "gtfsr-lake")
GLUE_JOB = os.getenv("GTFSR_GLUE_JOB", "gtfsr-curate")
AWS_CONN = "aws_default"
SNOWFLAKE_CONN = "snowflake_gtfsr"
MAX_STALE_MINUTES = int(os.getenv("GTFSR_MAX_STALE_MINUTES", "45"))  # 3 missed cycles
FRESHNESS_PREFIX = "curated/route_delay_5m/"
TABLES = {"route_delay": "GTFSR.RAW.ROUTE_DELAY_5M", "alerts": "GTFSR.RAW.ALERTS"}

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=20),
}


def process_dates(data_interval_end: datetime) -> list[str]:
    """Yesterday + today (UTC) relative to the run's logical time.
    Yesterday is included because late data / the last windows of the day settle after midnight."""
    end = data_interval_end.astimezone(timezone.utc)
    return [(end - timedelta(days=1)).strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")]


def check_freshness(now: datetime | None = None) -> None:
    """Is the streaming layer alive? If curated/ has not changed recently, running Glue + loading would just
    re-publish stale data and report success. Fail loudly instead (this is the alert for 'Spark is down')."""
    hook = S3Hook(aws_conn_id=AWS_CONN)
    # Real LastModified timestamps, not key order (Spark file names are random UUIDs, so key order != time order).
    objects = hook.get_file_metadata(prefix=FRESHNESS_PREFIX, bucket_name=BUCKET) or []
    if not objects:
        raise RuntimeError(f"no objects under s3://{BUCKET}/{FRESHNESS_PREFIX}: streaming layer never wrote")
    newest = max(o["LastModified"] for o in objects)
    age_min = ((now or datetime.now(timezone.utc)) - newest).total_seconds() / 60
    if age_min > MAX_STALE_MINUTES:
        raise RuntimeError(f"curated zone is {age_min:.0f} min old (limit {MAX_STALE_MINUTES}): streaming job down?")


def run_data_quality(loaded_route: dict, loaded_alerts: dict, run_id: str) -> dict:
    """SQL checks per date + reconciliation with the Glue manifest + freshness. Raises on blocking failures."""
    manifest = json.loads(
        S3Hook(aws_conn_id=AWS_CONN).read_key(key=f"serving/_manifests/{safe_run_id(run_id)}.json", bucket_name=BUCKET)
    )
    conn = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN).get_conn()
    results: list[dq.DQResult] = []
    try:
        cur = conn.cursor()
        for date in loaded_route:
            results += dq.run_checks(cur, TABLES, date)
            results.append(dq.reconcile(manifest["rows"]["route_delay_5m"].get(date, 0), loaded_route[date], f"route_delay[{date}]"))
            results.append(dq.reconcile(manifest["rows"]["alerts"].get(date, 0), loaded_alerts.get(date, 0), f"alerts[{date}]"))
        cur.execute("SELECT MINUTES_BEHIND FROM GTFSR.ANALYTICS.V_PIPELINE_FRESHNESS")
        results.append(dq.freshness((cur.fetchone() or [None])[0], MAX_STALE_MINUTES))
    finally:
        conn.close()

    bad = dq.failures(results)
    if bad:
        raise RuntimeError("data quality failed: " + "; ".join(f"{r.name} ({r.violations}) {r.description}" for r in bad))
    return {"checks": len(results), "warnings": [r.name for r in results if not r.passed],
            "report": [r.__dict__ for r in results]}


@dag(
    dag_id="gtfsr_serving_pipeline",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gtfsr", "snowflake", "glue"],
    doc_md=__doc__,
)
def gtfsr_serving_pipeline():
    @task
    def freshness_gate(data_interval_end=None) -> list[str]:
        check_freshness()
        return process_dates(data_interval_end)

    dates = freshness_gate()

    glue_curate = GlueJobOperator(
        task_id="glue_curate",
        job_name=GLUE_JOB,
        script_args={
            "--process_dates": "{{ ti.xcom_pull(task_ids='freshness_gate') | join(',') }}",
            "--run_id": "{{ run_id }}",
            "--bucket": BUCKET,
        },
        aws_conn_id=AWS_CONN,
        wait_for_completion=True,
        deferrable=True,  # frees the worker slot while Glue runs (minutes) instead of blocking it
    )
    # The dates reach Glue through a Jinja string, which Airflow cannot see as a dependency: be explicit,
    # otherwise Glue could start before the freshness gate has finished.
    dates >> glue_curate

    def _load(spec, label):
        @task(task_id=f"load_{label}")
        def load(dates: list[str], run_id=None) -> dict[str, int]:
            conn = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN).get_conn()
            try:
                return {d: load_partition(conn, spec, d, run_id) for d in dates}
            finally:
                conn.close()

        return load(dates)

    loaded_route = _load(ROUTE_DELAY, "route_delay")
    loaded_alerts = _load(ALERTS, "alerts")
    glue_curate >> [loaded_route, loaded_alerts]

    @task
    def data_quality(loaded_route: dict, loaded_alerts: dict, run_id=None) -> dict:
        return run_data_quality(loaded_route, loaded_alerts, run_id)

    quality = data_quality(loaded_route, loaded_alerts)

    @task
    def publish_status(quality: dict) -> None:
        """Last step runs only if every gate passed. Real deployments would post to Slack/SNS here."""
        print(f"[publish] OK - {quality['checks']} checks passed; warnings: {quality['warnings'] or 'none'}")

    publish_status(quality)


gtfsr_serving_pipeline()
