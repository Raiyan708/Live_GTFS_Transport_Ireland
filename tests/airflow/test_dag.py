"""Runs inside the Airflow image (scripts/run_airflow_tests.sh): needs Airflow + provider packages."""
import json
from datetime import datetime, timezone
from unittest import mock

import pytest

airflow = pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

DAG_ID = "gtfsr_serving_pipeline"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    return bag.get_dag(DAG_ID)


def upstream(dag, task_id):
    return {t.task_id for t in dag.get_task(task_id).upstream_list}


def test_dag_settings(dag):
    assert dag.max_active_runs == 1  # overlapping runs would fight over the same partitions
    assert dag.catchup is False
    assert str(dag.schedule_interval) == "*/15 * * * *"


def test_task_graph_matches_the_design(dag):
    assert set(dag.task_ids) == {"freshness_gate", "glue_curate", "load_route_delay", "load_alerts",
                                 "data_quality", "publish_status"}
    assert upstream(dag, "glue_curate") == {"freshness_gate"}  # explicit dependency (was missing once)
    assert upstream(dag, "load_route_delay") >= {"glue_curate"}
    assert upstream(dag, "load_alerts") >= {"glue_curate"}
    assert upstream(dag, "data_quality") == {"load_route_delay", "load_alerts"}
    assert upstream(dag, "publish_status") == {"data_quality"}


def test_loads_cannot_start_before_glue_finishes(dag):
    # walk upstream transitively: glue_curate must be an ancestor of every load
    for tid in ("load_route_delay", "load_alerts"):
        assert "glue_curate" in {t.task_id for t in dag.get_task(tid).get_flat_relatives(upstream=True)}


def test_glue_is_deferrable_and_waits(dag):
    glue = dag.get_task("glue_curate")
    assert glue.deferrable is True and glue.wait_for_completion is True
    assert "--process_dates" in glue.script_args and "--run_id" in glue.script_args


def test_retries_configured(dag):
    for t in dag.tasks:
        assert t.retries >= 1 and t.retry_exponential_backoff


def test_process_dates_crosses_month_and_converts_to_utc():
    import sys
    sys.path.insert(0, "/opt/airflow/dags")
    from gtfsr_serving_pipeline import process_dates

    assert process_dates(datetime(2026, 3, 1, 0, 5, tzinfo=timezone.utc)) == ["2026-02-28", "2026-03-01"]


# --- data_quality task logic, with the cloud hooks faked -----------------------
class FakeCursor:
    def __init__(self, violations=0, minutes_behind=5):
        self.violations, self.minutes_behind, self._last = violations, minutes_behind, ""

    def execute(self, sql, params=None):
        self._last = sql

    def fetchone(self):
        return (self.minutes_behind,) if "MINUTES_BEHIND" in self._last else (self.violations,)


class FakeConn:
    def __init__(self, **kw):
        self.cur = FakeCursor(**kw)

    def cursor(self):
        return self.cur

    def close(self):
        pass


def run_dq(violations=0, manifest_rows=10, loaded=10, minutes_behind=5):
    import sys
    sys.path.insert(0, "/opt/airflow/dags")
    import gtfsr_serving_pipeline as m

    manifest = json.dumps({"rows": {"route_delay_5m": {"2026-11-06": manifest_rows}, "alerts": {"2026-11-06": 4}}})
    with mock.patch.object(m, "S3Hook") as s3, mock.patch.object(m, "SnowflakeHook") as sf:
        s3.return_value.read_key.return_value = manifest
        sf.return_value.get_conn.return_value = FakeConn(violations=violations, minutes_behind=minutes_behind)
        return m.run_data_quality({"2026-11-06": loaded}, {"2026-11-06": 4}, "scheduled__2026-11-06T16:00:00+00:00")


def test_dq_task_passes_on_clean_data():
    assert run_dq()["checks"] > 5


def test_dq_task_fails_on_check_violation():
    with pytest.raises(RuntimeError, match="data quality failed"):
        run_dq(violations=3)


def test_dq_task_fails_when_snowflake_count_differs_from_glue_manifest():
    # DQ SQL itself returns 0 violations, but Glue wrote 10 rows and Snowflake holds 9
    with pytest.raises(RuntimeError, match="reconcile_with_glue_manifest"):
        run_dq(manifest_rows=10, loaded=9)


def test_dq_task_fails_when_warehouse_is_stale():
    with pytest.raises(RuntimeError, match="freshness"):
        run_dq(minutes_behind=500)


# --- freshness gate -----------------------------------------------------------
def fresh_gate(objects, now=datetime(2026, 11, 6, 17, 0, tzinfo=timezone.utc)):
    import sys
    sys.path.insert(0, "/opt/airflow/dags")
    import gtfsr_serving_pipeline as m

    with mock.patch.object(m, "S3Hook") as s3:
        s3.return_value.get_file_metadata.return_value = objects
        m.check_freshness(now)


def obj(minute):
    return {"Key": "k", "LastModified": datetime(2026, 11, 6, 16, minute, tzinfo=timezone.utc)}


def test_freshness_gate_passes_when_recent_data_exists_even_if_listed_out_of_order():
    fresh_gate([obj(0), obj(55), obj(10)])  # newest is 16:55, now 17:00 -> 5 min old


def test_freshness_gate_fails_when_everything_is_old():
    with pytest.raises(RuntimeError, match="min old"):
        fresh_gate([obj(0), obj(5)])  # 16:05 -> 55 min old, limit 45


def test_freshness_gate_fails_when_nothing_was_ever_written():
    with pytest.raises(RuntimeError, match="never wrote"):
        fresh_gate([])
