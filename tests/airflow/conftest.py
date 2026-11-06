"""DAG tests need the real Airflow + providers: they run in the Airflow image (scripts/run_airflow_tests.sh).

Guard: this repo has a top-level `airflow/` folder (the DAGs) and `.` is on pytest's pythonpath, so on the host
`import airflow` resolves to OUR folder as a namespace package. `find_spec("airflow.models")` only exists in the real library.
"""
import importlib.util

try:
    _REAL_AIRFLOW = importlib.util.find_spec("airflow.models") is not None
except (ImportError, ValueError):
    _REAL_AIRFLOW = False

if not _REAL_AIRFLOW:
    collect_ignore_glob = ["test_*.py"]
