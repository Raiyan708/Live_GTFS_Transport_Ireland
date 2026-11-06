#!/usr/bin/env bash
# Build the Airflow test image (once) and run DAG tests inside it.
set -euo pipefail
cd "$(dirname "$0")/.."
docker build -q -t gtfsr-airflow-test docker/airflow >/dev/null
docker run --rm \
  -e PYTHONPATH=/opt/airflow/src -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
  -e AIRFLOW__CORE__UNIT_TEST_MODE=True -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow.db \
  -v "$PWD/airflow/dags:/opt/airflow/dags:ro" -v "$PWD/src:/opt/airflow/src:ro" -v "$PWD/tests/airflow:/opt/airflow/tests:ro" \
  --entrypoint bash gtfsr-airflow-test -c \
  'airflow db migrate >/dev/null 2>&1 && python -m pytest /opt/airflow/tests -q -p no:cacheprovider "$@"' _ "$@"
