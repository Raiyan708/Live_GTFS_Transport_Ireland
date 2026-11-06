#!/usr/bin/env bash
# Launch the dashboard against the LOCAL stack (Kafka/emulator via docker compose).
# For Snowflake instead:  DATA_SOURCE=snowflake SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... scripts/run_dashboard.sh
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD/src:$PWD"
export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:5555}"
export S3_ENDPOINT_URL="${S3_ENDPOINT_URL:-http://localhost:5555}"
export AWS_REGION="${AWS_REGION:-eu-west-1}" AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-eu-west-1}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}" AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
exec ./.venv/bin/streamlit run dashboard/app.py --server.port "${PORT:-8501}" --server.headless true \
     --browser.gatherUsageStats false "$@"
