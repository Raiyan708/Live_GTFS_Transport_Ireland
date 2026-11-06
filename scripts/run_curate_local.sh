#!/usr/bin/env bash
# Run the Glue curate job locally against the S3 emulator.  Usage: scripts/run_curate_local.sh [--process_dates 2026-11-06]
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose --profile streaming run --rm --no-deps -T \
  -e PYTHONPATH=/app/src:/app --entrypoint /opt/spark/bin/spark-submit spark-raw-sink \
  /app/glue/curate_job.py --s3_endpoint http://aws-emulator:5000 "$@"
