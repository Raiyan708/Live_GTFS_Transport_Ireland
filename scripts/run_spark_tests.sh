#!/usr/bin/env bash
# Run the Spark-dependent tests inside the Spark container (host has no Java).
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose --profile streaming run --rm --no-deps -T \
  --entrypoint python3 -e PYTHONPATH=/app/src:/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip spark-raw-sink \
  -m pytest /app/tests/spark -q -p no:cacheprovider "$@"
