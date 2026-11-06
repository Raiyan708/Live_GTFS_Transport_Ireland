#!/usr/bin/env bash
# Build the deployment artifacts and (optionally) upload them to the lake bucket.
#
#   scripts/package_and_upload.sh --build-only          # just create dist/*.zip
#   BUCKET=my-lake scripts/package_and_upload.sh        # build + upload to s3://$BUCKET/artifacts/
#
# dist/gtfsrt.zip         Glue --extra-py-files   (the whole gtfsrt package: transform + shared helpers)
# dist/lambda_alerts.zip  Lambda package          (only what the handler imports; boto3 is in the runtime)
# dist/curate_job.py      Glue script
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf dist && mkdir -p dist

# -x excludes bytecode and Spark streaming jobs (not needed in Glue/Lambda; they pull in heavy imports)
(cd src && zip -qr ../dist/gtfsrt.zip gtfsrt -x '*__pycache__*' '*.pyc' 'gtfsrt/streaming/*' 'gtfsrt/lambda_alerts/*')
(cd src && zip -qr ../dist/lambda_alerts.zip gtfsrt/__init__.py gtfsrt/lambda_alerts -x '*__pycache__*' '*.pyc')
cp glue/curate_job.py dist/curate_job.py
echo "built:"; ls -l dist | sed 's/^/  /'

[[ "${1:-}" == "--build-only" ]] && exit 0
: "${BUCKET:?set BUCKET to the lake bucket name (or use --build-only)}"
EXTRA=(); [[ -n "${AWS_ENDPOINT_URL:-}" ]] && EXTRA=(--endpoint-url "$AWS_ENDPOINT_URL")
for f in gtfsrt.zip lambda_alerts.zip curate_job.py; do
  aws "${EXTRA[@]}" s3 cp "dist/$f" "s3://$BUCKET/artifacts/$f"
done
