"""AWS Glue ETL (Spark) job: curated/ -> serving/   (dedupe, conform, compact).

Glue passes arguments as `--key value`; argparse parses those fine, so the SAME script
runs in the Glue runtime and in the local Spark container (no awsglue dependency needed).

  --bucket          S3 bucket (default gtfsr-lake)
  --process_dates   comma list YYYY-MM-DD (UTC partition dates). Default: yesterday + today.
  --allow_empty     do not fail when no input rows are found
  --s3_endpoint     emulator endpoint for local runs (omit in AWS)

Idempotent: dynamic partition overwrite replaces ONLY the requested date partitions, so a
rerun / backfill for the same dates yields the same output.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

from gtfsrt.runid import safe_run_id
from gtfsrt.transform.curate import curate_alerts, curate_route_delay, filter_dates


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", default="gtfsr-lake")
    p.add_argument("--process_dates", default="")
    p.add_argument("--allow_empty", action="store_true")
    p.add_argument("--s3_endpoint", default="")
    p.add_argument("--base_uri", default="", help="override 's3a://<bucket>' (used by tests with file://)")
    p.add_argument("--run_id", default="", help="orchestrator run id; names the manifest file")
    args, _unknown = p.parse_known_args(argv)  # Glue adds --JOB_NAME, --JOB_RUN_ID, ... : ignore them
    return args


def resolve_dates(arg: str, now: datetime | None = None) -> list[str]:
    if arg.strip():
        return [d.strip() for d in arg.split(",") if d.strip()]
    now = now or datetime.now(timezone.utc)
    return [(now - timedelta(days=1)).strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d")]


def build_session(args: argparse.Namespace) -> SparkSession:
    b = (
        SparkSession.builder.appName("gtfsr-curate")
        # replace only the partitions present in the output, not the whole table
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # no _SUCCESS marker files: they would fire Snowpipe's S3 event notifications for nothing
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        # Snowflake reads TIMESTAMP_MICROS cleanly; Spark's default INT96 is a legacy encoding.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    )
    if args.s3_endpoint:
        b = (
            b.config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.access.key", "test")
            .config("spark.hadoop.fs.s3a.secret.key", "test")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        )
    return b.getOrCreate()


def run(spark: SparkSession, base: str, dates: list[str], allow_empty: bool) -> dict[str, int]:
    summary: dict = {"by_date": {}}
    jobs = [
        ("route_delay_5m", f"{base}/curated/route_delay_5m", f"{base}/serving/route_delay_5m",
         "window_date", curate_route_delay),
        ("alerts", f"{base}/curated/alerts", f"{base}/serving/alerts", "alert_date", curate_alerts),
    ]
    for name, src, dst, part_col, fn in jobs:
        # `basePath` makes the partition column (e.g. window_date) available when reading a subfolder tree.
        raw = spark.read.option("basePath", src).parquet(src)
        scoped = filter_dates(raw, part_col, dates)
        n_in = scoped.count()
        if n_in == 0 and not allow_empty:
            raise RuntimeError(f"{name}: no input rows for dates {dates} (use --allow_empty to permit)")
        if n_in == 0:
            # Allowed-empty: write nothing. Writing an empty frame creates no folder (so the read-back
            # below would fail) and we must never let empty input replace existing output.
            summary[f"{name}_in"], summary[f"{name}_out"] = 0, 0
            summary["by_date"][name] = {}
            print(f"[curate] {name}: no input rows for {dates}; skipped")
            continue
        out = fn(scoped)
        (
            out.repartition(part_col)  # one shuffle group per date -> compact files
            .write.mode("overwrite")
            .option("maxRecordsPerFile", 1_000_000)
            .partitionBy(part_col)
            .parquet(dst)
        )
        written = filter_dates(spark.read.option("basePath", dst).parquet(dst), part_col, dates)
        by_date = {str(r[0]): int(r[1]) for r in written.groupBy(part_col).count().collect()}
        n_out = sum(by_date.values())
        summary["by_date"][name] = by_date
        summary[f"{name}_in"], summary[f"{name}_out"] = n_in, n_out
        print(f"[curate] {name}: in={n_in} out={n_out} dates={dates}")
    return summary


def write_manifest(base: str, run_id: str, dates: list[str], summary: dict, endpoint: str = "") -> str:
    """Publish what this run wrote, so the orchestrator can RECONCILE it against the warehouse.
    Lives under serving/_manifests/ (the Snowflake COPY only reads the per-table folders)."""
    body = json.dumps({"run_id": run_id, "dates": dates, "rows": summary["by_date"],
                       "written_at": datetime.now(timezone.utc).isoformat()}, indent=2)
    rel = f"serving/_manifests/{safe_run_id(run_id)}.json"
    if base.startswith("s3a://"):
        import boto3  # available in the Glue runtime

        bucket = base[len("s3a://"):].split("/", 1)[0]
        kwargs = {"endpoint_url": endpoint} if endpoint else {}
        boto3.client("s3", **kwargs).put_object(Bucket=bucket, Key=rel, Body=body.encode(),
                                                ContentType="application/json")
    else:
        import pathlib

        target = pathlib.Path(base.replace("file://", "")) / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body)
    return rel


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    spark = build_session(args)
    base = args.base_uri or f"s3a://{args.bucket}"
    dates = resolve_dates(args.process_dates)
    summary = run(spark, base, dates, args.allow_empty)
    print("[curate] manifest:", write_manifest(base, args.run_id, dates, summary, args.s3_endpoint))


if __name__ == "__main__":
    main()
