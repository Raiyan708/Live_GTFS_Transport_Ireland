"""Debug helper: batch-read a lake path and print schema, counts and a sample.

    docker compose run --rm --no-deps -T --entrypoint /opt/spark/bin/spark-submit \
        spark-raw-sink /app/scripts/inspect_lake.py raw [group_col ...]
"""
import sys

from gtfsrt.streaming.common import build_spark, s3_uri

path = sys.argv[1]
group_cols = sys.argv[2:]
spark = build_spark("inspect")
df = spark.read.option("basePath", s3_uri(path)).parquet(s3_uri(path))
df.printSchema()
print("ROWS:", df.count())
if group_cols:
    df.groupBy(*group_cols).count().orderBy(*group_cols).show(50, truncate=False)
df.show(5, truncate=80)
