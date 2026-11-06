"""Spark tests need Java + PySpark: they run in the Spark container (scripts/run_spark_tests.sh).
On the host (no Java) they are skipped at collection time instead of erroring."""
import shutil

try:
    import pyspark  # noqa: F401

    _HAS_SPARK = shutil.which("java") is not None
except ImportError:
    _HAS_SPARK = False

if not _HAS_SPARK:
    collect_ignore_glob = ["test_*.py"]

import pytest  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]").appName("tests")
        .config("spark.jars.packages", "")  # tests never touch Kafka/S3
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()
