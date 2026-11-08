"""I/O layer: where the dashboard's data comes from.

HOT  path : DynamoDB `active_alerts` (seconds fresh)              -> active_alerts()
COLD path : Snowflake ANALYTICS views  (DATA_SOURCE=snowflake)    -> route_windows(), alerts()
            or the serving-zone Parquet in S3/emulator (DATA_SOURCE=local, no Snowflake account needed)
Both cold sources return the SAME columns, so the page code never knows which one it is talking to.
"""
from __future__ import annotations

import os

import pandas as pd

DATA_SOURCE = os.getenv("DATA_SOURCE", "local")  # "local" | "snowflake"


# --------------------------------------------------------------------------- hot path
_active_table = None
MAX_ALERT_PAGES = 50  # bounds the pagination loop: a misbehaving store must not trap a request thread forever


def _table():
    """One DynamoDB table handle for the process, with hard timeouts (a slow store fails fast instead of hanging a thread)."""
    global _active_table
    if _active_table is None:
        import boto3
        from botocore.config import Config

        kwargs = {"endpoint_url": os.environ["AWS_ENDPOINT_URL"]} if os.getenv("AWS_ENDPOINT_URL") else {}
        cfg = Config(connect_timeout=2, read_timeout=5, retries={"max_attempts": 0})
        _active_table = boto3.resource("dynamodb", region_name=os.getenv("AWS_REGION", "eu-west-1"), config=cfg, **kwargs).Table(
            os.getenv("ACTIVE_TABLE", "gtfsr_active_alerts"))
    return _active_table


def active_alerts() -> list[dict]:
    from boto3.dynamodb.conditions import Key

    table = _table()
    # GSI query (status = ACTIVE), not a table Scan: cost and latency stay flat as history grows.
    cond = Key("status").eq("ACTIVE")
    resp = table.query(IndexName="status-index", KeyConditionExpression=cond)
    items = resp["Items"]
    pages = 1
    while "LastEvaluatedKey" in resp:
        if pages >= MAX_ALERT_PAGES:
            raise RuntimeError(f"active alerts exceeded {MAX_ALERT_PAGES} pages: refusing to keep paging")
        resp = table.query(IndexName="status-index", KeyConditionExpression=cond, ExclusiveStartKey=resp["LastEvaluatedKey"])
        items += resp["Items"]
        pages += 1
    return items


# --------------------------------------------------------------------------- cold path
def _local_fs():
    from pyarrow import fs

    endpoint = os.getenv("S3_ENDPOINT_URL", "http://localhost:5555")
    return fs.S3FileSystem(endpoint_override=endpoint.replace("http://", "").replace("https://", ""),
                           scheme="https" if endpoint.startswith("https") else "http",
                           access_key=os.getenv("AWS_ACCESS_KEY_ID", "test"),
                           secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
                           region=os.getenv("AWS_REGION", "eu-west-1"))


def _read_serving(folder: str) -> pd.DataFrame:
    import pyarrow.parquet as pq

    bucket = os.getenv("S3_BUCKET", "gtfsr-lake")
    try:
        table = pq.read_table(f"{bucket}/serving/{folder}", filesystem=_local_fs(), partitioning="hive")
    except Exception:  # nothing curated yet
        return pd.DataFrame()
    df = table.to_pandas()
    for c in df.columns:  # hive partition columns can arrive as categoricals
        if str(df[c].dtype) == "category":
            df[c] = df[c].astype(str)
    return df


def _snowflake(sql: str) -> pd.DataFrame:
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE"),  # key-pair auth; never a password in code
        role="GTFSR_ANALYST", warehouse="GTFSR_BI_WH", database="GTFSR", schema="ANALYTICS")
    try:
        cur = conn.cursor()
        cur.execute(sql)
        df = pd.DataFrame(cur.fetchall(), columns=[c[0].lower() for c in cur.description])
    finally:
        conn.close()
    return df


def route_windows() -> pd.DataFrame:
    if DATA_SOURCE == "snowflake":
        return _snowflake("SELECT * FROM V_ROUTE_DELAY_5M WHERE WINDOW_START >= DATEADD('day', -7, CURRENT_TIMESTAMP())")
    return _read_serving("route_delay_5m")


def alerts() -> pd.DataFrame:
    if DATA_SOURCE == "snowflake":
        return _snowflake("SELECT * FROM GTFSR.RAW.ALERTS WHERE ALERT_DATE >= DATEADD('day', -7, CURRENT_DATE())")
    return _read_serving("alerts")
