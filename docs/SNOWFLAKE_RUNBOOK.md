# Running the Snowflake part for real (about 30 minutes, no AWS needed)

**Goal:** turn "the Snowflake SQL has been parsed but never executed" into "it ran, and the Snowflake views return the same numbers
as my pandas calculations on the same data". `scripts/snowflake_verify.py` does the whole thing; you only create the account and
the key. **Never paste a private key or password into a chat, an issue or a commit.**

## 1. Create a free trial account (once)
1. Go to <https://signup.snowflake.com/>, choose **Standard** edition and a European region (e.g. AWS, Ireland).
2. Open the account menu in the Snowsight UI (bottom-left) → **Copy account identifier**. It looks like `myorg-myaccount`.

## 2. Make a key pair (recommended; a password also works if your trial has no MFA)
```bash
mkdir -p ~/.snowflake
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.snowflake/rsa_key.p8 -nocrypt
openssl rsa -in ~/.snowflake/rsa_key.p8 -pubout -out ~/.snowflake/rsa_key.pub
chmod 600 ~/.snowflake/rsa_key.p8
cat ~/.snowflake/rsa_key.pub      # copy the text BETWEEN the BEGIN/END lines (the PUBLIC key; safe to paste into Snowflake)
```
In a Snowsight worksheet (as your own user), register the **public** key:
```sql
ALTER USER <YOUR_SNOWFLAKE_USERNAME> SET RSA_PUBLIC_KEY='<paste the public key text here, no header/footer lines>';
```
The `.p8` file is the secret: it stays on your machine, and `.gitignore` already excludes `.env`; do not put it in the repo.

## 3. Preview, then run
```bash
cd Live_GTFS_Transport_Ireland
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test S3_ENDPOINT_URL=http://localhost:5555 PYTHONPATH=src:.

# (a) Preview: exports the data, prints the numbers Snowflake must reproduce, and shows what it would run. Does NOT connect.
./.venv/bin/python scripts/snowflake_verify.py --dry-run

# (b) The real run. Credentials come from your shell and are never printed or stored by the script.
export SNOWFLAKE_ACCOUNT='myorg-myaccount'
export SNOWFLAKE_USER='YOUR_SNOWFLAKE_USERNAME'
export SNOWFLAKE_PRIVATE_KEY_FILE="$HOME/.snowflake/rsa_key.p8"
./.venv/bin/python scripts/snowflake_verify.py
```
The data it uses is whatever is in the local lake (the Glue "serving" output). If the lake was wiped, rebuild it first
(`docs/DECISION_LOG.md`, Phase 8c: replay Kafka through the Spark jobs, then `scripts/run_curate_local.sh`).

## 4. What it does, in order
1. Runs `snowflake/00_setup.sql` (roles, two X-Small warehouses with 60-second auto-suspend, database, schemas, grants), grants the loader role
   to your user, then `01_storage_internal.sql`, `02_tables.sql`, `03_views.sql`.
2. Uploads the serving Parquet to an **internal** stage (`PUT`) in the same folder layout the production S3 stage uses.
3. Runs the **production** loader (`gtfsrt.warehouse.loader`): `BEGIN; DELETE the day; COPY INTO ...; audit row; COMMIT` (rollback on any error).
4. Runs the data-quality SQL (`warehouse/dq.py`).
5. Compares the **Snowflake views** with the pandas twins on the same data: row counts after de-duplication, total readings, total late
   readings, number of routes, the top-10 least-reliable routes (order, share and reading counts), alert rows and alert episodes.
6. Writes `docs/SNOWFLAKE_VERIFY_REPORT.md` (no secrets in it) and exits 0 only if every check passed.

## 5. Send me the result
Paste the contents of `docs/SNOWFLAKE_VERIFY_REPORT.md` (or the console output). If something fails, the error is scrubbed of secrets;
paste that too. I will fix the SQL or the script; expect one or two rounds, because this code has never met a real Snowflake.

## 6. Cost and clean-up
The trial includes free credits; this workload is a few thousand rows on X-Small warehouses that suspend after 60 s, so it costs effectively nothing.
To remove everything:
```sql
USE ROLE ACCOUNTADMIN;
DROP DATABASE IF EXISTS GTFSR;
DROP WAREHOUSE IF EXISTS GTFSR_LOAD_WH;
DROP WAREHOUSE IF EXISTS GTFSR_BI_WH;
DROP ROLE IF EXISTS GTFSR_LOADER;
DROP ROLE IF EXISTS GTFSR_ANALYST;
```

## What this does and does not prove
- **Proves:** the table DDL, the COPY with its transformation SELECT, the partition-replace transaction, the DQ SQL and the six views run on real Snowflake
  and agree with the pandas implementation.
- **Does not prove:** the S3 storage integration (`01_storage.sql`; needs an AWS account) or the Airflow DAG against Snowflake (a second, shorter step
  once this passes: point the DAG's connection at the same account).
