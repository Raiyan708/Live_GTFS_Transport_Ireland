# Real-time Irish transport delay alerting

GTFS-Realtime (Transport for Ireland) → Kafka → Spark Structured Streaming → **hot path** (stateful alerts → Lambda → DynamoDB/SNS)
and **cold path** (S3 → Glue → Snowflake → Streamlit), orchestrated by Airflow.

**Read `docs/DECISION_LOG.md` first** (its opening note matters): every design decision, the alternatives rejected, and an honest list of what is *not* verified.
Your own Phase 1 write-up is `DECISIONS.md`; the two Phase 1 implementations (`ingestion/` vs `src/gtfsrt/`) differ and need reconciling: see the table at the top of the decision log.

```
feed ─► producer ─► Kafka ──┬─► Spark raw sink ─────────────► S3 raw/
 (protobuf→JSON)  key=date|trip │─► Spark 5-min route windows ──► S3 curated/route_delay_5m ─┐
                              │─► Spark per-trip state machine ► Kafka delay_alerts ──┬─► Lambda ─► DynamoDB + SNS   (HOT, seconds)
                              │                                                        └─► Spark ─► S3 curated/alerts ┤
                                                          Glue ETL: dedupe/conform/compact ◄──────────────────────────┘
                                                          └► S3 serving/ ─► Airflow: partition-replace COPY ─► Snowflake RAW ─► views ─► Streamlit
```

## Run it locally (no AWS or Snowflake account needed)
Needs Docker. Python is only used on the host for the producer, Lambda runner, tests and dashboard.
```bash
python3 -m venv .venv && ./.venv/bin/pip install -r requirements.txt
cp .env.example .env
docker compose up -d kafka kafka-init aws-emulator aws-init          # Kafka + S3/DynamoDB/SNS emulator (moto)
export PYTHONPATH=src AWS_ENDPOINT_URL=http://localhost:5555 AWS_DEFAULT_REGION=eu-west-1 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
./.venv/bin/python scripts/init_aws_local.py                         # DynamoDB tables + SNS topic
docker compose --profile streaming up -d spark-raw-sink spark-route-aggregates spark-delay-alerts spark-alerts-sink
FEED_MODE=simulator POLL_INTERVAL_SECONDS=2 ./.venv/bin/python -m gtfsrt.producer          # synthetic feed (real protobuf bytes)
ALERT_TOPIC_ARN=arn:aws:sns:eu-west-1:123456789012:gtfsr-alerts ./.venv/bin/python -m gtfsrt.lambda_alerts.local_runner
./scripts/run_curate_local.sh --process_dates $(date -u +%F)         # Glue job, run locally
./scripts/run_dashboard.sh                                           # http://localhost:8501
```
For the live feed: `FEED_MODE=live` with the variables from `.env.example` (same names as your real `.env`). Live mode makes **one API call per 60 s, alternating the two feeds** (NTA fair-usage limit), so each feed refreshes every 120 s.
`src/gtfsrt/producer.py` has **not** been run against the live API (only `ingestion/` has); expect to tune the Spark watermark to the real event-time lag first.

> ⚠ Stop Spark jobs **by name**: `docker compose --profile streaming stop spark-raw-sink …`. A bare `stop` also stops Kafka and the emulator,
> and the emulator keeps S3 in memory, so the lake is lost (it can be rebuilt by replaying Kafka).
> Tear down: `docker compose --profile streaming down -v`.

## Tests (118)
```bash
./.venv/bin/pytest -q              # 94 — state machine, decoding, Lambda (moto), DQ (SQLite), Snowflake SQL, dashboard views, packaging
./scripts/run_spark_tests.sh       # 11 — Glue transforms, in the Spark container (host has no Java)
./scripts/run_airflow_tests.sh     # 13 — the DAG, in an Airflow 2.10 image
```

## Layout
| Path | What |
|---|---|
| `src/gtfsrt/` | `decode`, `producer`, `simulator`, **`alerting`** (pure state machine), `streaming/` (Spark jobs), `lambda_alerts/`, `transform/`, `warehouse/` |
| `glue/curate_job.py` | Glue ETL entrypoint (also runs locally) |
| `snowflake/` | setup, storage, tables, views, Snowpipe alternative (**never executed**) |
| `airflow/dags/` | control-plane DAG |
| `infra/terraform/` | `terraform validate` passes; **never applied** |
| `dashboard/` | Streamlit app (hot: DynamoDB, cold: Snowflake or local Parquet) |

## Status: verified vs not
- **Verified by running:** producer → Kafka; all four Spark jobs; crash/restart recovery; replay from Kafka; Lambda against real Kafka + emulated DynamoDB; Glue job; dashboard (light + dark); all 118 tests.
- **Verified statically only:** Snowflake SQL (parsed, not executed), Terraform (`validate`, not applied), Airflow DAG (imports and graph, not run against real AWS/Snowflake).
- **Not tested:** real S3 behaviour (moto ≠ S3), Glue 4.0's Spark 3.3 (local is 3.5), the live NTA feed, MSK, IAM policies.
- **Known limitation:** if the feed goes completely silent, active alerts never expire (event-time timeouts need the watermark to advance). The dashboard shows data age; the Airflow freshness gate fails the DAG.
