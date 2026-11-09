# CV entry, demo script and interview answers

Everything below is something the repo can back up. Where a claim depends on something you still have to do, it says so.

## 1. CV entry (copy-paste)

**Real-Time Irish Public Transport Delay Alerting** | Python, Kafka, Spark Structured Streaming, AWS (Lambda, DynamoDB, S3, Glue), Snowflake, Airflow, Terraform, FastAPI, React/TypeScript | GitHub: `Raiyan708/Live_GTFS_Transport_Ireland` | Live demo: `raiyan708.github.io/Live_GTFS_Transport_Ireland`

- Built an end-to-end streaming pipeline on the National Transport Authority's live GTFS-Realtime feed (about 1,000 vehicles and 3,000 trips every 2 minutes): idempotent Kafka producer keyed by trip, Spark Structured Streaming, and a stateful per-trip alert engine (hysteresis, debounce, escalation tiers, deterministic alert ids) with at-least-once delivery and idempotent sinks.
- Validated the pipeline against the live feed instead of synthetic data, and let the data drive the design: recomputed Spark aggregates independently from raw Kafka (**0 differences over 4,599 route-windows**), tuned alert expiry from 97,000 observed trip appearances (30 min to 10 min), and found that 40.7% of readings were placeholders for trips that had not started, which had been understating the true late share (19% vs 30%).
- Found and fixed defects only real data exposed: alerts that never expired when expiry and start shared a timestamp, stale delay values on top-tier alerts, GPS fixes near (0,0), and a self-inflicted overload of a shared storage emulator (single-flight cache, client timeouts).
- Built a React/TypeScript + MapLibre dashboard backed by a FastAPI/SSE service: animated live map, vehicle-backed alert list, route-reliability ranking and heatmap, with an honest data-quality layer (stalled-trip detection, stale-pipeline warning). Public demo replays real recorded data.
- **267 automated tests** (Python, Spark, Airflow, TypeScript), mutation testing of the critical logic, CI, and a decision log recording every design choice, rejected alternative and unverified claim.

Shorter (4 lines): bullets 1, 2, 4 and the tests line.

**Add after you complete these:**
- *What you can say today (no Snowflake account needed):* "Warehouse view SQL (Snowflake dialect) verified against pandas implementations of the same metrics on a second SQL engine (DuckDB, via sqlglot), with mutation testing of the SQL."
- *Only after a real Snowflake run passes:* "Verified the Snowflake load path (partition-replace COPY, views, data-quality SQL) on Snowflake."
- *Only if you deploy to real AWS:* "Deployed with Terraform to AWS". Until then say "Terraform-defined" and "ran on a local AWS emulator". Do not say "deployed".

## 2. What not to claim (yet)
| Do not say | Because | How to earn it |
|---|---|---|
| "Deployed on AWS" / "production" | The AWS services ran on a local emulator; Terraform validates but was never applied | `terraform apply` on a real account, or keep the wording above |
| "Snowflake data warehouse" / "ran on Snowflake" | The SQL has never been accepted by Snowflake; only its view logic was checked (DuckDB) | Run `scripts/snowflake_verify.py` (30 min, needs a trial account; a card-free trial may or may not be available to you) |
| "Orchestrated by Airflow" as a verified layer | The DAG is tested in a container, not run against real services | After Snowflake passes, point the DAG at the same account |
| Throughput / latency numbers | None were measured under load | A load test; until then use the measured data volumes only |
| "Exactly-once" | It is at-least-once plus idempotency, on purpose | Keep the honest wording; interviewers like it |
| That delays are accurate | They are NTA's numbers, never compared with ground truth | Spot-check a few buses against the official real-time information |

## 3. The 90-second demo
1. Open the demo. The About panel explains the system in one screen: read the **solid vs dashed** architecture legend aloud.
2. Close it. "This is a replay of real evening-rush data. Each dot is a real vehicle; the rings pulse when it is 5+ minutes late."
3. Point at **STALLED**: "These trips' delay grows exactly as fast as the clock. I found that by looking at the data, and flagged them instead of calling them critical."
4. Untick/tick **predicted delays**: "Only 45% of alerts have a vehicle. The rest flipped in bulk, so the default hides them."
5. Open **Route reliability**: "40% of readings were for trips that had not started, so I excluded them, and I don't rank a route with fewer than three trips."
6. Finish with the honest line: "Snowflake and Airflow are written and unit-tested but not yet run for real" (or "now verified", once they are).

## 4. Questions you will get (and where the answer lives)
- **Exactly-once?** No: at-least-once plus idempotent sinks (deterministic alert ids, conditional DynamoDB writes, partition-replace loads). I tested redelivery. (`DECISION_LOG.md` D0.5, D3.2)
- **Why a stateful state machine instead of a windowed filter?** A threshold on one reading flaps; hysteresis, debounce and escalation tiers need per-trip state. (D0.3, D2.1)
- **How did you choose the watermark / expiry?** Trip updates carry no per-trip timestamp, so the lag report cannot size the watermark for the alert path (it stays at 2 minutes). Alert expiry came from measuring gaps between a trip's appearances. (Phase 8, 8b)
- **What was the hardest bug?** My own API overloading the shared emulator: abandoned requests kept running server-side, queries slowed down, more piled up. Diagnosed from open connections and the request mix; fixed with a single-flight cache. (Phase 8c, D8.12)
- **How do you know the numbers are right?** Independent recomputation from raw Kafka with 0 differences, plus parity tests between Spark and Python for the same rule. What I cannot know is whether NTA's delays match reality.
- **What would you do with another month?** Run Snowflake and Airflow for real, deploy to AWS with Terraform, measure under load, and add multi-day data so peak and off-peak patterns mean something.
