// Copy for the About panel. Every number here was MEASURED on the live NTA feed on 2026-11-08 (see docs/DECISION_LOG.md,
// Phases 8, 8b, 8c). If you change the pipeline, re-measure before editing these.

export const REPO_URL = "https://github.com/Raiyan708/Live_GTFS_Transport_Ireland";

export const FACTS: { big: string; text: string }[] = [
  { big: "0 differences", text: "Spark's 5-minute route aggregates were recomputed independently from raw Kafka data: 4,599 route-windows compared, none differed." },
  { big: "40.7%", text: "of trip-update readings are for trips that have not started yet, and 89% of those are placeholder zeros. Excluding them moves the true late share from 19% to 30%." },
  { big: "45% → 0.4%", text: "Alerts for trips with no vehicle flipped in bulk (up to 45% in one step); alerts backed by a vehicle position changed 0.4% per step. The default view shows only the stable ones." },
  { big: "10 of 18", text: "extreme delays (90+ min) have a delay that grows exactly 1:1 with the clock: those trips are not progressing (never started, or stuck). They are flagged STALLED instead of looking like ordinary critical delays." },
  { big: "99.8%", text: "of trips appear in back-to-back feed snapshots, so alerts now expire after 10 minutes of silence (it was 30). Chosen from the data, not guessed." },
  { big: "267 tests", text: "across the alert state machine, Lambda, Spark transforms, the API and the UI; the risky parts were also mutation-tested (break the code on purpose, confirm a test fails)." },
];

export const BUGS_FOUND = [
  "Expired alerts stayed 'active' forever when the expiry carried the same timestamp as the alert's start.",
  "Alert delays went stale: a top-tier alert gets no further events, so a bus getting later kept its old number.",
  "GPS fixes near (0, 0) slipped past an exact-zero check and would have plotted buses in the Atlantic.",
  "My own dashboard API snowballed the single-threaded storage emulator and starved the Spark jobs for ~20 minutes (fixed with a single-flight cache and timeouts).",
];

export const LIMITS = [
  "Snowflake and the Airflow DAG have not run against real services. The six warehouse views were checked for logic on a second SQL engine (DuckDB) against pandas, but Snowflake itself has not accepted the SQL yet.",
  "AWS services run on a local emulator; the Terraform validates but has not been applied.",
  "The recording covers one evening. Peak-versus-off-peak patterns need days of data.",
  "Delays are NTA's numbers, sampled every 2 minutes (one API call per minute is allowed). I have not compared them with ground truth.",
];

export const STACK = ["Python", "Apache Kafka", "Spark Structured Streaming", "AWS Lambda / DynamoDB / S3 / Glue", "Snowflake", "Airflow", "Terraform", "FastAPI", "React", "TypeScript", "MapLibre", "Docker"];
