# Decision Log

## Phase 1 — Ingestion (feed → Kafka)

**WHAT?**
Get NTA's GTFS-Realtime feed (VehiclePositions + TripUpdates) flowing reliably onto Kafka, in a shape Spark can consume in Phase 2, without depending on anything else in the pipeline yet.

**WHY?**
Several forks, decided deliberately rather than defaulted into:

- **Kafka environment:** local Docker Kafka (KRaft mode, real Apache Kafka not Redpanda) for fast iteration, since the same config/CLI transfers to AWS MSK later.
- **Topic design:** two topics (`vehicle_positions`, `trip_updates`), not one — different message shapes, independent scaling/retention.
- **Partition key:** `route_id` — guarantees per-route ordering, which Phase 2's per-route windowed aggregation depends on. Traded off against `vehicle_id`/`trip_id` (better load spread, no per-route ordering) and no key (max throughput, no ordering at all).
- **Message format:** JSON, not raw protobuf passthrough — human-inspectable while learning, no protobuf schema needed on the Spark side.
- **Producer reliability:** `acks=all` + idempotent producer — effectively at-least-once with no duplicate-on-retry risk; end-to-end exactly-once vs. at-least-once semantics revisited properly in Phase 2 once Spark checkpointing is in play.
- **Failure handling:** catch fetch errors, log, skip that cycle, keep the loop alive — rejected both "let it crash" (no supervisor exists yet to restart it) and backoff-on-repeated-failure (unneeded complexity until we see sustained throttling in practice).

**HOW?**
- `docker-compose.yml` — single-broker Apache Kafka 3.7 in KRaft mode, dual listeners (`PLAINTEXT` for other containers, `PLAINTEXT_HOST` for the host).
- Two topics created, 6 partitions each, replication factor 1 (single local broker).
- `ingestion/fetch.py` — polls NTA's GTFS-RT endpoints, parses protobuf `FeedMessage` via `gtfs-realtime-bindings`.
- `ingestion/transform.py` — protobuf → JSON, keeping `stop_time_update` as a list (it's a repeated field — some trips carry delay info for multiple upcoming stops, confirmed from real data).
- `ingestion/producer.py` — idempotent Kafka producer, JSON-serializes and produces keyed by `route_id`; delivery callback logs failures only (successes would flood the console at this volume).
- `ingestion/main.py` — polls the two feeds **alternately**, one call every 60s, because NTA's fair-usage policy allows only one API call per token per 60 seconds (confirmed from the portal's fair-usage policy text — not per-endpoint, shared across both feeds). Each individual feed refreshes every 120s as a result. Graceful shutdown flushes the producer on Ctrl+C.

**RESULT?**
- Verified end-to-end: real feed data → parsed → transformed → produced → confirmed present in Kafka via console consumer, then via a full multi-cycle run of `main.py` (1000+ vehicle positions, 3000+ trip updates per cycle, zero errors after the rate-limit fix).
- What broke along the way: `confluent-kafka` failed to build on Python 3.13/arm64 (no prebuilt wheel yet) until `librdkafka` was installed via Homebrew and `CFLAGS`/`LDFLAGS` pointed at it explicitly. The first version of the poll loop hit NTA's 429 rate limit by calling both feeds back-to-back every 30s — fixed by alternating one call per feed every 60s once the actual fair-usage policy (1 call/60s per token, shared across endpoints) was confirmed from the portal docs rather than assumed.
- What I'd improve later: no backoff yet on sustained failure (currently just skip-and-continue every cycle); `route_id` values look like internal variant-encoded IDs rather than clean route names, which Phase 2's per-route grouping will need to reckon with; no schema validation beyond protobuf's own defaults (e.g. a missing `vehicle_id` silently becomes `""`).
