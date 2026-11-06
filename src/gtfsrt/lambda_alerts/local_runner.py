"""Local stand-in for the MSK -> Lambda event source mapping.

Polls the alerts topic, wraps records in the shape AWS delivers to a Kafka-triggered Lambda, calls the SAME
`lambda_handler`, and commits offsets only AFTER it succeeds. If the handler raises, the batch is retried
(never skipped) - mirroring the real mapping, and exactly why the handler must be idempotent.

    PYTHONPATH=src AWS_ENDPOINT_URL=http://localhost:5555 python -m gtfsrt.lambda_alerts.local_runner [--once]
"""
from __future__ import annotations

import argparse
import base64
import logging
import os
import time
from collections import defaultdict

from .handler import lambda_handler

log = logging.getLogger("gtfsrt.lambda_runner")


def to_lambda_event(messages) -> dict:
    records = defaultdict(list)
    for m in messages:
        records[f"{m.topic()}-{m.partition()}"].append({
            "topic": m.topic(), "partition": m.partition(), "offset": m.offset(),
            "timestamp": m.timestamp()[1], "timestampType": "CREATE_TIME",
            "key": base64.b64encode(m.key() or b"").decode(),
            "value": base64.b64encode(m.value() or b"").decode(),
        })
    return {"eventSource": "SelfManagedKafka", "records": dict(records)}


def main() -> None:  # pragma: no cover - needs a broker
    from confluent_kafka import Consumer

    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="drain the topic then exit (for scripts/tests)")
    ap.add_argument("--batch", type=int, default=100)
    ap.add_argument("--group", default="lambda-alerts-local")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # we commit only after the handler succeeds
    })
    consumer.subscribe([os.getenv("TOPIC_DELAY_ALERTS", "gtfsr.delay_alerts")])

    idle = 0
    try:
        while True:
            msgs = [m for m in consumer.consume(args.batch, timeout=2.0) if m.error() is None]
            if not msgs:
                idle += 1
                if args.once and idle >= 3:
                    break
                continue
            idle = 0
            event = to_lambda_event(msgs)
            for attempt in range(1, 6):
                try:
                    stats = lambda_handler(event, None)
                    break
                except Exception as exc:  # whole batch is retried, like the real event source mapping
                    log.error("handler failed (attempt %d): %s", attempt, exc)
                    time.sleep(min(2 ** attempt, 20))
            else:
                raise SystemExit("handler kept failing; not committing offsets")
            consumer.commit(asynchronous=False)
            log.info("batch of %d committed: %s", len(msgs), stats)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
