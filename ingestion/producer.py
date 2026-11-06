import json

from confluent_kafka import Producer


def make_producer(bootstrap_servers: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
    })


def _delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")


def produce_json(producer: Producer, topic: str, key: str, value: dict) -> None:
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(value).encode("utf-8"),
        callback=_delivery_report,
    )
    producer.poll(0)
