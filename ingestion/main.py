import os
import time

from dotenv import load_dotenv

from ingestion.fetch import fetch_feed
from ingestion.producer import make_producer, produce_json
from ingestion.transform import trip_update_to_json, vehicle_position_to_json

# NTA's fair usage policy: one API call per token every 60 seconds.
# We alternate between the two feeds rather than calling both per cycle,
# so each individual feed refreshes every 120s.
CALL_INTERVAL_SECONDS = 60


def partition_key(record: dict) -> str:
    """Kafka message key: the trip, so all updates for one trip land on one partition in order.

    Phase 2's alert state machine tracks each trip over time, and Kafka only guarantees ordering within a
    partition. start_date is included because trip_ids repeat across service days.
    A vehicle not currently on a trip has an empty trip_id: fall back to vehicle_id so those records
    still spread across partitions instead of all hashing to the same key.
    """
    if record["trip_id"]:
        return f"{record['start_date']}|{record['trip_id']}"
    return record["vehicle_id"] or "unknown"


def poll_and_produce(producer, url, api_key, api_key_header, topic, transform_fn):
    try:
        feed = fetch_feed(url, api_key, api_key_header)
    except Exception as exc:
        print(f"poll failed for {topic}: {exc}")
        return 0

    for entity in feed.entity:
        record = transform_fn(entity, feed.header.timestamp)
        produce_json(producer, topic, key=partition_key(record), value=record)
    return len(feed.entity)


def main():
    load_dotenv()

    api_key = os.environ["NTA_API_KEY"]
    api_key_header = os.environ["NTA_API_KEY_HEADER"]
    vp_url = os.environ["NTA_VEHICLE_POSITIONS_URL"]
    tu_url = os.environ["NTA_TRIP_UPDATES_URL"]

    producer = make_producer("localhost:9092")

    feeds = [
        (vp_url, "vehicle_positions", vehicle_position_to_json),
        (tu_url, "trip_updates", trip_update_to_json),
    ]

    try:
        i = 0
        while True:
            url, topic, transform_fn = feeds[i % len(feeds)]
            count = poll_and_produce(producer, url, api_key, api_key_header, topic, transform_fn)
            print(f"produced {count} records to {topic}")
            i += 1
            time.sleep(CALL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("shutting down, flushing producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
