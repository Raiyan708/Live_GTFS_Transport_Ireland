import os

import requests
from google.transit import gtfs_realtime_pb2


def fetch_feed(url: str, api_key: str, api_key_header: str) -> gtfs_realtime_pb2.FeedMessage:
    response = requests.get(url, headers={api_key_header: api_key}, timeout=10)
    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    feed = fetch_feed(
        url=os.environ["NTA_VEHICLE_POSITIONS_URL"],
        api_key=os.environ["NTA_API_KEY"],
        api_key_header=os.environ["NTA_API_KEY_HEADER"],
    )

    print(f"feed timestamp: {feed.header.timestamp}")
    print(f"entity count: {len(feed.entity)}")
    print(f"first entity:\n{feed.entity[0]}")
