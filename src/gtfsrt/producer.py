"""Feed -> Kafka producer.

Run:  python -m gtfsrt.producer
Modes (FEED_MODE): "live" polls the NTA API; "simulator" generates protobuf locally.
"""
from __future__ import annotations

import json
import logging
import signal
import time
from dataclasses import dataclass
from typing import Any, Protocol

import requests
from google.protobuf.message import DecodeError

from .config import FeedConfig, Topics
from .decode import parse_feed, trip_key, trip_update_events, vehicle_position_events
from .simulator import Simulator

log = logging.getLogger("gtfsrt.producer")


# --------------------------------------------------------------------------
# Pure logic (no I/O) -> unit-testable
# --------------------------------------------------------------------------
@dataclass
class PollResult:
    trip_events: list[dict[str, Any]]
    vehicle_events: list[dict[str, Any]]
    header_ts: int | None
    skipped_duplicate: bool = False
    decode_error: bool = False


def build_events(
    trip_payload: bytes | None,
    vehicle_payload: bytes | None,
    last_header_ts: int | None,
    ingest_time_ms: int,
) -> PollResult:
    """Decode payloads into events; skip the poll if the feed hasn't refreshed.

    The NTA feed refreshes roughly every 30s. If header.timestamp is unchanged we
    would just re-send identical data, so we skip it (cheap idempotency at the edge).
    """
    try:
        trip_events: list[dict[str, Any]] = []
        header_ts: int | None = None
        if trip_payload is not None:
            feed = parse_feed(trip_payload)
            header_ts = int(feed.header.timestamp) or None
            if header_ts is not None and header_ts == last_header_ts:
                return PollResult([], [], header_ts, skipped_duplicate=True)
            trip_events = list(trip_update_events(feed, ingest_time_ms))

        vehicle_events: list[dict[str, Any]] = []
        if vehicle_payload is not None:
            vfeed = parse_feed(vehicle_payload)
            vehicle_events = list(vehicle_position_events(vfeed, ingest_time_ms))
    except DecodeError:
        return PollResult([], [], last_header_ts, decode_error=True)
    return PollResult(trip_events, vehicle_events, header_ts)


# --------------------------------------------------------------------------
# Feed sources
# --------------------------------------------------------------------------
class FeedSource(Protocol):
    def fetch_cycle(self, cycle: int) -> tuple[bytes | None, bytes | None]:
        """Return (trip_updates_payload, vehicles_payload) for this cycle; either may be None."""
        ...


class LiveSource:
    """Polls the NTA GTFS-R API. Backs off on 429 / 5xx."""

    def __init__(self, cfg: FeedConfig):
        if not cfg.api_key:
            raise RuntimeError("FEED_MODE=live requires NTA_API_KEY")
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({cfg.api_key_header: cfg.api_key, "Accept": "application/x-protobuf"})
        self._backoff = 0.0

    def _get(self, url: str) -> bytes | None:
        if self._backoff:
            time.sleep(self._backoff)
        try:
            r = self.session.get(url, timeout=15)
        except requests.RequestException as exc:
            log.warning("feed request failed: %s", exc)
            self._backoff = min(60.0, (self._backoff or 2.0) * 2)
            return None
        if r.status_code in (429, 500, 502, 503, 504):
            self._backoff = min(60.0, (self._backoff or 2.0) * 2)
            log.warning("feed returned %s, backing off %.0fs", r.status_code, self._backoff)
            return None
        if r.status_code != 200:
            log.error("feed returned %s: %s", r.status_code, r.text[:200])
            return None
        self._backoff = 0.0
        return r.content

    def fetch_cycle(self, cycle: int) -> tuple[bytes | None, bytes | None]:
        """ONE API call per cycle, alternating feeds: the fair-usage limit is 1 call / 60 s / token, shared
        across both endpoints. Each feed therefore refreshes every 2 cycles (120 s)."""
        if cycle % 2 == 0:
            return self._get(self.cfg.trip_updates_url), None
        return None, self._get(self.cfg.vehicles_url)


class SimSource:
    def __init__(self, seed: int = 42, n_trips: int = 30):
        self.sim = Simulator(seed=seed, n_trips=n_trips)

    def fetch_cycle(self, cycle: int) -> tuple[bytes | None, bytes | None]:
        now = int(time.time())
        return self.sim.trip_updates_feed(now), self.sim.vehicles_feed(now)


# --------------------------------------------------------------------------
# Kafka I/O
# --------------------------------------------------------------------------
def make_kafka_producer(bootstrap: str):
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap,
            # Wait for all in-sync replicas: no silent loss on leader failover.
            "acks": "all",
            # Broker de-dupes producer retries (producer id + sequence number).
            "enable.idempotence": True,
            # Small batching window: better throughput for ~1ms of latency, irrelevant at 30s polls.
            "linger.ms": 50,
            "compression.type": "lz4",
        }
    )


def _on_delivery(err, msg) -> None:
    if err is not None:
        log.error("delivery failed for key=%s: %s", msg.key(), err)


def publish(producer, topics: Topics, result: PollResult) -> int:
    n = 0
    for ev in result.trip_events:
        producer.produce(
            topics.trip_updates,
            key=trip_key(ev["start_date"], ev["trip_id"]).encode(),
            value=json.dumps(ev, separators=(",", ":")).encode(),
            on_delivery=_on_delivery,
        )
        n += 1
        producer.poll(0)  # serve delivery callbacks, avoid local queue overflow
    for ev in result.vehicle_events:
        producer.produce(
            topics.vehicle_positions,
            key=ev["vehicle_id"].encode(),
            value=json.dumps(ev, separators=(",", ":")).encode(),
            on_delivery=_on_delivery,
        )
        n += 1
        producer.poll(0)
    return n


def main() -> None:  # pragma: no cover - exercised against a live broker
    import os

    try:  # pick up the project's .env (API key etc.) when python-dotenv is installed; values are never logged
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    feed_cfg = FeedConfig.from_env()
    topics = Topics.from_env()
    source: FeedSource = LiveSource(feed_cfg) if feed_cfg.mode == "live" else SimSource()
    producer = make_kafka_producer(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))

    stop = False

    def _stop(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    last_ts: int | None = None
    cycle = 0
    interval = feed_cfg.call_interval_seconds
    log.info("producer started mode=%s call_interval=%ss", feed_cfg.mode, interval)
    while not stop:
        started = time.time()
        trip_payload, vehicle_payload = source.fetch_cycle(cycle)
        cycle += 1
        if trip_payload is None and vehicle_payload is None:
            log.warning("nothing fetched this cycle (request failed or throttled)")
        else:
            res = build_events(trip_payload, vehicle_payload, last_ts, int(started * 1000))
            if res.decode_error:
                log.error("undecodable payload skipped")
            elif res.skipped_duplicate:
                log.info("feed unchanged (ts=%s); skipping", res.header_ts)
            else:
                if res.header_ts is not None:  # vehicle-only cycles carry no trip-feed timestamp
                    last_ts = res.header_ts
                sent = publish(producer, topics, res)
                log.info("published %d events (feed ts=%s)", sent, res.header_ts)
        # sleep the remainder of the interval, waking early on shutdown
        deadline = started + interval
        while not stop and time.time() < deadline:
            time.sleep(0.5)
            producer.poll(0)
    producer.flush(10)
    log.info("producer stopped")


if __name__ == "__main__":
    main()
