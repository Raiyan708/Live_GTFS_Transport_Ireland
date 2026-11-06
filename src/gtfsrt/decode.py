"""GTFS-Realtime protobuf  ->  flat, JSON-serialisable events.

Why decode at the edge (producer) rather than in Spark?
  * A malformed payload fails here, at a boundary we control, not inside a
    long-running streaming job.
  * Downstream stages get a simple, versioned JSON contract.
Trade-off: JSON is bulkier than protobuf and has no enforced schema evolution.
A schema registry + Avro would be the production upgrade.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Iterable

from google.transit import gtfs_realtime_pb2 as pb

SCHEMA_VERSION = 1

_TRIP_SCHEDULE_REL = pb.TripDescriptor.ScheduleRelationship
_STOP_SCHEDULE_REL = pb.TripUpdate.StopTimeUpdate.ScheduleRelationship
_VEHICLE_STATUS = pb.VehiclePosition.VehicleStopStatus
_OCCUPANCY = pb.VehiclePosition.OccupancyStatus


def parse_feed(payload: bytes) -> pb.FeedMessage:
    """Parse raw protobuf bytes. Raises google.protobuf.message.DecodeError on garbage."""
    feed = pb.FeedMessage()
    feed.ParseFromString(payload)
    return feed


def trip_key(start_date: str, trip_id: str) -> str:
    """Kafka message key for trip-scoped topics.

    Same trip -> same partition -> per-trip ordering (required by the state machine).
    start_date is included because trip_ids repeat across service days.
    """
    return f"{start_date}|{trip_id}"


def _date_from_epoch(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y%m%d")


def _stop_delay(stu: pb.TripUpdate.StopTimeUpdate) -> int | None:
    """Delay (seconds) of one stop_time_update; arrival preferred over departure."""
    if stu.HasField("arrival") and stu.arrival.HasField("delay"):
        return stu.arrival.delay
    if stu.HasField("departure") and stu.departure.HasField("delay"):
        return stu.departure.delay
    return None


def current_delay_seconds(tu: pb.TripUpdate) -> int | None:
    """The single number the alerting logic cares about.

    Order of preference (see DECISION_LOG D1.3):
      1. canceled trip            -> None (a cancelled trip is not "0 seconds late")
      2. trip-level `delay` field -> use it
      3. first stop_time_update that carries a delay (the next upcoming stop)
      4. otherwise                -> None
    """
    if tu.trip.schedule_relationship == _TRIP_SCHEDULE_REL.CANCELED:
        return None
    if tu.HasField("delay"):
        return tu.delay
    for stu in tu.stop_time_update:
        d = _stop_delay(stu)
        if d is not None:
            return d
    return None


def trip_update_events(
    feed: pb.FeedMessage, ingest_time_ms: int | None = None
) -> Iterable[dict[str, Any]]:
    """Yield one event per TripUpdate entity in the feed."""
    ingest_time_ms = ingest_time_ms or int(time.time() * 1000)
    header_ts = int(feed.header.timestamp) or int(ingest_time_ms / 1000)

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip = tu.trip
        if not trip.trip_id:
            continue  # cannot key or order it; drop (counted by the caller via len diff)

        event_time = int(tu.timestamp) or header_ts
        start_date = trip.start_date or _date_from_epoch(event_time)

        stus = []
        for stu in tu.stop_time_update:
            stus.append(
                {
                    "stop_id": stu.stop_id or None,
                    "stop_sequence": stu.stop_sequence if stu.HasField("stop_sequence") else None,
                    "arrival_delay": stu.arrival.delay if stu.HasField("arrival") and stu.arrival.HasField("delay") else None,
                    "departure_delay": stu.departure.delay if stu.HasField("departure") and stu.departure.HasField("delay") else None,
                    "schedule_relationship": _STOP_SCHEDULE_REL.Name(stu.schedule_relationship),
                }
            )
        first = stus[0] if stus else {}

        yield {
            "schema_version": SCHEMA_VERSION,
            "entity_id": entity.id,
            "trip_id": trip.trip_id,
            "route_id": trip.route_id or None,
            "start_date": start_date,
            "start_time": trip.start_time or None,
            "direction_id": trip.direction_id if trip.HasField("direction_id") else None,
            "vehicle_id": tu.vehicle.id or None,
            "schedule_relationship": _TRIP_SCHEDULE_REL.Name(trip.schedule_relationship),
            "feed_timestamp": header_ts,
            "event_time": event_time,
            "ingest_time_ms": ingest_time_ms,
            "current_delay_seconds": current_delay_seconds(tu),
            "next_stop_id": first.get("stop_id"),
            "next_stop_sequence": first.get("stop_sequence"),
            "stop_time_updates": stus,
        }


def vehicle_position_events(
    feed: pb.FeedMessage, ingest_time_ms: int | None = None
) -> Iterable[dict[str, Any]]:
    """Yield one event per VehiclePosition entity (kept for the geospatial extension)."""
    ingest_time_ms = ingest_time_ms or int(time.time() * 1000)
    header_ts = int(feed.header.timestamp) or int(ingest_time_ms / 1000)

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        vehicle_id = v.vehicle.id or entity.id
        if not vehicle_id or not v.HasField("position"):
            continue
        event_time = int(v.timestamp) or header_ts
        yield {
            "schema_version": SCHEMA_VERSION,
            "entity_id": entity.id,
            "vehicle_id": vehicle_id,
            "trip_id": v.trip.trip_id or None,
            "route_id": v.trip.route_id or None,
            "start_date": v.trip.start_date or _date_from_epoch(event_time),
            "latitude": v.position.latitude,
            "longitude": v.position.longitude,
            "bearing": v.position.bearing if v.position.HasField("bearing") else None,
            "speed_mps": v.position.speed if v.position.HasField("speed") else None,
            "current_stop_sequence": v.current_stop_sequence if v.HasField("current_stop_sequence") else None,
            "stop_id": v.stop_id or None,
            "current_status": _VEHICLE_STATUS.Name(v.current_status) if v.HasField("current_status") else None,
            "occupancy_status": _OCCUPANCY.Name(v.occupancy_status) if v.HasField("occupancy_status") else None,
            "feed_timestamp": header_ts,
            "event_time": event_time,
            "ingest_time_ms": ingest_time_ms,
        }
