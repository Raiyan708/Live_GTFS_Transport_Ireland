from google.transit import gtfs_realtime_pb2


def vehicle_position_to_json(entity: gtfs_realtime_pb2.FeedEntity, feed_timestamp: int) -> dict:
    vp = entity.vehicle
    return {
        "vehicle_id": vp.vehicle.id,
        "trip_id": vp.trip.trip_id,
        "route_id": vp.trip.route_id,
        "direction_id": vp.trip.direction_id,
        "start_date": vp.trip.start_date,
        "start_time": vp.trip.start_time,
        "latitude": vp.position.latitude,
        "longitude": vp.position.longitude,
        "bearing": vp.position.bearing,
        "vehicle_timestamp": vp.timestamp,
        "feed_timestamp": feed_timestamp,
    }


def trip_update_to_json(entity: gtfs_realtime_pb2.FeedEntity, feed_timestamp: int) -> dict:
    tu = entity.trip_update
    return {
        "vehicle_id": tu.vehicle.id,
        "trip_id": tu.trip.trip_id,
        "route_id": tu.trip.route_id,
        "direction_id": tu.trip.direction_id,
        "start_date": tu.trip.start_date,
        "start_time": tu.trip.start_time,
        "stop_time_updates": [
            {
                "stop_sequence": stu.stop_sequence,
                "stop_id": stu.stop_id,
                "arrival_delay": stu.arrival.delay,
                "departure_delay": stu.departure.delay,
            }
            for stu in tu.stop_time_update
        ],
        "trip_update_timestamp": tu.timestamp,
        "feed_timestamp": feed_timestamp,
    }
