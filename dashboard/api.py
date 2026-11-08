"""HTTP API for the React dashboard in web/.

    PYTHONPATH=src:. uvicorn dashboard.api:app --port 8000

  GET /api/health         liveness + how fresh the data is
  GET /api/vehicles       one snapshot: every vehicle's latest position + trip delay, plus headline stats
  GET /api/stream         Server-Sent Events: a new snapshot whenever the board changed (browser reconnects itself)
  GET /api/alerts/active  hot path (DynamoDB); {"available": false} instead of an error if it is unreachable

No business logic lives here: it only serialises VehicleBoard / views.py. SSE rather than WebSocket because the data
flows one way (server -> browser), EventSource reconnects automatically, and it passes through plain HTTP proxies.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from . import analytics
from .alerts import build_active_alerts
from .singleflight import SingleFlightCache
from .vehicles import KafkaTail, VehicleBoard, summarize

ANALYTICS_CACHE_SECONDS = 60.0  # the cold path changes every few minutes; do not re-read Parquet per request
ALERTS_CACHE_SECONDS = 5.0      # N open tabs must cost ONE store query per 5 s, not N (see singleflight.py)
PUSH_CHECK_SECONDS = 2.0     # how often a connected browser's loop looks for a new version
HEARTBEAT_SECONDS = 15.0     # keeps idle connections from being closed by proxies


def payload(board: VehicleBoard, now: float | None = None) -> dict:
    now = time.time() if now is None else now
    vehicles = board.snapshot(now)
    return {"version": board.version, "server_time": int(now), "stats": summarize(vehicles), "vehicles": vehicles}


def load_routes_file(path: str | Path | None = None) -> dict | None:
    """routes.json built by `python -m gtfsrt.static_gtfs`. None if absent/corrupt: names are a nicety, never a crash."""
    p = Path(path or os.getenv("ROUTES_JSON", "data/gtfs_static/routes.json"))
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data.get("routes"), dict) else None
    except (OSError, ValueError, AttributeError):
        return None


def create_app(
    board: VehicleBoard | None = None,
    start_tail: bool | None = None,
    routes: dict | None = None,
    windows_loader: Callable[[], Any] | None = None,
) -> FastAPI:
    board = board or VehicleBoard()
    windows_cache = SingleFlightCache(ANALYTICS_CACHE_SECONDS)
    alerts_cache = SingleFlightCache(ALERTS_CACHE_SECONDS)
    routes_data = routes if routes is not None else load_routes_file()
    # Tests pass start_tail=False; the real app tails Kafka unless VEHICLE_SOURCE says otherwise.
    start_tail = (os.getenv("VEHICLE_SOURCE", "kafka") == "kafka") if start_tail is None else start_tail
    tail: KafkaTail | None = None

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        nonlocal tail
        if start_tail:
            from gtfsrt.config import Topics

            t = Topics.from_env()
            tail = KafkaTail(board, os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                             t.vehicle_positions, t.trip_updates)
            tail.start()
        yield
        if tail is not None:
            tail.stop_event.set()

    app = FastAPI(title="GTFS-R dashboard API", lifespan=lifespan)

    @app.get("/api/health")
    def health() -> dict:
        last = board.last_update_epoch
        return {"status": "degraded" if (tail is not None and tail.error) else "ok",
                "error": tail.error if tail is not None else None,
                "vehicles": len(board.snapshot(time.time())),
                "rejected_bad_coordinates": board.rejected_out_of_bounds,
                "seconds_since_last_event": None if last is None else int(time.time() - last)}

    @app.get("/api/vehicles")
    def vehicles() -> dict:
        return payload(board)

    @app.get("/api/routes", response_class=JSONResponse)
    def route_names() -> JSONResponse:
        """route_id -> names. Static for a whole schedule period, so the browser may cache it for an hour."""
        body = {"available": routes_data is not None, **(routes_data or {"routes": {}, "feed": {}, "attribution": None})}
        return JSONResponse(body, headers={"Cache-Control": "public, max-age=3600"})

    @app.get("/api/stream")
    async def stream(request: Request) -> StreamingResponse:
        async def events():
            sent_version, last_sent = -1, 0.0
            while not await request.is_disconnected():
                if board.version != sent_version:
                    sent_version = board.version
                    last_sent = time.monotonic()
                    yield f"event: snapshot\ndata: {json.dumps(payload(board), separators=(',', ':'))}\n\n"
                elif time.monotonic() - last_sent > HEARTBEAT_SECONDS:
                    last_sent = time.monotonic()
                    yield ": ping\n\n"
                await asyncio.sleep(PUSH_CHECK_SECONDS)

        return StreamingResponse(events(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    def load_windows():
        """Cold-path windows via the single-flight cache. -> (DataFrame | None, error | None)."""
        def loader():
            fn = windows_loader
            if fn is None:
                from . import sources

                fn = sources.route_windows
            return fn()

        return windows_cache.get(loader)

    @app.get("/api/analytics/reliability")
    def reliability(
        hours: int = Query(24, ge=1, le=24 * 30),
        min_obs: int = Query(100, ge=1, le=1_000_000),
        min_trips: int = Query(3, ge=1, le=1000),
        limit: int = Query(15, ge=1, le=50),
    ) -> dict:
        df, err = load_windows()
        if df is None:  # lake / Snowflake unreachable and nothing cached yet
            return {"available": False, "reason": type(err).__name__ if err else "Loading", "has_data": False, "coverage": None,
                    "ranking": [], "heatmap": {"hours": analytics.HOURS, "routes": []}}
        # `stale`: the last refresh failed, so this is the previous good copy (the UI says so)
        return {"available": True, "stale": err is not None,
                **analytics.reliability(df, time.time(), hours=hours, min_obs=min_obs, min_trips=min_trips, limit=limit)}

    @app.get("/api/alerts/active")
    def active_alerts() -> dict:
        from . import sources

        now = time.time()
        items, err = alerts_cache.get(lambda: sources.active_alerts())
        if items is None:  # store down / no table yet and nothing cached: the UI shows "unavailable", not a stack trace
            return {"available": False, "reason": type(err).__name__ if err else "Loading", "server_time": int(now),
                    "summary": {"active": 0, "by_tier": {0: 0, 1: 0, 2: 0}, "with_vehicle": 0, "unconfirmed": 0, "stalled": 0}, "items": []}
        return {"available": True, "stale": err is not None, "server_time": int(now),
                **build_active_alerts(items, board.positions_by_trip(now), now, board.current_delays(now),
                                      board.stalled_trips(now), board.upcoming_trips(now))}

    return app


app = create_app()
