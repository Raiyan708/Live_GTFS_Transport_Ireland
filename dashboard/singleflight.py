"""Share ONE slow lookup between many concurrent callers.

Why this exists (a real incident, 2026-11-08): the dashboard polls the alerts store every few seconds from every open
tab. When the store (a single-threaded emulator) slowed down, each poll took longer than the poll interval; the browser
abandoned the request but the server kept running it, so every new poll added another in-flight query. The extra load
slowed the store further, and the pile-up starved the Spark jobs sharing it for ~20 minutes.

Rules here:
  * at most ONE load runs at a time; other callers never start their own;
  * a fresh value is served without touching the store;
  * while a refresh is running, callers get the last value immediately (they do not queue behind it);
  * only the very first caller waits (there is nothing to serve yet);
  * a failure is remembered for the TTL too, so a dead store is asked at most once per TTL, not once per request.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Callable


class SingleFlightCache:
    def __init__(self, ttl: float, clock: Callable[[], float] = time.monotonic):
        self.ttl, self._clock = ttl, clock
        self._lock = threading.Lock()
        self._value: Any = None
        self._has_value = False
        self._error: Exception | None = None
        self._at = float("-inf")

    def _fresh(self) -> bool:
        return self._clock() - self._at < self.ttl

    def get(self, loader: Callable[[], Any]) -> tuple[Any, Exception | None]:
        """-> (last good value or None, the error of the most recent load or None)."""
        if self._fresh():
            return self._value, self._error
        # Stale (or never loaded). The first-ever caller must wait for a value; everyone else gets the stale copy at once.
        if not self._lock.acquire(blocking=not self._has_value):
            return self._value, self._error
        try:
            if self._fresh():  # someone finished while we waited for the lock
                return self._value, self._error
            try:
                self._value, self._has_value, self._error = loader(), True, None
            except Exception as exc:  # keep serving the last good value, but tell the caller it is stale
                self._error = exc
            self._at = self._clock()
            return self._value, self._error
        finally:
            self._lock.release()
