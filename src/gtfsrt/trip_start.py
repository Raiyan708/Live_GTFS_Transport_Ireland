"""Has this trip actually started yet?

Why it matters (measured on the live feed, 2026-11-08, 249k trip-update readings): 40.7% of readings are for trips whose
scheduled start is still in the FUTURE (they appear a median 40 min, up to 2 h, before departure). 88.6% of those say
"delay = 0" and only 2.6% are late: they are timetable placeholders, not measurements. Counting them dilutes every
"late share" (19.3% overall vs 30.1% among trips that have started). Reliability statistics should only use readings
from trips that have started.

Convention: scheduled start = midnight of the service day, Europe/Dublin wall clock, + the HH:MM:SS offset. GTFS allows
hours >= 24 (a 25:10:00 trip runs after midnight of its service day). On the two DST-change days a year the GTFS
reference ("noon minus 12 h") differs from midnight by one hour; that edge is ignored on purpose.
The Spark twin of this function lives in streaming/common.py; tests/spark checks that the two agree.
"""
from __future__ import annotations

import datetime as dt
try:
    from zoneinfo import ZoneInfo
except ImportError:  # Python 3.8: the Spark image
    from backports.zoneinfo import ZoneInfo

DUBLIN = ZoneInfo("Europe/Dublin")


def scheduled_start_epoch(start_date: str | None, start_time: str | None) -> int | None:
    """Epoch seconds of the trip's scheduled start, or None if it cannot be determined (never guessed)."""
    try:
        hh, mm, ss = (int(x) for x in str(start_time).split(":"))
        midnight = dt.datetime.strptime(str(start_date), "%Y%m%d").replace(tzinfo=DUBLIN)
        # Add the offset to the *UTC* instant of local midnight: wall-clock arithmetic across a DST change would be ambiguous.
        return int(midnight.timestamp()) + hh * 3600 + mm * 60 + ss
    except (TypeError, ValueError):
        return None


def has_started(event_time: int, start_date: str | None, start_time: str | None) -> bool:
    """False only when we KNOW the scheduled start is after the reading. Unknown start time -> True (keep the reading)."""
    start = scheduled_start_epoch(start_date, start_time)
    return start is None or event_time >= start
