import datetime as dt

import pytest

from gtfsrt.trip_start import has_started, scheduled_start_epoch

UTC = dt.timezone.utc


def utc(y, mo, d, h=0, mi=0, s=0):
    return int(dt.datetime(y, mo, d, h, mi, s, tzinfo=UTC).timestamp())


def test_winter_dublin_time_equals_utc():
    assert scheduled_start_epoch("20261108", "21:35:00") == utc(2026, 11, 8, 21, 35)


def test_summer_dublin_time_is_one_hour_ahead_of_utc():
    # 2026-07-01 08:00 in Dublin (IST, UTC+1) is 07:00 UTC: getting this wrong shifts every summer reading by an hour
    assert scheduled_start_epoch("20260701", "08:00:00") == utc(2026, 7, 1, 7, 0)


def test_gtfs_hours_past_24_run_into_the_next_calendar_day_of_the_service_day():
    assert scheduled_start_epoch("20261108", "25:10:00") == utc(2026, 11, 9, 1, 10)


@pytest.mark.parametrize("d,t", [(None, "08:00:00"), ("20261108", None), ("20261108", ""), ("20261108", "8am"),
                                 ("2026-11-08", "08:00:00"), ("20261308", "08:00:00"), ("20261108", "08:00")])
def test_unparseable_input_is_none_never_guessed(d, t):
    assert scheduled_start_epoch(d, t) is None


def test_has_started_boundary_is_inclusive_and_unknown_means_keep():
    start = utc(2026, 11, 8, 21, 35)
    assert has_started(start, "20261108", "21:35:00") is True          # exactly at departure counts as started
    assert has_started(start - 1, "20261108", "21:35:00") is False     # one second before: not started
    assert has_started(start - 3600, "20261108", None) is True          # cannot tell -> do not throw the reading away
    assert has_started(0, "garbage", "garbage") is True
