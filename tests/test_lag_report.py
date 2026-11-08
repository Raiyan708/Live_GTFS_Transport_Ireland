import pytest

from gtfsrt.lag_report import percentile, render, summarize


def rec(event_time=1000, feed=1100, trip="T", date="20261106", route="R1", delay=0, key="event_time"):
    return {key: event_time, "feed_timestamp": feed, "trip_id": trip, "start_date": date,
            "route_id": route, "current_delay_seconds": delay}


def test_percentile_is_nearest_rank_and_returns_an_observed_value():
    v = list(range(1, 101))  # 1..100
    assert (percentile(v, 50), percentile(v, 90), percentile(v, 99), percentile(v, 100)) == (50, 90, 99, 100)
    assert percentile([7], 99) == 7
    with pytest.raises(ValueError):
        percentile([], 50)


def test_lag_and_suggested_watermark():
    rows = [rec(event_time=1000 - lag, feed=1000) for lag in range(0, 100)]  # lags 0..99s
    s = summarize(rows)
    assert s["lag_seconds"]["p99"] == 98 and s["lag_seconds"]["max"] == 99
    assert s["suggested_watermark_seconds"] == 98 + 120  # p99 + one feed cycle


def test_works_with_the_other_producers_field_name():
    s = summarize([rec(key="trip_update_timestamp", event_time=900, feed=1000)])
    assert s["lag_seconds"]["p50"] == 100 and s["records_without_event_time"] == 0


def test_zero_or_missing_event_time_is_unusable_not_a_huge_lag():
    # protobuf's default 0 must not become a lag of ~1.7 billion seconds
    s = summarize([rec(event_time=0, feed=1_700_000_000), {"feed_timestamp": 5, "trip_id": "T"}])
    assert s["records_without_event_time"] == 2 and s["lag_seconds"] is None


def test_counts_data_quality_problems_and_route_shape():
    s = summarize([rec(trip="", route="5444_77"), rec(date="", route="5444_77"), rec(delay=None, route="46A")])
    assert s["empty_trip_id"] == 1 and s["empty_start_date"] == 1 and s["with_delay_value"] == 2
    assert s["distinct_routes"] == 2 and s["route_id_examples"][0] == "5444_77"


def test_negative_lag_is_reported_as_clock_skew():
    assert summarize([rec(event_time=1100, feed=1000)])["lag_seconds"]["negative"] == 1


def test_empty_input_and_render_do_not_crash():
    s = summarize([])
    assert s["records"] == 0 and "no lag could be computed" in render(s)
    assert "suggested WATERMARK_DELAY" in render(summarize([rec()]))
