import threading
import time

from dashboard.singleflight import SingleFlightCache


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def test_many_concurrent_first_callers_trigger_exactly_one_load():
    """The incident: N pollers hit a slow store at once and each started its own query."""
    calls, gate = [], threading.Event()

    def slow():
        calls.append(1)
        gate.wait(2)
        return "data"

    c = SingleFlightCache(ttl=5)
    results = []
    ts = [threading.Thread(target=lambda: results.append(c.get(slow))) for _ in range(12)]
    [t.start() for t in ts]
    time.sleep(0.2)
    gate.set()
    [t.join() for t in ts]
    assert len(calls) == 1 and results == [("data", None)] * 12


def test_fresh_value_is_served_without_reloading_and_expires_after_the_ttl():
    clk, calls = Clock(), []
    c = SingleFlightCache(ttl=5, clock=clk)
    load = lambda: calls.append(1) or len(calls)
    assert c.get(load) == (1, None)
    clk.t = 4.9
    assert c.get(load) == (1, None) and len(calls) == 1
    clk.t = 5.1
    assert c.get(load) == (2, None)


def test_a_failed_load_is_remembered_so_a_dead_store_is_not_hammered():
    clk, calls = Clock(), []

    def boom():
        calls.append(1)
        raise ConnectionError("store down")

    c = SingleFlightCache(ttl=5, clock=clk)
    v, err = c.get(boom)
    assert v is None and isinstance(err, ConnectionError)
    clk.t = 3
    c.get(boom)
    c.get(boom)
    assert len(calls) == 1  # three requests in 3 s, one attempt
    clk.t = 6
    c.get(boom)
    assert len(calls) == 2


def test_a_failure_after_a_success_keeps_serving_the_last_good_value_alongside_the_error():
    clk = Clock()
    c = SingleFlightCache(ttl=5, clock=clk)
    c.get(lambda: "good")
    clk.t = 6

    def boom():
        raise TimeoutError("slow")

    value, err = c.get(boom)
    assert value == "good" and isinstance(err, TimeoutError)  # the caller can show data AND say it is stale


def test_callers_during_a_refresh_get_the_stale_value_immediately_instead_of_queueing():
    clk, started, release = Clock(), threading.Event(), threading.Event()
    c = SingleFlightCache(ttl=5, clock=clk)
    c.get(lambda: "old")
    clk.t = 6

    def slow_refresh():
        started.set()
        release.wait(2)
        return "new"

    t = threading.Thread(target=lambda: c.get(slow_refresh))
    t.start()
    assert started.wait(2)
    t0 = time.monotonic()
    assert c.get(lambda: "should-not-run") == ("old", None)  # served at once from the stale copy
    assert time.monotonic() - t0 < 0.5
    release.set()
    t.join()
    assert c.get(lambda: "x") == ("new", None)
