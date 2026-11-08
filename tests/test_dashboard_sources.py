import boto3
import pytest
from moto import mock_aws

from dashboard import sources
from gtfsrt.lambda_alerts.tables import ACTIVE_TABLE, create_tables


@pytest.fixture(autouse=True)
def fresh_table_handle(monkeypatch):
    monkeypatch.setattr(sources, "_active_table", None)  # the handle is cached per process: do not leak across tests
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)


def seed(n_active, n_resolved=0):
    create_tables(boto3.client("dynamodb", region_name="eu-west-1"))
    t = boto3.resource("dynamodb", region_name="eu-west-1").Table(ACTIVE_TABLE)
    for i in range(n_active + n_resolved):
        t.put_item(Item={"trip_key": f"20261108|T{i}", "status": "ACTIVE" if i < n_active else "RESOLVED", "tier": 0,
                         "delay_seconds": 400, "event_time": 1000 + i, "route_id": "R", "trip_id": f"T{i}", "episode": 1})


@mock_aws
def test_returns_only_active_alerts_via_the_index():
    seed(n_active=3, n_resolved=2)
    assert sorted(i["trip_id"] for i in sources.active_alerts()) == ["T0", "T1", "T2"]


@mock_aws
def test_table_handle_is_created_once_and_carries_timeouts():
    seed(n_active=1)
    sources.active_alerts()
    first = sources._active_table
    sources.active_alerts()
    assert sources._active_table is first
    cfg = first.meta.client.meta.config
    assert cfg.connect_timeout == 2 and cfg.read_timeout == 5 and cfg.retries["total_max_attempts"] == 1  # first try only: the 5 s cache is the retry


@mock_aws
def test_paging_is_followed_and_then_capped(monkeypatch):
    seed(n_active=5)
    # force one item per page so pagination is genuinely exercised
    table = sources._table()
    real = table.query
    monkeypatch.setattr(table, "query", lambda **kw: real(Limit=1, **kw))
    assert len(sources.active_alerts()) == 5
    monkeypatch.setattr(sources, "MAX_ALERT_PAGES", 3)
    with pytest.raises(RuntimeError, match="refusing to keep paging"):
        sources.active_alerts()
