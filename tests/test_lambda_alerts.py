import base64
import json

import boto3
import pytest
from moto import mock_aws

from gtfsrt.lambda_alerts.handler import AlertProcessor, lambda_handler
from gtfsrt.lambda_alerts import handler as handler_mod
from gtfsrt.lambda_alerts.tables import ACTIVE_TABLE, EVENTS_TABLE, create_tables


class SpySNS:
    """Records publishes; can be told to fail the next N calls."""

    def __init__(self, fail_times=0):
        self.published, self.fail_times = [], fail_times

    def publish(self, **kw):
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("SNS unavailable")
        self.published.append(kw)


def alert(alert_id="a1", atype="DELAY_STARTED", t=1000, trip="20260101|T1", tier=0, delay=360, episode=1, route="46A"):
    return {"alert_id": alert_id, "alert_type": atype, "trip_key": trip, "trip_id": trip.split("|")[1],
            "start_date": "20260101", "route_id": route, "vehicle_id": "V1", "delay_seconds": delay,
            "tier": tier, "threshold_seconds": 300, "episode": episode, "event_time": t,
            "next_stop_id": "S1", "reason": None}


def record(a, offset=0, raw=None):
    value = raw if raw is not None else json.dumps(a).encode()
    return {"topic": "gtfsr.delay_alerts", "partition": 0, "offset": offset,
            "value": base64.b64encode(value).decode()}


def kafka_event(*records):
    return {"eventSource": "aws:kafka", "records": {"gtfsr.delay_alerts-0": list(records)}}


@pytest.fixture
def env():
    with mock_aws():
        ddb_client = boto3.client("dynamodb", region_name="eu-west-1")
        create_tables(ddb_client)
        ddb = boto3.resource("dynamodb", region_name="eu-west-1")
        yield ddb.Table(EVENTS_TABLE), ddb.Table(ACTIVE_TABLE)


def make(env, sns=None, now=lambda: 1_000_000):
    ev, ac = env
    return AlertProcessor(ev, ac, sns or SpySNS(), "arn:aws:sns:eu-west-1:1:alerts", now=now)


def test_new_alert_is_recorded_notified_and_marks_trip_active(env):
    sns = SpySNS(); p = make(env, sns)
    stats = p.process_event(kafka_event(record(alert())))
    assert stats["new"] == 1 and stats["notified"] == 1 and len(sns.published) == 1
    item = env[1].get_item(Key={"trip_key": "20260101|T1"})["Item"]
    assert item["status"] == "ACTIVE" and item["delay_seconds"] == 360 and item["route_id"] == "46A"
    assert env[0].get_item(Key={"alert_id": "a1"})["Item"]["notified"] is True
    attrs = sns.published[0]["MessageAttributes"]
    assert attrs["route_id"]["StringValue"] == "46A" and attrs["alert_type"]["StringValue"] == "DELAY_STARTED"


def test_redelivered_batch_does_not_notify_twice(env):
    sns = SpySNS(); p = make(env, sns)
    batch = kafka_event(record(alert()), record(alert("a2", "DELAY_ESCALATED", 1030, tier=1, delay=650), 1))
    p.process_event(batch)
    p.process_event(batch)  # Kafka event source retried the whole batch / Spark replayed
    assert len(sns.published) == 2  # still 2, not 4
    assert p.stats.duplicates == 2


def test_failed_publish_is_retried_not_lost(env):
    """The case the outbox flag exists for: event recorded, notification failed, batch redelivered."""
    sns = SpySNS(fail_times=1)
    p = make(env, sns)
    with pytest.raises(RuntimeError):
        p.process_event(kafka_event(record(alert())))  # Lambda errors -> whole batch retried
    assert env[0].get_item(Key={"alert_id": "a1"})["Item"]["notified"] is False  # recorded, not yet sent
    p2 = make(env, sns)  # retry in a new invocation
    p2.process_event(kafka_event(record(alert())))
    assert len(sns.published) == 1  # delivered exactly once in the end
    assert env[0].get_item(Key={"alert_id": "a1"})["Item"]["notified"] is True


def test_out_of_order_alert_cannot_roll_state_back(env):
    p = make(env)
    p.process_event(kafka_event(record(alert("a2", "DELAY_ESCALATED", 2000, tier=1, delay=650))))
    p.process_event(kafka_event(record(alert("a1", "DELAY_STARTED", 1000, tier=0, delay=360))))  # older, arrives late
    item = env[1].get_item(Key={"trip_key": "20260101|T1"})["Item"]
    assert item["tier"] == 1 and item["delay_seconds"] == 650 and item["alert_id"] == "a2"
    assert p.stats.stale_state_updates == 1
    assert env[0].get_item(Key={"alert_id": "a1"}).get("Item")  # but the history still has it


def test_recovery_resolves_active_alert_with_short_ttl(env):
    p = make(env, now=lambda: 5_000)
    p.process_event(kafka_event(record(alert("a1", t=1000)), record(alert("a3", "DELAY_RECOVERED", 3000, delay=60), 1)))
    item = env[1].get_item(Key={"trip_key": "20260101|T1"})["Item"]
    assert item["status"] == "RESOLVED" and int(item["ttl"]) == 5_000 + 3600


def test_expired_is_recorded_and_resolves_but_does_not_notify(env):
    sns = SpySNS(); p = make(env, sns)
    p.process_event(kafka_event(record(alert("a1", t=1000)), record(alert("a4", "DELAY_EXPIRED", 4000), 1)))
    assert [m["MessageAttributes"]["alert_type"]["StringValue"] for m in sns.published] == ["DELAY_STARTED"]
    assert env[1].get_item(Key={"trip_key": "20260101|T1"})["Item"]["status"] == "RESOLVED"


def test_poison_records_are_skipped_not_raised(env):
    sns = SpySNS(); p = make(env, sns)
    stats = p.process_event(kafka_event(
        record(None, 0, raw=b"{not json"),
        record(None, 1, raw=b'{"alert_id": "x"}'),            # missing required fields
        record(None, 2, raw=b'[1,2,3]'),                       # valid JSON, wrong shape
        record(alert(), 3),
    ))
    assert stats["received"] == 4 and stats["malformed"] == 3 and stats["new"] == 1
    assert len(sns.published) == 1


def test_active_alerts_can_be_listed_via_gsi(env):
    p = make(env)
    p.process_event(kafka_event(
        record(alert("a1", trip="20260101|T1", t=1000)),
        record(alert("a2", trip="20260101|T2", t=1100)),
        record(alert("a3", "DELAY_RECOVERED", trip="20260101|T2", t=1200, delay=50), 2),
    ))
    resp = env[1].query(IndexName="status-index", KeyConditionExpression=boto3.dynamodb.conditions.Key("status").eq("ACTIVE"))
    assert [i["trip_key"] for i in resp["Items"]] == ["20260101|T1"]


def test_real_sns_publish_call_shape_is_valid(env):
    """Exercise the real (moto) SNS API so MessageAttributes / Subject constraints are checked."""
    sns = boto3.client("sns", region_name="eu-west-1")
    arn = sns.create_topic(Name="alerts")["TopicArn"]
    ev, ac = env
    p = AlertProcessor(ev, ac, sns, arn, now=lambda: 1)
    stats = p.process_event(kafka_event(record(alert())))
    assert stats["notified"] == 1


def test_lambda_handler_entrypoint_uses_injected_factory(env, monkeypatch):
    sns = SpySNS()
    monkeypatch.setattr(handler_mod, "_processor_factory", lambda: make(env, sns))
    out = lambda_handler(kafka_event(record(alert())), None)
    assert out["new"] == 1 and len(sns.published) == 1
