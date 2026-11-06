"""DynamoDB table definitions: single source of truth for tests, local init, and docs.

alert_events   PK alert_id            history + idempotency ledger (7-day TTL)
active_alerts  PK trip_key            current state per trip (what a dashboard reads)
               GSI status-index       (status, event_time)  -> "list ACTIVE alerts" without a Scan
"""
from __future__ import annotations

EVENTS_TABLE = "gtfsr_alert_events"
ACTIVE_TABLE = "gtfsr_active_alerts"

EVENTS_TABLE_SPEC = dict(
    TableName=EVENTS_TABLE,
    KeySchema=[{"AttributeName": "alert_id", "KeyType": "HASH"}],
    AttributeDefinitions=[{"AttributeName": "alert_id", "AttributeType": "S"}],
    BillingMode="PAY_PER_REQUEST",  # spiky, low-volume traffic: no capacity planning
)

ACTIVE_TABLE_SPEC = dict(
    TableName=ACTIVE_TABLE,
    KeySchema=[{"AttributeName": "trip_key", "KeyType": "HASH"}],
    AttributeDefinitions=[
        {"AttributeName": "trip_key", "AttributeType": "S"},
        {"AttributeName": "status", "AttributeType": "S"},
        {"AttributeName": "event_time", "AttributeType": "N"},
    ],
    GlobalSecondaryIndexes=[
        {
            "IndexName": "status-index",
            "KeySchema": [
                {"AttributeName": "status", "KeyType": "HASH"},
                {"AttributeName": "event_time", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }
    ],
    BillingMode="PAY_PER_REQUEST",
)


def create_tables(dynamodb_client) -> None:
    """Idempotent create + enable TTL (attribute `ttl`, epoch seconds)."""
    existing = set(dynamodb_client.list_tables()["TableNames"])
    for spec in (EVENTS_TABLE_SPEC, ACTIVE_TABLE_SPEC):
        if spec["TableName"] not in existing:
            dynamodb_client.create_table(**spec)
        try:
            dynamodb_client.update_time_to_live(
                TableName=spec["TableName"],
                TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"},
            )
        except Exception:  # emulators may not implement TTL; real AWS does
            pass
