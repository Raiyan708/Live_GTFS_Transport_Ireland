"""AWS Lambda: Kafka (MSK) alerts topic  ->  DynamoDB (+ SNS notification).

Trigger: MSK / self-managed Kafka event source mapping. Payload shape:
  {"eventSource": "aws:kafka", "records": {"<topic>-<partition>": [
        {"topic", "partition", "offset", "timestamp", "key": <b64>, "value": <b64>}, ...]}}

IMPORTANT property of Kafka event sources: if the handler raises, the WHOLE batch is retried
(there is no per-record partial-failure response like SQS has). Therefore:
  * every operation here must be idempotent, and
  * a malformed record must never raise (poison-pill protection) -> counted + skipped.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

NOTIFY_TYPES = {"DELAY_STARTED", "DELAY_ESCALATED", "DELAY_RECOVERED"}  # EXPIRED is housekeeping, not news
RESOLVING_TYPES = {"DELAY_RECOVERED", "DELAY_EXPIRED"}
EVENT_TTL_SECONDS = 7 * 24 * 3600
RESOLVED_TTL_SECONDS = 3600  # keep "recently resolved" visible for an hour

REQUIRED_FIELDS = ("alert_id", "alert_type", "trip_key", "event_time")


@dataclass
class Stats:
    received: int = 0
    malformed: int = 0
    duplicates: int = 0
    new: int = 0
    notified: int = 0
    stale_state_updates: int = 0

    def as_dict(self) -> dict[str, int]:
        return self.__dict__.copy()


def _is_conditional_failure(exc: ClientError) -> bool:
    return exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException"


@dataclass
class AlertProcessor:
    events_table: Any  # boto3 Table
    active_table: Any  # boto3 Table
    sns: Any  # boto3 SNS client
    topic_arn: str | None
    now: Callable[[], float] = time.time
    stats: Stats = field(default_factory=Stats)

    # -- decoding --------------------------------------------------------
    @staticmethod
    def decode(record: dict) -> dict | None:
        try:
            alert = json.loads(base64.b64decode(record["value"]))
            if not isinstance(alert, dict) or any(alert.get(f) in (None, "") for f in REQUIRED_FIELDS):
                return None
            return alert
        except Exception:
            return None

    # -- idempotent ledger + outbox-lite ---------------------------------
    def _record_event(self, alert: dict) -> bool:
        """Returns True if this is the first time we see alert_id."""
        item = {
            **alert,
            "event_time": int(alert["event_time"]),
            "notified": False,
            "ttl": int(self.now()) + EVENT_TTL_SECONDS,
        }
        try:
            self.events_table.put_item(Item=item, ConditionExpression="attribute_not_exists(alert_id)")
            return True
        except ClientError as exc:
            if _is_conditional_failure(exc):
                return False
            raise

    def _already_notified(self, alert_id: str) -> bool:
        resp = self.events_table.get_item(Key={"alert_id": alert_id}, ConsistentRead=True)
        return bool(resp.get("Item", {}).get("notified"))

    def _notify(self, alert: dict) -> None:
        if not self.topic_arn or alert["alert_type"] not in NOTIFY_TYPES:
            return
        self.sns.publish(
            TopicArn=self.topic_arn,
            Subject=f"{alert['alert_type']} route {alert.get('route_id') or '?'}"[:100],
            Message=json.dumps(alert),
            MessageAttributes={  # lets subscribers filter by route / type without parsing the body
                "alert_type": {"DataType": "String", "StringValue": alert["alert_type"]},
                "route_id": {"DataType": "String", "StringValue": str(alert.get("route_id") or "unknown")},
            },
        )
        self.events_table.update_item(
            Key={"alert_id": alert["alert_id"]},
            UpdateExpression="SET notified = :t",
            ExpressionAttributeValues={":t": True},
        )
        self.stats.notified += 1

    # -- current-state view ----------------------------------------------
    def _update_active(self, alert: dict) -> None:
        resolved = alert["alert_type"] in RESOLVING_TYPES
        ttl = int(self.now()) + (RESOLVED_TTL_SECONDS if resolved else EVENT_TTL_SECONDS)
        try:
            self.active_table.update_item(
                Key={"trip_key": alert["trip_key"]},
                # Only apply if newer than what we hold -> out-of-order delivery cannot roll state back.
                ConditionExpression="attribute_not_exists(event_time) OR event_time < :et",
                UpdateExpression=(
                    "SET #s = :s, tier = :tier, delay_seconds = :d, route_id = :r, vehicle_id = :v, "
                    "trip_id = :t, episode = :ep, event_time = :et, alert_type = :at, alert_id = :aid, "
                    "#ttl = :ttl"
                ),
                ExpressionAttributeNames={"#s": "status", "#ttl": "ttl"},
                ExpressionAttributeValues={
                    ":s": "RESOLVED" if resolved else "ACTIVE",
                    ":tier": int(alert.get("tier") or 0),
                    ":d": int(alert.get("delay_seconds") or 0),
                    ":r": alert.get("route_id") or "unknown",
                    ":v": alert.get("vehicle_id") or "unknown",
                    ":t": alert.get("trip_id") or "",
                    ":ep": int(alert.get("episode") or 0),
                    ":et": int(alert["event_time"]),
                    ":at": alert["alert_type"],
                    ":aid": alert["alert_id"],
                    ":ttl": ttl,
                },
            )
        except ClientError as exc:
            if _is_conditional_failure(exc):
                self.stats.stale_state_updates += 1
            else:
                raise

    # -- main ------------------------------------------------------------
    def process_alert(self, alert: dict) -> None:
        first_time = self._record_event(alert)
        if first_time:
            self.stats.new += 1
        else:
            self.stats.duplicates += 1
        # Even for a duplicate we re-check the outbox flag: a previous attempt may have
        # recorded the event but died before notifying. Never lose a notification.
        if first_time or not self._already_notified(alert["alert_id"]):
            self._notify(alert)
        self._update_active(alert)  # idempotent + order-safe, so always safe to apply

    def process_event(self, event: dict) -> dict[str, int]:
        for records in (event.get("records") or {}).values():
            for record in records:
                self.stats.received += 1
                alert = self.decode(record)
                if alert is None:
                    self.stats.malformed += 1
                    log.error("malformed alert skipped topic=%s partition=%s offset=%s",
                              record.get("topic"), record.get("partition"), record.get("offset"))
                    continue
                self.process_alert(alert)
        return self.stats.as_dict()


# --------------------------------------------------------------------------
# Lambda entrypoint. Clients are created once per container (warm start reuse).
# --------------------------------------------------------------------------
_processor_factory: Callable[[], AlertProcessor] | None = None


def _default_factory() -> AlertProcessor:
    import boto3

    from .tables import ACTIVE_TABLE, EVENTS_TABLE

    endpoint = os.getenv("AWS_ENDPOINT_URL")  # set for the local emulator; unset in real AWS
    kwargs = {"endpoint_url": endpoint} if endpoint else {}
    ddb = boto3.resource("dynamodb", **kwargs)
    return AlertProcessor(
        events_table=ddb.Table(os.getenv("EVENTS_TABLE", EVENTS_TABLE)),
        active_table=ddb.Table(os.getenv("ACTIVE_TABLE", ACTIVE_TABLE)),
        sns=boto3.client("sns", **kwargs),
        topic_arn=os.getenv("ALERT_TOPIC_ARN"),
    )


def lambda_handler(event: dict, context: Any = None) -> dict[str, int]:
    factory = _processor_factory or _default_factory
    processor = factory()  # fresh Stats per invocation
    stats = processor.process_event(event)
    log.info("batch done %s", json.dumps(stats))
    return stats
