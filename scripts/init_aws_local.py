"""Create the DynamoDB tables + SNS topic in the local AWS emulator (idempotent).

    PYTHONPATH=src AWS_ENDPOINT_URL=http://localhost:5555 python scripts/init_aws_local.py
"""
import os

import boto3

from gtfsrt.lambda_alerts.tables import create_tables

endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localhost:5555")
common = dict(endpoint_url=endpoint, region_name=os.getenv("AWS_REGION", "eu-west-1"),
              aws_access_key_id="test", aws_secret_access_key="test")

create_tables(boto3.client("dynamodb", **common))
arn = boto3.client("sns", **common).create_topic(Name="gtfsr-alerts")["TopicArn"]  # idempotent by name
print("tables:", boto3.client("dynamodb", **common).list_tables()["TableNames"])
print("ALERT_TOPIC_ARN =", arn)
