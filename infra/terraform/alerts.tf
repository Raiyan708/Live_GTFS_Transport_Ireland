# ---------------------------------------------------------------- Hot path: DynamoDB + SNS + Lambda
# Table definitions MUST match src/gtfsrt/lambda_alerts/tables.py (a test enforces the names + keys).

resource "aws_dynamodb_table" "alert_events" {
  name         = "gtfsr_alert_events"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "alert_id"

  attribute {
    name = "alert_id"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery { enabled = true }
}

resource "aws_dynamodb_table" "active_alerts" {
  name         = "gtfsr_active_alerts"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "trip_key"

  attribute {
    name = "trip_key"
    type = "S"
  }
  attribute {
    name = "status"
    type = "S"
  }
  attribute {
    name = "event_time"
    type = "N"
  }

  global_secondary_index {
    name            = "status-index"
    hash_key        = "status"
    range_key       = "event_time"
    projection_type = "ALL"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}

resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Where batches go after the event source mapping gives up: a stuck alert must be inspectable, never lost.
resource "aws_sqs_queue" "alerts_dlq" {
  name                      = "${local.name}-alerts-dlq"
  message_retention_seconds = 1209600 # 14 days
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "alerts_lambda" {
  name               = "${local.name}-alerts-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.alerts_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda_access" {
  statement {
    sid       = "Tables"
    actions   = ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.alert_events.arn, aws_dynamodb_table.active_alerts.arn]
  }
  statement {
    sid       = "Notify"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
  statement {
    sid = "ReadMskTopic"
    actions = ["kafka-cluster:Connect", "kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeTopic", "kafka-cluster:ReadData", "kafka-cluster:DescribeClusterDynamicConfiguration",
    "kafka:DescribeCluster", "kafka:GetBootstrapBrokers"]
    resources = ["*"] # scoped further once the cluster/topic/group ARNs are known
  }
  statement {
    sid       = "DeadLetter"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.alerts_dlq.arn]
  }
}

resource "aws_iam_role_policy" "lambda_access" {
  name   = "alerts-access"
  role   = aws_iam_role.alerts_lambda.id
  policy = data.aws_iam_policy_document.lambda_access.json
}

resource "aws_lambda_function" "alerts" {
  function_name = "${local.name}-alerts-consumer"
  role          = aws_iam_role.alerts_lambda.arn
  runtime       = "python3.12"
  handler       = "gtfsrt.lambda_alerts.handler.lambda_handler"
  s3_bucket     = aws_s3_bucket.lake.bucket
  s3_key        = var.lambda_package_key
  timeout       = 30
  memory_size   = 256

  environment {
    variables = {
      ALERT_TOPIC_ARN = aws_sns_topic.alerts.arn
      EVENTS_TABLE    = aws_dynamodb_table.alert_events.name
      ACTIVE_TABLE    = aws_dynamodb_table.active_alerts.name
    }
  }
}

resource "aws_lambda_event_source_mapping" "alerts" {
  function_name                      = aws_lambda_function.alerts.arn
  event_source_arn                   = var.msk_cluster_arn
  topics                             = [var.alerts_topic]
  starting_position                  = "TRIM_HORIZON"
  batch_size                         = 100
  maximum_batching_window_in_seconds = 2 # alerts are latency-sensitive: do not wait long to fill a batch

  # A poison batch must not block its partition forever.
  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.alerts_dlq.arn
    }
  }
}

output "lake_bucket" { value = aws_s3_bucket.lake.bucket }
output "alerts_topic_arn" { value = aws_sns_topic.alerts.arn }
output "glue_job_name" { value = aws_glue_job.curate.name }
output "snowflake_role_arn" {
  value = length(aws_iam_role.snowflake_read) == 0 ? null : aws_iam_role.snowflake_read[0].arn
}
