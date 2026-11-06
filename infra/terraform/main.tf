# AWS resources for the COLD path + alert HOT path.
# STATUS: `terraform validate` passes; NEVER APPLIED (no AWS account used) [UNVERIFIED at runtime].
#
# NOT managed here, on purpose:
#   * Kafka (MSK) cluster  - referenced by ARN (variable). It is expensive and has its own lifecycle.
#   * Spark streaming jobs - run on your compute of choice (EMR Serverless / ECS / EKS); see README.
#   * Snowflake objects    - snowflake/*.sql
# Deploy order: terraform apply  ->  scripts/package_and_upload.sh  ->  start streaming jobs.

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = var.project, managed_by = "terraform" }
  }
}

variable "region" {
  type    = string
  default = "eu-west-1" # Dublin: data stays in-country, lowest latency to the feed
}

variable "project" {
  type    = string
  default = "gtfsr"
}

variable "bucket_name" {
  type        = string
  description = "Globally unique S3 bucket for the lake (raw/, curated/, serving/, artifacts/)."
}

variable "msk_cluster_arn" {
  type        = string
  description = "ARN of the existing MSK cluster the Lambda reads alerts from."
}

variable "alerts_topic" {
  type    = string
  default = "gtfsr.delay_alerts"
}

variable "lambda_package_key" {
  type        = string
  default     = "artifacts/lambda_alerts.zip"
  description = "S3 key (inside the lake bucket) of the Lambda deployment package."
}

variable "snowflake_iam_user_arn" {
  type        = string
  default     = ""
  description = "STORAGE_AWS_IAM_USER_ARN from `DESC INTEGRATION GTFSR_S3_INT`. Empty = skip the Snowflake role."
}

variable "snowflake_external_id" {
  type        = string
  default     = ""
  description = "STORAGE_AWS_EXTERNAL_ID from `DESC INTEGRATION GTFSR_S3_INT`."
}

variable "alert_email" {
  type        = string
  default     = ""
  description = "Optional: subscribe this address to the alerts topic (it must confirm the subscription)."
}

data "aws_caller_identity" "current" {}

locals {
  name = var.project
}
