# ---------------------------------------------------------------- Data lake bucket
resource "aws_s3_bucket" "lake" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Cost control. raw/ is the replay source: keep it, but move it to cheaper storage when cold.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "raw-to-infrequent-access"
    status = "Enabled"
    filter { prefix = "raw/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    filter {} # whole bucket: Spark's S3A writes are multipart; abandoned parts are invisible cost
    abort_incomplete_multipart_upload { days_after_initiation = 2 }
  }
}

# ---------------------------------------------------------------- Glue: catalog + crawler + ETL job
resource "aws_glue_catalog_database" "gtfsr" {
  name        = "${local.name}_lake"
  description = "Curated + serving zones of the GTFS-RT delay pipeline"
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.name}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Least privilege: Glue reads curated/, writes serving/ (+ artifacts read for its own script). Nothing else.
data "aws_iam_policy_document" "glue_s3" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/curated/*", "${aws_s3_bucket.lake.arn}/artifacts/*"]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"] # overwrite = delete+put of partition files
    resources = ["${aws_s3_bucket.lake.arn}/serving/*"]
  }
}

resource "aws_iam_role_policy" "glue_s3" {
  name   = "lake-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_s3.json
}

# Crawler on curated/: discovers partitions and flags schema drift.
# update_behavior = LOG: a changed column type is LOGGED, never silently applied under the ETL job.
resource "aws_glue_crawler" "curated" {
  name          = "${local.name}-curated"
  database_name = aws_glue_catalog_database.gtfsr.name
  role          = aws_iam_role.glue.arn
  table_prefix  = "curated_"

  s3_target {
    path = "s3://${aws_s3_bucket.lake.bucket}/curated/"
    # Spark's file sink keeps a _spark_metadata/ log next to the data: it is not a table.
    exclusions = ["**/_spark_metadata/**"]
  }

  schema_change_policy {
    update_behavior = "LOG"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_NEW_FOLDERS_ONLY" # cheaper, and old partitions are immutable once closed
  }
}

resource "aws_glue_job" "curate" {
  name              = "${local.name}-curate"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0" # Spark 3.3
  worker_type       = "G.1X"
  number_of_workers = 2  # the minimum; this data is small
  timeout           = 20 # minutes: a hung job must not bill for hours
  max_retries       = 0  # Airflow owns retries (one place to reason about them)

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/artifacts/curate_job.py"
  }

  default_arguments = {
    "--extra-py-files"                   = "s3://${aws_s3_bucket.lake.bucket}/artifacts/gtfsrt.zip"
    "--bucket"                           = aws_s3_bucket.lake.bucket
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
  }
}

# ---------------------------------------------------------------- Snowflake reads serving/ via an assumed role
data "aws_iam_policy_document" "snowflake_assume" {
  count = var.snowflake_iam_user_arn == "" ? 0 : 1
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.snowflake_iam_user_arn]
    }
    condition { # external id stops the "confused deputy" problem: only OUR integration can use this role
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.snowflake_external_id]
    }
  }
}

resource "aws_iam_role" "snowflake_read" {
  count              = var.snowflake_iam_user_arn == "" ? 0 : 1
  name               = "${local.name}-snowflake-read"
  assume_role_policy = data.aws_iam_policy_document.snowflake_assume[0].json
}

data "aws_iam_policy_document" "snowflake_s3" {
  statement {
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.lake.arn}/serving/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["serving/*"]
    }
  }
}

resource "aws_iam_role_policy" "snowflake_s3" {
  count  = var.snowflake_iam_user_arn == "" ? 0 : 1
  name   = "serving-read-only"
  role   = aws_iam_role.snowflake_read[0].id
  policy = data.aws_iam_policy_document.snowflake_s3.json
}
