-- ============================================================================
-- 01_storage.sql  -  S3 access. STATUS: NOT EXECUTED [UNVERIFIED]
--
-- Storage INTEGRATION (not keys in the stage): Snowflake assumes an IAM role in YOUR account.
-- Setup is a two-step handshake:
--   1. create the integration here, 2. run DESC INTEGRATION and copy STORAGE_AWS_IAM_USER_ARN and
--   STORAGE_AWS_EXTERNAL_ID into the IAM role's trust policy (see infra/terraform/snowflake_access.tf).
-- ============================================================================
USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS GTFSR_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT_ID>:role/gtfsr-snowflake-read'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<BUCKET>/serving/');   -- ONLY the serving zone, never raw/curated

GRANT USAGE ON INTEGRATION GTFSR_S3_INT TO ROLE GTFSR_LOADER;
DESC INTEGRATION GTFSR_S3_INT;   -- copy the two values mentioned above

USE ROLE GTFSR_LOADER;
USE SCHEMA GTFSR.RAW;

CREATE FILE FORMAT IF NOT EXISTS GTFSR.RAW.PARQUET_FF TYPE = PARQUET;

CREATE STAGE IF NOT EXISTS GTFSR.RAW.SERVING_STAGE
  URL = 's3://<BUCKET>/serving/'
  STORAGE_INTEGRATION = GTFSR_S3_INT
  FILE_FORMAT = GTFSR.RAW.PARQUET_FF;

-- Sanity checks BEFORE loading anything (these catch the two classic problems):
--   LIST @GTFSR.RAW.SERVING_STAGE;                                  -- IAM trust / path correct?
--   SELECT $1 FROM @GTFSR.RAW.SERVING_STAGE/route_delay_5m/ LIMIT 5; -- how do TIMESTAMP columns arrive?
--       (Glue writes TIMESTAMP_MICROS; confirm the VARIANT representation before trusting 02_tables.sql casts)
