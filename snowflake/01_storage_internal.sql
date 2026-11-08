-- ============================================================================
-- 01_storage_internal.sql  -  INTERNAL stage: test the whole load path with ONLY a Snowflake account (no AWS).
-- STATUS: NOT EXECUTED [UNVERIFIED]  (run by scripts/snowflake_verify.py)
-- 01_storage.sql is the production variant (S3 + storage integration). The COPY statement is identical; only the
-- stage differs, so a green run here proves the load SQL, the views and the data-quality checks.
-- ============================================================================
USE ROLE ACCOUNTADMIN;
CREATE FILE FORMAT IF NOT EXISTS GTFSR.RAW.PARQUET_FF TYPE = PARQUET;
CREATE STAGE IF NOT EXISTS GTFSR.RAW.SERVING_STAGE_LOCAL FILE_FORMAT = GTFSR.RAW.PARQUET_FF;
GRANT USAGE ON FILE FORMAT GTFSR.RAW.PARQUET_FF TO ROLE GTFSR_LOADER;
GRANT READ, WRITE ON STAGE GTFSR.RAW.SERVING_STAGE_LOCAL TO ROLE GTFSR_LOADER;
