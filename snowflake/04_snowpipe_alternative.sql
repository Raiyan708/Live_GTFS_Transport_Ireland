-- ============================================================================
-- 04_snowpipe_alternative.sql  -  NOT WIRED INTO THE PIPELINE (documented alternative). [UNVERIFIED]
--
-- Why it is not the primary path (DECISION_LOG D4.1):
--   Glue overwrites date partitions with NEW file names on every run. Snowpipe (like COPY) dedupes by
--   file name, so each rewrite would be loaded AGAIN -> duplicates. Snowpipe can append, but it cannot
--   express "replace this partition".
--
-- When I WOULD switch to it:
--   if the Glue job emitted immutable, uniquely named, append-only delta files (e.g. only rows newer than a
--   persisted high-water mark). Then Snowpipe = seconds-to-minutes latency, serverless, no Airflow load task.
--   Keep the natural-key dedupe views (QUALIFY) as the safety net either way.
-- ============================================================================
USE ROLE GTFSR_LOADER;
USE SCHEMA GTFSR.RAW;

CREATE PIPE IF NOT EXISTS ROUTE_DELAY_5M_PIPE AUTO_INGEST = TRUE AS
COPY INTO ROUTE_DELAY_5M
FROM (
  SELECT $1:window_start::TIMESTAMP_NTZ, $1:window_end::TIMESTAMP_NTZ, $1:route_id::VARCHAR,
         $1:observations::NUMBER, $1:trips_observed::NUMBER, $1:avg_delay_seconds::FLOAT,
         $1:avg_delay_minutes::FLOAT, $1:max_delay_seconds::NUMBER, $1:p95_delay_seconds::NUMBER,
         $1:late_observations::NUMBER, $1:late_ratio::FLOAT, $1:local_date::DATE, $1:local_hour::NUMBER,
         $1:local_day_of_week::NUMBER, $1:is_peak::BOOLEAN, $1:curated_at::TIMESTAMP_NTZ,
         TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'window_date=([0-9-]+)', 1, 1, 'e', 1)),
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
  FROM @GTFSR.RAW.SERVING_STAGE/route_delay_5m/
)
FILE_FORMAT = (FORMAT_NAME = 'GTFSR.RAW.PARQUET_FF');

-- AUTO_INGEST needs an S3 event notification -> the pipe's SQS queue:
--   SHOW PIPES;  -- copy "notification_channel" (an SQS ARN) into the bucket's event config
--   filter: prefix serving/route_delay_5m/, suffix .parquet   (this is why the Glue job disables _SUCCESS files)
-- Operate it:
--   SELECT SYSTEM$PIPE_STATUS('GTFSR.RAW.ROUTE_DELAY_5M_PIPE');   -- pendingFileCount, lastIngestedTimestamp
