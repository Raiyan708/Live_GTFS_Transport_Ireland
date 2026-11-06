-- ============================================================================
-- 02_tables.sql  -  RAW landing tables. STATUS: NOT EXECUTED [UNVERIFIED]
--
-- Tables mirror serving/ Parquet 1:1, plus lineage columns:
--   _SOURCE_FILE / _SOURCE_ROW  which file+row produced this record (debugging, audit)
--   _LOADED_AT                  when we loaded it
-- Natural keys (documented, NOT enforced - Snowflake does not enforce PK/UNIQUE; we test them in DQ):
--   ROUTE_DELAY_5M : (WINDOW_START, ROUTE_ID)        ALERTS : (ALERT_ID)
-- ============================================================================
USE ROLE GTFSR_LOADER;
USE SCHEMA GTFSR.RAW;

CREATE TABLE IF NOT EXISTS ROUTE_DELAY_5M (
  WINDOW_START        TIMESTAMP_NTZ NOT NULL,   -- UTC
  WINDOW_END          TIMESTAMP_NTZ NOT NULL,
  ROUTE_ID            VARCHAR       NOT NULL,
  OBSERVATIONS        NUMBER(18,0)  NOT NULL,
  TRIPS_OBSERVED      NUMBER(18,0),
  AVG_DELAY_SECONDS   FLOAT,
  AVG_DELAY_MINUTES   FLOAT,
  MAX_DELAY_SECONDS   NUMBER(10,0),
  P95_DELAY_SECONDS   NUMBER(10,0),
  LATE_OBSERVATIONS   NUMBER(18,0),
  LATE_RATIO          FLOAT,
  LOCAL_DATE          DATE,                     -- Europe/Dublin calendar date
  LOCAL_HOUR          NUMBER(2,0),
  LOCAL_DAY_OF_WEEK   NUMBER(1,0),              -- 1=Sunday .. 7=Saturday (Spark convention)
  IS_PEAK             BOOLEAN,
  CURATED_AT          TIMESTAMP_NTZ,
  WINDOW_DATE         DATE          NOT NULL,   -- UTC partition date, parsed from the S3 path
  _SOURCE_FILE        VARCHAR,
  _SOURCE_ROW         NUMBER,
  _LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (WINDOW_DATE);   -- every load/query filters by date; at this size clustering is optional, cheap to declare

CREATE TABLE IF NOT EXISTS ALERTS (
  ALERT_ID            VARCHAR       NOT NULL,
  ALERT_TYPE          VARCHAR       NOT NULL,   -- DELAY_STARTED | DELAY_ESCALATED | DELAY_RECOVERED | DELAY_EXPIRED
  TRIP_KEY            VARCHAR       NOT NULL,
  TRIP_ID             VARCHAR,
  START_DATE          VARCHAR,
  ROUTE_ID            VARCHAR,
  VEHICLE_ID          VARCHAR,
  DELAY_SECONDS       NUMBER(10,0),
  DELAY_MINUTES       FLOAT,
  TIER                NUMBER(2,0),
  THRESHOLD_SECONDS   NUMBER(10,0),
  SEVERITY            VARCHAR,                  -- MODERATE | SEVERE | CRITICAL
  EPISODE             NUMBER(10,0),
  EVENT_TIME          NUMBER(18,0),             -- epoch seconds
  ALERT_TS            TIMESTAMP_NTZ,
  NEXT_STOP_ID        VARCHAR,
  REASON              VARCHAR,
  LOCAL_DATE          DATE,
  LOCAL_HOUR          NUMBER(2,0),
  IS_PEAK             BOOLEAN,
  CURATED_AT          TIMESTAMP_NTZ,
  ALERT_DATE          DATE          NOT NULL,
  _SOURCE_FILE        VARCHAR,
  _SOURCE_ROW         NUMBER,
  _LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (ALERT_DATE);

-- Load audit trail: one row per (table, date) load attempt, written by Airflow. Powers freshness + reconciliation DQ.
CREATE TABLE IF NOT EXISTS LOAD_AUDIT (
  TABLE_NAME     VARCHAR,
  PARTITION_DATE DATE,
  ROWS_LOADED    NUMBER,
  RUN_ID         VARCHAR,
  LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
