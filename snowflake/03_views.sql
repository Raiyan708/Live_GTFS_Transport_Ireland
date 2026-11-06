-- ============================================================================
-- 03_views.sql  -  ANALYTICS layer. STATUS: NOT EXECUTED [UNVERIFIED]
-- RAW is the faithful landing zone; consumers only ever see these views.
-- Each view defends against duplicates (QUALIFY) so a bad reload can't inflate dashboards.
-- ============================================================================
USE ROLE GTFSR_LOADER;
USE SCHEMA GTFSR.ANALYTICS;

-- 1. De-duplicated 5-minute route windows ------------------------------------
CREATE OR REPLACE VIEW V_ROUTE_DELAY_5M AS
SELECT *
FROM GTFSR.RAW.ROUTE_DELAY_5M
QUALIFY ROW_NUMBER() OVER (PARTITION BY WINDOW_START, ROUTE_ID ORDER BY _LOADED_AT DESC) = 1;

-- 2. Hourly reliability by route (local time) --------------------------------
-- Averages are OBSERVATION-weighted: avg of averages would over-weight sparse windows.
CREATE OR REPLACE VIEW V_ROUTE_RELIABILITY_HOURLY AS
SELECT
  ROUTE_ID,
  LOCAL_DATE,
  LOCAL_HOUR,
  BOOLOR_AGG(IS_PEAK)                                              AS IS_PEAK,
  SUM(OBSERVATIONS)                                                AS OBSERVATIONS,
  SUM(AVG_DELAY_SECONDS * OBSERVATIONS) / NULLIF(SUM(OBSERVATIONS), 0) AS AVG_DELAY_SECONDS,
  MAX(MAX_DELAY_SECONDS)                                           AS MAX_DELAY_SECONDS,
  SUM(LATE_OBSERVATIONS) / NULLIF(SUM(OBSERVATIONS), 0)            AS LATE_RATIO
FROM V_ROUTE_DELAY_5M
GROUP BY ROUTE_ID, LOCAL_DATE, LOCAL_HOUR;

-- 3. Worst routes in the last 24h (what the dashboard leaderboard reads) ----
CREATE OR REPLACE VIEW V_ROUTE_RANKING_24H AS
SELECT
  ROUTE_ID,
  SUM(OBSERVATIONS)                                                    AS OBSERVATIONS,
  SUM(AVG_DELAY_SECONDS * OBSERVATIONS) / NULLIF(SUM(OBSERVATIONS), 0) AS AVG_DELAY_SECONDS,
  MAX(MAX_DELAY_SECONDS)                                               AS MAX_DELAY_SECONDS,
  SUM(LATE_OBSERVATIONS) / NULLIF(SUM(OBSERVATIONS), 0)                AS LATE_RATIO,
  RANK() OVER (ORDER BY SUM(LATE_OBSERVATIONS) / NULLIF(SUM(OBSERVATIONS), 0) DESC NULLS LAST) AS RELIABILITY_RANK
FROM V_ROUTE_DELAY_5M
WHERE WINDOW_START >= DATEADD('hour', -24, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
GROUP BY ROUTE_ID;

-- 4. One row per delay EPISODE: collapses STARTED/ESCALATED/RECOVERED/EXPIRED events ----
CREATE OR REPLACE VIEW V_ALERT_EPISODES AS
WITH a AS (
  SELECT * FROM GTFSR.RAW.ALERTS
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ALERT_ID ORDER BY _LOADED_AT DESC) = 1
)
SELECT
  TRIP_KEY,
  EPISODE,
  ANY_VALUE(ROUTE_ID)                                                    AS ROUTE_ID,
  MIN(CASE WHEN ALERT_TYPE = 'DELAY_STARTED' THEN ALERT_TS END)          AS STARTED_AT,
  MAX(CASE WHEN ALERT_TYPE IN ('DELAY_RECOVERED','DELAY_EXPIRED') THEN ALERT_TS END) AS ENDED_AT,
  MAX(CASE WHEN ALERT_TYPE = 'DELAY_RECOVERED' THEN 'RECOVERED'
           WHEN ALERT_TYPE = 'DELAY_EXPIRED'   THEN 'EXPIRED' END)       AS ENDED_BY,   -- NULL = still open
  COUNT_IF(ALERT_TYPE = 'DELAY_ESCALATED')                               AS ESCALATIONS,
  MAX(TIER)                                                              AS MAX_TIER,
  MAX(CASE WHEN ALERT_TYPE IN ('DELAY_STARTED','DELAY_ESCALATED') THEN DELAY_MINUTES END) AS PEAK_DELAY_MINUTES,
  DATEDIFF('minute',
           MIN(CASE WHEN ALERT_TYPE = 'DELAY_STARTED' THEN ALERT_TS END),
           MAX(CASE WHEN ALERT_TYPE IN ('DELAY_RECOVERED','DELAY_EXPIRED') THEN ALERT_TS END)) AS DURATION_MINUTES,
  BOOLOR_AGG(IS_PEAK)                                                    AS STARTED_IN_PEAK
FROM a
GROUP BY TRIP_KEY, EPISODE;

-- 5. Alerts per route per local day -------------------------------------------
CREATE OR REPLACE VIEW V_ALERTS_BY_ROUTE_DAY AS
WITH a AS (   -- dedupe BEFORE grouping: QUALIFY cannot see ALERT_ID once rows are grouped
  SELECT * FROM GTFSR.RAW.ALERTS
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ALERT_ID ORDER BY _LOADED_AT DESC) = 1
)
SELECT ROUTE_ID, LOCAL_DATE,
       COUNT_IF(ALERT_TYPE = 'DELAY_STARTED')   AS EPISODES_STARTED,
       COUNT_IF(SEVERITY = 'CRITICAL')           AS CRITICAL_ALERTS,
       MAX(DELAY_MINUTES)                        AS WORST_DELAY_MINUTES
FROM a
GROUP BY ROUTE_ID, LOCAL_DATE;

-- 6. Freshness: how stale is the warehouse? (also used by the Airflow DQ gate) ----
CREATE OR REPLACE VIEW V_PIPELINE_FRESHNESS AS
SELECT
  MAX(WINDOW_END)                                                        AS LATEST_WINDOW_END,
  DATEDIFF('minute', MAX(WINDOW_END), CURRENT_TIMESTAMP()::TIMESTAMP_NTZ) AS MINUTES_BEHIND,
  MAX(_LOADED_AT)                                                        AS LAST_LOADED_AT
FROM GTFSR.RAW.ROUTE_DELAY_5M;

GRANT SELECT ON ALL VIEWS IN SCHEMA GTFSR.ANALYTICS TO ROLE GTFSR_ANALYST;
