-- =============================================================================
-- GRIZL Snowflake Observability — Dynamic Tables
-- =============================================================================
-- Dynamic Tables are Snowflake's auto-refreshing materialized views. They
-- continuously re-evaluate their defining query as source data changes, with
-- a configurable TARGET_LAG that controls maximum staleness.
--
-- Here they serve two purposes:
--   1. Materialized time-series baselines — expensive 2-day window aggregations
--      run once in the background rather than on every anomaly-signal query.
--   2. Training-data snapshots for Cortex ML ANOMALY_DETECTION models, which
--      require a stable INPUT_DATA reference at CREATE time.
--
-- How to apply:
--   npm --prefix snowflake run sql:dynamic-tables:dry-run
--   npm --prefix snowflake run sql:dynamic-tables
--
-- Requires: GRIZL.OBSERVABILITY schema and RAW_LOGS table (from
-- sql/grizl-observability.sql). GRIZL_WH warehouse must exist.
--
-- Snowflake edition requirement: Dynamic Tables require Snowflake Enterprise or
-- Business Critical. They are NOT available in Standard edition.
-- For Standard edition, replace with scheduled Tasks + regular tables.
-- =============================================================================


-- ── HTTP ERROR RATE TIME SERIES ───────────────────────────────────────────────
-- 5-minute bucketed error-rate aggregation per service/route over the last 2 days.
-- TARGET_LAG = '5 minutes' means each time bin is at most 5 minutes stale.
-- Used as INPUT_DATA for the HTTP_ERROR_RATE_ANOMALY Cortex ML model.

CREATE OR REPLACE DYNAMIC TABLE GRIZL.OBSERVABILITY.DT_HTTP_ERROR_RATE
  TARGET_LAG = '5 minutes'
  WAREHOUSE = GRIZL_WH
  COMMENT = 'Materialized 5-min error-rate time series per service/route. Source for Cortex ML anomaly detection and SQL z-score fallback.'
AS
SELECT
  TO_TIMESTAMP_LTZ(FLOOR(DATE_PART('epoch_second', INGEST_TIMESTAMP) / 300) * 300) AS TIME_BIN,
  CONCAT(SERVICE, '::', ROUTE)                                                       AS SERIES_KEY,
  SERVICE,
  ROUTE,
  COUNT(*)                                                                            AS REQUESTS,
  SUM(IFF(STATUS_CODE >= 500 OR SEVERITY IN ('ERROR', 'CRITICAL'), 1, 0))            AS ERRORS,
  DIV0(
    SUM(IFF(STATUS_CODE >= 500 OR SEVERITY IN ('ERROR', 'CRITICAL'), 1.0, 0.0)),
    COUNT(*)
  )                                                                                   AS ERROR_RATE
FROM GRIZL.OBSERVABILITY.HTTP_REQUESTS
WHERE INGEST_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '2 days'
GROUP BY 1, 2, 3, 4;


-- ── ROUTE LATENCY TIME SERIES ─────────────────────────────────────────────────
-- 5-minute p95 latency per service/route. Only includes requests with DURATION_MS.

CREATE OR REPLACE DYNAMIC TABLE GRIZL.OBSERVABILITY.DT_ROUTE_LATENCY
  TARGET_LAG = '5 minutes'
  WAREHOUSE = GRIZL_WH
  COMMENT = 'Materialized 5-min p95 latency time series per service/route.'
AS
SELECT
  TO_TIMESTAMP_LTZ(FLOOR(DATE_PART('epoch_second', INGEST_TIMESTAMP) / 300) * 300) AS TIME_BIN,
  CONCAT(SERVICE, '::', ROUTE)                                                       AS SERIES_KEY,
  SERVICE,
  ROUTE,
  COUNT(*)                                                                            AS REQUESTS,
  AVG(DURATION_MS)                                                                   AS AVG_DURATION_MS,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY DURATION_MS)                         AS P95_DURATION_MS
FROM GRIZL.OBSERVABILITY.HTTP_REQUESTS
WHERE INGEST_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '2 days'
  AND DURATION_MS IS NOT NULL
GROUP BY 1, 2, 3, 4;


-- ── ERROR SIGNATURE TIME SERIES ───────────────────────────────────────────────
-- 5-minute error signature spike count per service/error signature.

CREATE OR REPLACE DYNAMIC TABLE GRIZL.OBSERVABILITY.DT_ERROR_SIGNATURES
  TARGET_LAG = '5 minutes'
  WAREHOUSE = GRIZL_WH
  COMMENT = 'Materialized 5-min error signature count per service. Source for spike detection.'
AS
SELECT
  TO_TIMESTAMP_LTZ(FLOOR(DATE_PART('epoch_second', INGEST_TIMESTAMP) / 300) * 300) AS TIME_BIN,
  CONCAT(SERVICE, '::', ERROR_SIGNATURE)                                             AS SERIES_KEY,
  SERVICE,
  ERROR_SIGNATURE,
  COUNT(*)                                                                            AS ERROR_COUNT
FROM GRIZL.OBSERVABILITY.APPLICATION_ERRORS
WHERE INGEST_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '2 days'
  AND ERROR_SIGNATURE IS NOT NULL
GROUP BY 1, 2, 3, 4;


-- ── FORWARDER FRESHNESS TIME SERIES ──────────────────────────────────────────
-- Forwarder healthy-event volume by 5-minute bin (drop = freshness anomaly).

CREATE OR REPLACE DYNAMIC TABLE GRIZL.OBSERVABILITY.DT_FORWARDER_FRESHNESS
  TARGET_LAG = '5 minutes'
  WAREHOUSE = GRIZL_WH
  COMMENT = 'Materialized forwarder healthy-event count per 5-min bin. Drop signals freshness anomaly.'
AS
SELECT
  TO_TIMESTAMP_LTZ(FLOOR(DATE_PART('epoch_second', INGEST_TIMESTAMP) / 300) * 300) AS TIME_BIN,
  'forwarder_freshness'                                                              AS SERIES_KEY,
  COUNT(*)                                                                            AS HEALTHY_EVENTS
FROM GRIZL.OBSERVABILITY.FORWARDER_HEALTH
WHERE INGEST_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '2 days'
  AND EVENT_TYPE = 'batch_sent'
  AND SEVERITY NOT IN ('ERROR', 'CRITICAL')
GROUP BY 1, 2;


-- ── FORWARDER DROP/FAILURE TIME SERIES ───────────────────────────────────────
-- Forwarder skip/retry/failure event volume per 5-minute bin.

CREATE OR REPLACE DYNAMIC TABLE GRIZL.OBSERVABILITY.DT_FORWARDER_DROPS
  TARGET_LAG = '5 minutes'
  WAREHOUSE = GRIZL_WH
  COMMENT = 'Materialized forwarder skip/failure count per 5-min bin.'
AS
SELECT
  TO_TIMESTAMP_LTZ(FLOOR(DATE_PART('epoch_second', INGEST_TIMESTAMP) / 300) * 300) AS TIME_BIN,
  'forwarder_drops'                                                                  AS SERIES_KEY,
  COUNT(*)                                                                            AS DROP_COUNT
FROM GRIZL.OBSERVABILITY.FORWARDER_HEALTH
WHERE INGEST_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '2 days'
  AND (EVENT_TYPE = 'message_skipped' OR SEVERITY IN ('ERROR', 'CRITICAL'))
GROUP BY 1, 2;


-- ── POST-DEPLOYMENT REGRESSION TIME SERIES ───────────────────────────────────
-- Error rate per deployment SHA over the last 2 days.
-- Detects regressions introduced by a specific deployment.

CREATE OR REPLACE DYNAMIC TABLE GRIZL.OBSERVABILITY.DT_DEPLOYMENT_ERROR_RATE
  TARGET_LAG = '5 minutes'
  WAREHOUSE = GRIZL_WH
  COMMENT = 'Materialized error rate per deployment SHA. Used to detect post-deployment regressions.'
AS
SELECT
  TO_TIMESTAMP_LTZ(FLOOR(DATE_PART('epoch_second', INGEST_TIMESTAMP) / 300) * 300) AS TIME_BIN,
  CONCAT(SERVICE, '::', DEPLOYMENT_SHA)                                             AS SERIES_KEY,
  SERVICE,
  DEPLOYMENT_SHA,
  COUNT(*)                                                                            AS REQUESTS,
  DIV0(
    SUM(IFF(STATUS_CODE >= 500 OR SEVERITY IN ('ERROR', 'CRITICAL'), 1.0, 0.0)),
    COUNT(*)
  )                                                                                   AS ERROR_RATE
FROM GRIZL.OBSERVABILITY.HTTP_REQUESTS
WHERE INGEST_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '2 days'
  AND DEPLOYMENT_SHA IS NOT NULL
GROUP BY 1, 2, 3, 4;
