-- =============================================================================
-- GRIZL Snowflake Observability — Snowsight Dashboard Tile Queries
-- =============================================================================
-- These queries back Snowsight dashboard tiles for operational triage.
-- Paste each labeled block into a Snowsight worksheet and pin to a dashboard.
--
-- Equivalent to the Databricks SQL Dashboard tile queries and the KQL
-- Real-Time Dashboard queries in grizl-fabric-observability.
-- =============================================================================


-- ── Tile 1: Error rate by service (last 60 minutes) ──────────────────────────
-- Chart type: Bar  |  Y: ERROR_RATE  |  X: SERVICE
SELECT
  SERVICE,
  COUNT(*)                                                                          AS TOTAL_REQUESTS,
  SUM(IFF(STATUS_CODE >= 500 OR SEVERITY IN ('ERROR', 'CRITICAL'), 1, 0))          AS ERROR_COUNT,
  ROUND(
    DIV0(
      SUM(IFF(STATUS_CODE >= 500 OR SEVERITY IN ('ERROR', 'CRITICAL'), 1.0, 0.0)),
      COUNT(*)
    ) * 100,
  2)                                                                                AS ERROR_RATE_PCT
FROM GRIZL.OBSERVABILITY.HTTP_REQUESTS
WHERE INGEST_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '60 minutes'
GROUP BY 1
ORDER BY ERROR_RATE_PCT DESC;


-- ── Tile 2: Request rate by 5-minute bin (last 2 hours) ──────────────────────
-- Chart type: Line  |  Y: REQUESTS  |  X: TIME_BIN  |  Series: SERVICE
SELECT
  TO_TIMESTAMP_LTZ(FLOOR(DATE_PART('epoch_second', INGEST_TIMESTAMP) / 300) * 300) AS TIME_BIN,
  SERVICE,
  COUNT(*)                                                                           AS REQUESTS
FROM GRIZL.OBSERVABILITY.HTTP_REQUESTS
WHERE INGEST_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '2 hours'
GROUP BY 1, 2
ORDER BY 1, 2;


-- ── Tile 3: P95 latency by route (last 60 minutes, top 20) ───────────────────
-- Chart type: Bar (horizontal)  |  Y: P95_MS  |  X: ROUTE
SELECT
  ROUTE,
  SERVICE,
  COUNT(*)                                                                    AS REQUESTS,
  ROUND(AVG(DURATION_MS), 1)                                                  AS AVG_MS,
  ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY DURATION_MS), 1)        AS P95_MS
FROM GRIZL.OBSERVABILITY.HTTP_REQUESTS
WHERE INGEST_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '60 minutes'
  AND DURATION_MS IS NOT NULL
GROUP BY 1, 2
ORDER BY P95_MS DESC
LIMIT 20;


-- ── Tile 4: Top error signatures (last 60 minutes) ────────────────────────────
-- Chart type: Table  |  Columns: SERVICE, ERROR_SIGNATURE, ERROR_COUNT
SELECT
  SERVICE,
  ERROR_SIGNATURE,
  COUNT(*)                                                                      AS ERROR_COUNT,
  MIN(INGEST_TIMESTAMP)                                                         AS FIRST_SEEN,
  MAX(INGEST_TIMESTAMP)                                                         AS LAST_SEEN,
  LISTAGG(DISTINCT DEPLOYMENT_SHA, ', ') WITHIN GROUP (ORDER BY DEPLOYMENT_SHA) AS DEPLOYMENT_SHAS
FROM GRIZL.OBSERVABILITY.APPLICATION_ERRORS
WHERE INGEST_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '60 minutes'
GROUP BY 1, 2
ORDER BY ERROR_COUNT DESC
LIMIT 25;


-- ── Tile 5: Current anomaly signals ──────────────────────────────────────────
-- Chart type: Table  |  All columns
SELECT
  TIME_BIN,
  SIGNAL_TYPE,
  SERVICE,
  ROUTE,
  ROUND(ANOMALY_SCORE, 2)       AS ANOMALY_SCORE,
  ROUND(ACTUAL, 4)              AS ACTUAL,
  ROUND(BASELINE_MEAN, 4)       AS BASELINE,
  REQUESTS
FROM GRIZL.OBSERVABILITY.GRIZL_RECENT_ANOMALY_SIGNALS
ORDER BY ANOMALY_SCORE DESC;


-- ── Tile 6: Recent deployments ───────────────────────────────────────────────
-- Chart type: Table  |  Shows recent deployment events
SELECT
  INGEST_TIMESTAMP,
  SERVICE,
  ENVIRONMENT,
  DEPLOYMENT_SHA,
  EVENT_TYPE
FROM GRIZL.OBSERVABILITY.DEPLOYMENTS
WHERE INGEST_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
ORDER BY INGEST_TIMESTAMP DESC
LIMIT 30;


-- ── Tile 7: Forwarder health (last 2 hours, 5-min bins) ──────────────────────
-- Chart type: Bar (stacked)  |  Y: EVENT_COUNT  |  X: TIME_BIN  |  Series: EVENT_TYPE
SELECT
  TO_TIMESTAMP_LTZ(FLOOR(DATE_PART('epoch_second', INGEST_TIMESTAMP) / 300) * 300) AS TIME_BIN,
  EVENT_TYPE,
  COUNT(*)                                                                           AS EVENT_COUNT
FROM GRIZL.OBSERVABILITY.FORWARDER_HEALTH
WHERE INGEST_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '2 hours'
GROUP BY 1, 2
ORDER BY 1, 2;


-- ── Tile 8: Error rate by route and deployment SHA (last 4 hours) ─────────────
-- Chart type: Heatmap or table  |  Surface post-deployment regression candidates
SELECT
  SERVICE,
  DEPLOYMENT_SHA,
  ROUTE,
  COUNT(*)                                                                            AS REQUESTS,
  SUM(IFF(STATUS_CODE >= 500 OR SEVERITY IN ('ERROR', 'CRITICAL'), 1, 0))            AS ERRORS,
  ROUND(
    DIV0(
      SUM(IFF(STATUS_CODE >= 500 OR SEVERITY IN ('ERROR', 'CRITICAL'), 1.0, 0.0)),
      COUNT(*)
    ) * 100,
  2)                                                                                  AS ERROR_RATE_PCT
FROM GRIZL.OBSERVABILITY.HTTP_REQUESTS
WHERE INGEST_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '4 hours'
  AND DEPLOYMENT_SHA IS NOT NULL
GROUP BY 1, 2, 3
HAVING REQUESTS >= 10
ORDER BY ERROR_RATE_PCT DESC
LIMIT 30;


-- ── Tile 9: 5-minute anomaly score trend (last 2 hours) ──────────────────────
-- Built on DT_HTTP_ERROR_RATE for live updates.
-- Chart type: Line  |  Y: Z_SCORE  |  X: TIME_BIN  |  Reference line at 1.5
WITH baseline AS (
  SELECT
    SERIES_KEY,
    AVG(ERROR_RATE)    AS BASELINE_MEAN,
    STDDEV(ERROR_RATE) AS BASELINE_STDDEV
  FROM GRIZL.OBSERVABILITY.DT_HTTP_ERROR_RATE
  WHERE TIME_BIN < CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
  GROUP BY 1
)
SELECT
  ts.TIME_BIN,
  ts.SERIES_KEY,
  ts.SERVICE,
  ts.ROUTE,
  ts.REQUESTS,
  ROUND(DIV0(ts.ERROR_RATE - b.BASELINE_MEAN, b.BASELINE_STDDEV), 2) AS Z_SCORE
FROM GRIZL.OBSERVABILITY.DT_HTTP_ERROR_RATE ts
JOIN baseline b USING (SERIES_KEY)
WHERE ts.TIME_BIN >= CURRENT_TIMESTAMP() - INTERVAL '2 hours'
  AND b.BASELINE_STDDEV > 0
  AND ts.REQUESTS >= 5
ORDER BY ts.TIME_BIN, Z_SCORE DESC;
