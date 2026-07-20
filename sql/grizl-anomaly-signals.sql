-- =============================================================================
-- GRIZL Snowflake Observability — SQL Anomaly Signal Views
-- =============================================================================
-- These views implement z-score anomaly detection over the Dynamic Table
-- baselines (sql/grizl-dynamic-tables.sql). They are the immediate-use
-- fallback path that works without Cortex ML model creation.
--
-- The Cortex ML ANOMALY_DETECTION path (sql/grizl-cortex-ml.sql) replaces
-- these views with purpose-built time-series ML models once training data
-- accumulates. The two approaches are interchangeable — GRIZL_RECENT_ANOMALY_SIGNALS
-- (the Snowflake Alert trigger) can point at either output.
--
-- Detection design (matches grizl-databricks-observability z-score pattern):
--   Baseline window : last 2 days, excluding the most recent 15 minutes
--   Detection window: last 15 minutes
--   Bin granularity : 5 minutes (FLOOR(epoch_seconds / 300) * 300)
--   Anomaly threshold: z-score >= 1.5  (configurable in the Alert)
--   Minimum data    : >= 20 requests, STDDEV > 0 (natural data requirements)
--
-- How to apply:
--   npm --prefix snowflake run sql:anomaly-signals:dry-run
--   npm --prefix snowflake run sql:anomaly-signals
--
-- Requires: Dynamic Tables from sql/grizl-dynamic-tables.sql.
-- =============================================================================


-- ── BACKEND HTTP ERROR RATE ANOMALIES ────────────────────────────────────────
-- Z-score anomalies in 5xx/error rate by service and route.
-- Equivalent to KQL BackendHttpErrorRateAnomalies() and Databricks
-- backend_http_error_rate_anomalies view.

CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.BACKEND_HTTP_ERROR_RATE_ANOMALIES
  COMMENT = 'Positive z-score anomalies in backend HTTP 5xx/error rate per service/route. Equivalent to KQL BackendHttpErrorRateAnomalies().'
AS
WITH baseline AS (
  SELECT
    SERIES_KEY, SERVICE, ROUTE,
    AVG(ERROR_RATE)    AS BASELINE_MEAN,
    STDDEV(ERROR_RATE) AS BASELINE_STDDEV
  FROM GRIZL.OBSERVABILITY.DT_HTTP_ERROR_RATE
  WHERE TIME_BIN < CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
  GROUP BY 1, 2, 3
),
detection AS (
  SELECT
    TIME_BIN, SERIES_KEY, SERVICE, ROUTE, REQUESTS, ERRORS, ERROR_RATE
  FROM GRIZL.OBSERVABILITY.DT_HTTP_ERROR_RATE
  WHERE TIME_BIN >= CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
)
SELECT
  d.TIME_BIN,
  d.SERVICE,
  d.ROUTE,
  d.REQUESTS,
  d.ERRORS,
  d.ERROR_RATE                                              AS ACTUAL,
  b.BASELINE_MEAN,
  b.BASELINE_STDDEV,
  DIV0(d.ERROR_RATE - b.BASELINE_MEAN, b.BASELINE_STDDEV) AS ANOMALY_SCORE,
  'backend_http_error_rate'                                 AS SIGNAL_TYPE
FROM detection d
JOIN baseline b USING (SERIES_KEY)
WHERE d.REQUESTS >= 20
  AND b.BASELINE_STDDEV > 0
  AND DIV0(d.ERROR_RATE - b.BASELINE_MEAN, b.BASELINE_STDDEV) >= 1.5;


-- ── ROUTE LATENCY ANOMALIES ───────────────────────────────────────────────────
-- Z-score anomalies in p95 latency by service and route.
-- Equivalent to KQL RouteLatencyAnomalies() and Databricks route_latency_anomalies.

CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.ROUTE_LATENCY_ANOMALIES
  COMMENT = 'Positive z-score anomalies in route p95 latency per service/route. Equivalent to KQL RouteLatencyAnomalies().'
AS
WITH baseline AS (
  SELECT
    SERIES_KEY, SERVICE, ROUTE,
    AVG(P95_DURATION_MS)    AS BASELINE_MEAN,
    STDDEV(P95_DURATION_MS) AS BASELINE_STDDEV
  FROM GRIZL.OBSERVABILITY.DT_ROUTE_LATENCY
  WHERE TIME_BIN < CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
  GROUP BY 1, 2, 3
),
detection AS (
  SELECT
    TIME_BIN, SERIES_KEY, SERVICE, ROUTE, REQUESTS, P95_DURATION_MS
  FROM GRIZL.OBSERVABILITY.DT_ROUTE_LATENCY
  WHERE TIME_BIN >= CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
)
SELECT
  d.TIME_BIN,
  d.SERVICE,
  d.ROUTE,
  d.REQUESTS,
  NULL::FLOAT AS ERRORS,
  d.P95_DURATION_MS                                              AS ACTUAL,
  b.BASELINE_MEAN,
  b.BASELINE_STDDEV,
  DIV0(d.P95_DURATION_MS - b.BASELINE_MEAN, b.BASELINE_STDDEV) AS ANOMALY_SCORE,
  'route_latency_p95'                                            AS SIGNAL_TYPE
FROM detection d
JOIN baseline b USING (SERIES_KEY)
WHERE d.REQUESTS >= 10
  AND b.BASELINE_STDDEV > 0
  AND DIV0(d.P95_DURATION_MS - b.BASELINE_MEAN, b.BASELINE_STDDEV) >= 1.5;


-- ── ERROR SIGNATURE SPIKE ANOMALIES ──────────────────────────────────────────
-- Z-score anomalies in application error signature spike counts.
-- Equivalent to KQL ErrorSignatureSpikeAnomalies() and Databricks
-- error_signature_spike_anomalies.

CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.ERROR_SIGNATURE_SPIKE_ANOMALIES
  COMMENT = 'Positive z-score anomalies in per-signature error spike counts per service. Equivalent to KQL ErrorSignatureSpikeAnomalies().'
AS
WITH baseline AS (
  SELECT
    SERIES_KEY, SERVICE, ERROR_SIGNATURE,
    AVG(ERROR_COUNT)    AS BASELINE_MEAN,
    STDDEV(ERROR_COUNT) AS BASELINE_STDDEV
  FROM GRIZL.OBSERVABILITY.DT_ERROR_SIGNATURES
  WHERE TIME_BIN < CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
  GROUP BY 1, 2, 3
),
detection AS (
  SELECT
    TIME_BIN, SERIES_KEY, SERVICE, ERROR_SIGNATURE, ERROR_COUNT
  FROM GRIZL.OBSERVABILITY.DT_ERROR_SIGNATURES
  WHERE TIME_BIN >= CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
)
SELECT
  d.TIME_BIN,
  d.SERVICE,
  d.ERROR_SIGNATURE                                               AS ROUTE,
  NULL::NUMBER AS REQUESTS,
  d.ERROR_COUNT::FLOAT                                            AS ERRORS,
  d.ERROR_COUNT::FLOAT                                            AS ACTUAL,
  b.BASELINE_MEAN,
  b.BASELINE_STDDEV,
  DIV0(d.ERROR_COUNT - b.BASELINE_MEAN, b.BASELINE_STDDEV)       AS ANOMALY_SCORE,
  'error_signature_spike'                                         AS SIGNAL_TYPE
FROM detection d
JOIN baseline b USING (SERIES_KEY)
WHERE d.ERROR_COUNT >= 3
  AND b.BASELINE_STDDEV > 0
  AND DIV0(d.ERROR_COUNT - b.BASELINE_MEAN, b.BASELINE_STDDEV) >= 1.5;


-- ── FORWARDER FRESHNESS DROP ANOMALIES ───────────────────────────────────────
-- Negative z-score anomalies in forwarder healthy-event volume.
-- A drop (not a spike) signals that the forwarder is stuck or unhealthy.
-- Equivalent to KQL ForwarderFreshnessDropAnomalies() and Databricks
-- forwarder_freshness_drop_anomalies.

CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.FORWARDER_FRESHNESS_DROP_ANOMALIES
  COMMENT = 'Negative z-score anomalies in forwarder healthy-event volume (volume drop = freshness problem). Equivalent to KQL ForwarderFreshnessDropAnomalies().'
AS
WITH baseline AS (
  SELECT
    SERIES_KEY,
    AVG(HEALTHY_EVENTS)    AS BASELINE_MEAN,
    STDDEV(HEALTHY_EVENTS) AS BASELINE_STDDEV
  FROM GRIZL.OBSERVABILITY.DT_FORWARDER_FRESHNESS
  WHERE TIME_BIN < CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
  GROUP BY 1
),
detection AS (
  SELECT TIME_BIN, SERIES_KEY, HEALTHY_EVENTS
  FROM GRIZL.OBSERVABILITY.DT_FORWARDER_FRESHNESS
  WHERE TIME_BIN >= CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
)
SELECT
  d.TIME_BIN,
  'grizl-log-forwarder'                                             AS SERVICE,
  'forwarder_freshness'                                             AS ROUTE,
  d.HEALTHY_EVENTS::NUMBER                                          AS REQUESTS,
  0::FLOAT                                                          AS ERRORS,
  d.HEALTHY_EVENTS::FLOAT                                           AS ACTUAL,
  b.BASELINE_MEAN,
  b.BASELINE_STDDEV,
  -- Negate: a volume DROP is the anomaly (below the baseline)
  DIV0(b.BASELINE_MEAN - d.HEALTHY_EVENTS, b.BASELINE_STDDEV)      AS ANOMALY_SCORE,
  'forwarder_freshness_drop'                                        AS SIGNAL_TYPE
FROM detection d
JOIN baseline b USING (SERIES_KEY)
WHERE b.BASELINE_STDDEV > 0
  AND DIV0(b.BASELINE_MEAN - d.HEALTHY_EVENTS, b.BASELINE_STDDEV) >= 1.5;


-- ── FORWARDER DROP/FAILURE ANOMALIES ─────────────────────────────────────────
-- Positive z-score anomalies in forwarder skip/failure event volume.
-- Equivalent to KQL ForwarderDropFailureAnomalies() and Databricks
-- forwarder_drop_failure_anomalies.

CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.FORWARDER_DROP_FAILURE_ANOMALIES
  COMMENT = 'Positive z-score anomalies in forwarder skip/failure event counts. Equivalent to KQL ForwarderDropFailureAnomalies().'
AS
WITH baseline AS (
  SELECT
    SERIES_KEY,
    AVG(DROP_COUNT)    AS BASELINE_MEAN,
    STDDEV(DROP_COUNT) AS BASELINE_STDDEV
  FROM GRIZL.OBSERVABILITY.DT_FORWARDER_DROPS
  WHERE TIME_BIN < CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
  GROUP BY 1
),
detection AS (
  SELECT TIME_BIN, SERIES_KEY, DROP_COUNT
  FROM GRIZL.OBSERVABILITY.DT_FORWARDER_DROPS
  WHERE TIME_BIN >= CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
)
SELECT
  d.TIME_BIN,
  'grizl-log-forwarder'                                         AS SERVICE,
  'forwarder_drops'                                             AS ROUTE,
  0::NUMBER                                                     AS REQUESTS,
  d.DROP_COUNT::FLOAT                                           AS ERRORS,
  d.DROP_COUNT::FLOAT                                           AS ACTUAL,
  b.BASELINE_MEAN,
  b.BASELINE_STDDEV,
  DIV0(d.DROP_COUNT - b.BASELINE_MEAN, b.BASELINE_STDDEV)      AS ANOMALY_SCORE,
  'forwarder_drop_failure'                                      AS SIGNAL_TYPE
FROM detection d
JOIN baseline b USING (SERIES_KEY)
WHERE b.BASELINE_STDDEV > 0
  AND DIV0(d.DROP_COUNT - b.BASELINE_MEAN, b.BASELINE_STDDEV) >= 1.5;


-- ── POST-DEPLOYMENT REGRESSION ANOMALIES ─────────────────────────────────────
-- Compares error rate for the most recent deployment SHA against the pre-deployment
-- baseline. Fires when the latest SHA shows a significantly higher error rate than
-- prior SHAs for the same service.
-- Equivalent to KQL PostDeploymentRegressionAnomalies() and Databricks
-- post_deployment_regression_anomalies.

CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.POST_DEPLOYMENT_REGRESSION_ANOMALIES
  COMMENT = 'Z-score regression anomalies in error rate per deployment SHA. Equivalent to KQL PostDeploymentRegressionAnomalies().'
AS
WITH baseline AS (
  -- Compute baseline stats excluding the most recent deployment SHA per service
  SELECT
    SERVICE,
    AVG(ERROR_RATE)    AS BASELINE_MEAN,
    STDDEV(ERROR_RATE) AS BASELINE_STDDEV
  FROM GRIZL.OBSERVABILITY.DT_DEPLOYMENT_ERROR_RATE
  WHERE TIME_BIN < CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
  GROUP BY 1
),
recent_sha AS (
  -- Most recent deployment SHA per service in the detection window
  SELECT
    SERVICE,
    DEPLOYMENT_SHA,
    AVG(ERROR_RATE) AS ACTUAL_ERROR_RATE,
    SUM(REQUESTS)   AS TOTAL_REQUESTS
  FROM GRIZL.OBSERVABILITY.DT_DEPLOYMENT_ERROR_RATE
  WHERE TIME_BIN >= CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
  GROUP BY 1, 2
)
SELECT
  CURRENT_TIMESTAMP()                                             AS TIME_BIN,
  r.SERVICE,
  r.DEPLOYMENT_SHA                                               AS ROUTE,
  r.TOTAL_REQUESTS                                               AS REQUESTS,
  NULL::FLOAT                                                    AS ERRORS,
  r.ACTUAL_ERROR_RATE                                            AS ACTUAL,
  b.BASELINE_MEAN,
  b.BASELINE_STDDEV,
  DIV0(r.ACTUAL_ERROR_RATE - b.BASELINE_MEAN, b.BASELINE_STDDEV) AS ANOMALY_SCORE,
  'post_deployment_regression'                                    AS SIGNAL_TYPE
FROM recent_sha r
JOIN baseline b USING (SERVICE)
WHERE r.TOTAL_REQUESTS >= 20
  AND b.BASELINE_STDDEV > 0
  AND DIV0(r.ACTUAL_ERROR_RATE - b.BASELINE_MEAN, b.BASELINE_STDDEV) >= 1.5;


-- ── GRIZL RECENT ANOMALY SIGNALS (UNION VIEW) ────────────────────────────────
-- Union of all anomaly signal views. This is the query the Snowflake Alert
-- (sql/grizl-alert-queries.sql) monitors every 5 minutes.
-- Equivalent to KQL GrizlRecentAnomalySignals() and Databricks
-- grizl_recent_anomaly_signals UNION view.

CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.GRIZL_RECENT_ANOMALY_SIGNALS
  COMMENT = 'UNION of all anomaly signal views. Single query for the Snowflake Alert trigger. Equivalent to KQL GrizlRecentAnomalySignals() and Databricks grizl_recent_anomaly_signals.'
AS
SELECT
  TIME_BIN, SIGNAL_TYPE, SERVICE, ROUTE,
  REQUESTS, ERRORS, ACTUAL, BASELINE_MEAN, BASELINE_STDDEV, ANOMALY_SCORE
FROM GRIZL.OBSERVABILITY.BACKEND_HTTP_ERROR_RATE_ANOMALIES

UNION ALL

SELECT
  TIME_BIN, SIGNAL_TYPE, SERVICE, ROUTE,
  REQUESTS, ERRORS, ACTUAL, BASELINE_MEAN, BASELINE_STDDEV, ANOMALY_SCORE
FROM GRIZL.OBSERVABILITY.ROUTE_LATENCY_ANOMALIES

UNION ALL

SELECT
  TIME_BIN, SIGNAL_TYPE, SERVICE, ROUTE,
  REQUESTS, ERRORS, ACTUAL, BASELINE_MEAN, BASELINE_STDDEV, ANOMALY_SCORE
FROM GRIZL.OBSERVABILITY.ERROR_SIGNATURE_SPIKE_ANOMALIES

UNION ALL

SELECT
  TIME_BIN, SIGNAL_TYPE, SERVICE, ROUTE,
  REQUESTS, ERRORS, ACTUAL, BASELINE_MEAN, BASELINE_STDDEV, ANOMALY_SCORE
FROM GRIZL.OBSERVABILITY.FORWARDER_FRESHNESS_DROP_ANOMALIES

UNION ALL

SELECT
  TIME_BIN, SIGNAL_TYPE, SERVICE, ROUTE,
  REQUESTS, ERRORS, ACTUAL, BASELINE_MEAN, BASELINE_STDDEV, ANOMALY_SCORE
FROM GRIZL.OBSERVABILITY.FORWARDER_DROP_FAILURE_ANOMALIES

UNION ALL

SELECT
  TIME_BIN, SIGNAL_TYPE, SERVICE, ROUTE,
  REQUESTS, ERRORS, ACTUAL, BASELINE_MEAN, BASELINE_STDDEV, ANOMALY_SCORE
FROM GRIZL.OBSERVABILITY.POST_DEPLOYMENT_REGRESSION_ANOMALIES

ORDER BY ANOMALY_SCORE DESC;
