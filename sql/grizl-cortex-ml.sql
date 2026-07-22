-- =============================================================================
-- GRIZL Snowflake Observability — Cortex ML Anomaly Detection
-- =============================================================================
-- This file implements the Cortex ML ANOMALY_DETECTION path, which replaces
-- the SQL z-score views (sql/grizl-anomaly-signals.sql) with purpose-built
-- time-series ML models once sufficient training data accumulates.
--
-- SNOWFLAKE.ML.ANOMALY_DETECTION is a Cortex ML Feature available in
-- Snowflake Enterprise and above. It trains an unsupervised time-series
-- model on historical data and returns anomaly scores, confidence intervals,
-- and boolean IS_ANOMALY flags.
--
-- Workflow:
--   1. Wait for at least 2 days of RAW_LOGS data in the Dynamic Tables.
--   2. Run the CREATE statements to train each model (one-time setup).
--   3. Schedule the DETECT stored procedures via Task every 5 minutes.
--   4. Wire GRIZL_RECENT_ANOMALY_SIGNALS_ML (at the bottom of this file)
--      to the Alert instead of the SQL z-score UNION view.
--
-- How to apply:
--   npm --prefix snowflake run sql:cortex-ml:dry-run
--   npm --prefix snowflake run sql:cortex-ml
--
-- Requires:
--   - Snowflake Enterprise edition
--   - Dynamic Tables from sql/grizl-dynamic-tables.sql with >= 2 days of data
--   - SNOWFLAKE.ML privilege granted to GRIZL_ROLE:
--       GRANT DATABASE ROLE SNOWFLAKE.ML_USER TO ROLE GRIZL_ROLE;
-- =============================================================================


-- ── TRAINING INPUT VIEWS ─────────────────────────────────────────────────────
-- SYSTEM$REFERENCE requires 'VIEW' or 'TABLE' — 'DYNAMIC TABLE' is not a valid
-- scope for CALL-context reference creation. These views expose Dynamic Table
-- history for model training, excluding the last 30 minutes so that detection
-- timestamps (ML_DETECT_*_INPUT rolls a 30-min window) are always strictly
-- after the last training point — required by Snowflake ML ANOMALY_DETECTION.

CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.ML_TRAIN_ERROR_RATE_INPUT
  COMMENT = 'DT_HTTP_ERROR_RATE excluding last 30 min. Passed as INPUT_DATA to HTTP_ERROR_RATE_ANOMALY_MODEL training.'
AS
SELECT TIME_BIN, SERIES_KEY, SERVICE, ROUTE, ERROR_RATE
FROM GRIZL.OBSERVABILITY.DT_HTTP_ERROR_RATE
WHERE TIME_BIN < DATEADD('minute', -30, CURRENT_TIMESTAMP());

CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.ML_TRAIN_LATENCY_INPUT
  COMMENT = 'DT_ROUTE_LATENCY excluding last 30 min. Passed as INPUT_DATA to ROUTE_LATENCY_ANOMALY_MODEL training.'
AS
SELECT TIME_BIN, SERIES_KEY, SERVICE, ROUTE, P95_DURATION_MS
FROM GRIZL.OBSERVABILITY.DT_ROUTE_LATENCY
WHERE TIME_BIN < DATEADD('minute', -30, CURRENT_TIMESTAMP());

CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.ML_TRAIN_ERROR_SIGS_INPUT
  COMMENT = 'DT_ERROR_SIGNATURES excluding last 30 min. Passed as INPUT_DATA to ERROR_SIGNATURE_ANOMALY_MODEL training.'
AS
SELECT TIME_BIN, SERIES_KEY, SERVICE, ERROR_SIGNATURE, ERROR_COUNT
FROM GRIZL.OBSERVABILITY.DT_ERROR_SIGNATURES
WHERE TIME_BIN < DATEADD('minute', -30, CURRENT_TIMESTAMP());


-- ── CORTEX ML MODEL: HTTP ERROR RATE ────────────────────────────────────────
-- Trains on DT_HTTP_ERROR_RATE (2-day baseline) via the training view.
-- SERIES_COLNAME partitions the model by service::route key.
-- LABEL_COLNAME = NULL means unsupervised detection.

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION GRIZL.OBSERVABILITY.HTTP_ERROR_RATE_ANOMALY_MODEL(
  INPUT_DATA        => SYSTEM$REFERENCE('VIEW', 'GRIZL.OBSERVABILITY.ML_TRAIN_ERROR_RATE_INPUT'),
  SERIES_COLNAME    => 'SERIES_KEY',
  TIMESTAMP_COLNAME => 'TIME_BIN',
  TARGET_COLNAME    => 'ERROR_RATE',
  LABEL_COLNAME     => NULL
);


-- ── CORTEX ML MODEL: ROUTE LATENCY ──────────────────────────────────────────

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION GRIZL.OBSERVABILITY.ROUTE_LATENCY_ANOMALY_MODEL(
  INPUT_DATA        => SYSTEM$REFERENCE('VIEW', 'GRIZL.OBSERVABILITY.ML_TRAIN_LATENCY_INPUT'),
  SERIES_COLNAME    => 'SERIES_KEY',
  TIMESTAMP_COLNAME => 'TIME_BIN',
  TARGET_COLNAME    => 'P95_DURATION_MS',
  LABEL_COLNAME     => NULL
);


-- ── CORTEX ML MODEL: ERROR SIGNATURE SPIKES ─────────────────────────────────

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION GRIZL.OBSERVABILITY.ERROR_SIGNATURE_ANOMALY_MODEL(
  INPUT_DATA        => SYSTEM$REFERENCE('VIEW', 'GRIZL.OBSERVABILITY.ML_TRAIN_ERROR_SIGS_INPUT'),
  SERIES_COLNAME    => 'SERIES_KEY',
  TIMESTAMP_COLNAME => 'TIME_BIN',
  TARGET_COLNAME    => 'ERROR_COUNT',
  LABEL_COLNAME     => NULL
);


-- ── DETECTION INPUT VIEWS (persistent, used by SYSTEM$REFERENCE) ─────────────
-- SYSTEM$REFERENCE requires a persistent (non-temporary) object.
-- These views filter the Dynamic Tables to a rolling 30-minute detection window
-- and are passed to DETECT_ANOMALIES inside the stored procedures.

CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.ML_DETECT_ERROR_RATE_INPUT
  COMMENT = 'Rolling 30-min window of DT_HTTP_ERROR_RATE. Passed as INPUT_DATA to DETECT_ANOMALIES via SYSTEM$REFERENCE.'
AS
SELECT TIME_BIN, SERIES_KEY, SERVICE, ROUTE, ERROR_RATE
FROM GRIZL.OBSERVABILITY.DT_HTTP_ERROR_RATE
WHERE TIME_BIN >= DATEADD('minute', -30, CURRENT_TIMESTAMP());

CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.ML_DETECT_LATENCY_INPUT
  COMMENT = 'Rolling 30-min window of DT_ROUTE_LATENCY. Passed as INPUT_DATA to DETECT_ANOMALIES via SYSTEM$REFERENCE.'
AS
SELECT TIME_BIN, SERIES_KEY, SERVICE, ROUTE, P95_DURATION_MS
FROM GRIZL.OBSERVABILITY.DT_ROUTE_LATENCY
WHERE TIME_BIN >= DATEADD('minute', -30, CURRENT_TIMESTAMP());


-- ── DETECTION TABLE (written by Tasks) ───────────────────────────────────────
-- A regular table that each Task writes detection results into every 5 minutes.
-- The ML UNION view (below) reads from this table rather than re-running CALL.

CREATE TABLE IF NOT EXISTS GRIZL.OBSERVABILITY.ML_ANOMALY_DETECTIONS (
  DETECTED_AT     TIMESTAMP_LTZ NOT NULL  COMMENT 'Timestamp when the detection Task ran',
  MODEL_NAME      VARCHAR                 COMMENT 'Which Cortex ML model produced this row',
  TS              TIMESTAMP_LTZ           COMMENT 'Time bin of the anomaly (from DETECT_ANOMALIES)',
  SERIES_KEY      VARCHAR                 COMMENT 'Series identifier (service::route or service::error_signature)',
  SERVICE         VARCHAR,
  ROUTE           VARCHAR,
  Y               FLOAT                   COMMENT 'Observed value',
  FORECAST        FLOAT                   COMMENT 'Model forecast for this bin',
  LOWER_BOUND     FLOAT                   COMMENT 'Lower confidence bound',
  UPPER_BOUND     FLOAT                   COMMENT 'Upper confidence bound',
  IS_ANOMALY      BOOLEAN,
  PERCENTILE      FLOAT                   COMMENT 'Model confidence percentile',
  DISTANCE        FLOAT                   COMMENT 'Normalized deviation — equivalent to z-score',
  SIGNAL_TYPE     VARCHAR
)
DATA_RETENTION_TIME_IN_DAYS = 7
CLUSTER BY (DATE_TRUNC('day', DETECTED_AT))
COMMENT = 'Cortex ML anomaly detection results. Written by scheduled Tasks; read by GRIZL_RECENT_ANOMALY_SIGNALS_ML.';


-- ── DETECT PROCEDURE: HTTP ERROR RATE ────────────────────────────────────────
-- Calls the trained model against the most-recent 30 minutes of DT_HTTP_ERROR_RATE
-- and inserts flagged anomalies into ML_ANOMALY_DETECTIONS.
-- Called by Task GRIZL_ERROR_RATE_DETECT_TASK every 5 minutes.

CREATE OR REPLACE PROCEDURE GRIZL.OBSERVABILITY.SP_DETECT_HTTP_ERROR_RATE()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
  -- ML_DETECT_ERROR_RATE_INPUT is a persistent view (not temp) so SYSTEM$REFERENCE works.
  -- CURRENT_TIMESTAMP() used inline to avoid DECLARE bind-variable issues with snow CLI.
  INSERT INTO GRIZL.OBSERVABILITY.ML_ANOMALY_DETECTIONS (
    DETECTED_AT, MODEL_NAME, TS, SERIES_KEY, SERVICE, ROUTE,
    Y, FORECAST, LOWER_BOUND, UPPER_BOUND, IS_ANOMALY, PERCENTILE, DISTANCE, SIGNAL_TYPE
  )
  SELECT
    CURRENT_TIMESTAMP(),
    'HTTP_ERROR_RATE_ANOMALY_MODEL',
    r.TS,
    r.SERIES,
    SPLIT_PART(r.SERIES, '::', 1),
    SPLIT_PART(r.SERIES, '::', 2),
    r.Y, r.FORECAST, r.LOWER_BOUND, r.UPPER_BOUND,
    r.IS_ANOMALY, r.PERCENTILE, r.DISTANCE,
    'backend_http_error_rate_ml'
  FROM TABLE(
    GRIZL.OBSERVABILITY.HTTP_ERROR_RATE_ANOMALY_MODEL!DETECT_ANOMALIES(
      INPUT_DATA        => SYSTEM$REFERENCE('VIEW', 'GRIZL.OBSERVABILITY.ML_DETECT_ERROR_RATE_INPUT'),
      SERIES_COLNAME    => 'SERIES_KEY',
      TIMESTAMP_COLNAME => 'TIME_BIN',
      TARGET_COLNAME    => 'ERROR_RATE'
    )
  ) r
  WHERE r.IS_ANOMALY = TRUE
    AND r.TS >= DATEADD('minute', -15, CURRENT_TIMESTAMP());

  RETURN 'OK';
END;
$$;


-- ── DETECT PROCEDURE: ROUTE LATENCY ─────────────────────────────────────────

CREATE OR REPLACE PROCEDURE GRIZL.OBSERVABILITY.SP_DETECT_ROUTE_LATENCY()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
  INSERT INTO GRIZL.OBSERVABILITY.ML_ANOMALY_DETECTIONS (
    DETECTED_AT, MODEL_NAME, TS, SERIES_KEY, SERVICE, ROUTE,
    Y, FORECAST, LOWER_BOUND, UPPER_BOUND, IS_ANOMALY, PERCENTILE, DISTANCE, SIGNAL_TYPE
  )
  SELECT
    CURRENT_TIMESTAMP(), 'ROUTE_LATENCY_ANOMALY_MODEL', r.TS, r.SERIES,
    SPLIT_PART(r.SERIES, '::', 1), SPLIT_PART(r.SERIES, '::', 2),
    r.Y, r.FORECAST, r.LOWER_BOUND, r.UPPER_BOUND,
    r.IS_ANOMALY, r.PERCENTILE, r.DISTANCE, 'route_latency_ml'
  FROM TABLE(
    GRIZL.OBSERVABILITY.ROUTE_LATENCY_ANOMALY_MODEL!DETECT_ANOMALIES(
      INPUT_DATA        => SYSTEM$REFERENCE('VIEW', 'GRIZL.OBSERVABILITY.ML_DETECT_LATENCY_INPUT'),
      SERIES_COLNAME    => 'SERIES_KEY',
      TIMESTAMP_COLNAME => 'TIME_BIN',
      TARGET_COLNAME    => 'P95_DURATION_MS'
    )
  ) r
  WHERE r.IS_ANOMALY = TRUE
    AND r.TS >= DATEADD('minute', -15, CURRENT_TIMESTAMP());

  RETURN 'OK';
END;
$$;


-- ── SCHEDULED TASKS ──────────────────────────────────────────────────────────
-- Run detection procedures every 5 minutes.
-- Resume tasks after creating them: ALTER TASK ... RESUME;

CREATE OR REPLACE TASK GRIZL.OBSERVABILITY.TASK_DETECT_ERROR_RATE
  WAREHOUSE = GRIZL_WH
  SCHEDULE  = '5 MINUTES'
  COMMENT   = 'Runs HTTP error rate Cortex ML anomaly detection every 5 minutes.'
AS
CALL GRIZL.OBSERVABILITY.SP_DETECT_HTTP_ERROR_RATE();

CREATE OR REPLACE TASK GRIZL.OBSERVABILITY.TASK_DETECT_ROUTE_LATENCY
  WAREHOUSE = GRIZL_WH
  SCHEDULE  = '5 MINUTES'
  COMMENT   = 'Runs route latency Cortex ML anomaly detection every 5 minutes.'
AS
CALL GRIZL.OBSERVABILITY.SP_DETECT_ROUTE_LATENCY();

-- Resume both tasks after creation:
-- ALTER TASK GRIZL.OBSERVABILITY.TASK_DETECT_ERROR_RATE RESUME;
-- ALTER TASK GRIZL.OBSERVABILITY.TASK_DETECT_ROUTE_LATENCY RESUME;


-- ── RECENT ANOMALY SIGNALS (ML PATH) ─────────────────────────────────────────
-- Reads from ML_ANOMALY_DETECTIONS (written by the Tasks above).
-- Use this view instead of GRIZL_RECENT_ANOMALY_SIGNALS when Cortex ML
-- models are trained and Tasks are active.
-- Wire this view into the Snowflake Alert (sql/grizl-alert-queries.sql).

CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.GRIZL_RECENT_ANOMALY_SIGNALS_ML
  COMMENT = 'Cortex ML anomaly signals from the last 15 minutes. Alternative to GRIZL_RECENT_ANOMALY_SIGNALS when ML models are active.'
AS
SELECT
  TS             AS TIME_BIN,
  SIGNAL_TYPE,
  SERVICE,
  ROUTE,
  NULL::NUMBER   AS REQUESTS,
  NULL::FLOAT    AS ERRORS,
  Y              AS ACTUAL,
  FORECAST       AS BASELINE_MEAN,
  NULL::FLOAT    AS BASELINE_STDDEV,
  DISTANCE       AS ANOMALY_SCORE,
  MODEL_NAME,
  IS_ANOMALY,
  PERCENTILE
FROM GRIZL.OBSERVABILITY.ML_ANOMALY_DETECTIONS
WHERE TS >= CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
  AND IS_ANOMALY = TRUE
ORDER BY DISTANCE DESC;
