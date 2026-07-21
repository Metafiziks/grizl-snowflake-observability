-- =============================================================================
-- GRIZL Snowflake Observability — Snowflake Alerts and External Notifications
-- =============================================================================
-- Snowflake Alerts monitor a condition query on a schedule and execute a THEN
-- action when the condition is met (EXISTS returns at least one row).
--
-- Alert trigger path:
--   GRIZL_ANOMALY_ALERT (every 5 min)
--     → IF EXISTS in GRIZL_RECENT_ANOMALY_SIGNALS
--     → THEN CALL GRIZL.OBSERVABILITY.SP_NOTIFY_ANOMALY_INCIDENT()
--     → SP calls GRIZL_ORCHESTRATOR_WEBHOOK external function
--     → External orchestrator: Cortex Agent evidence → GitHub issue → Copilot
--
-- Switch to the Cortex ML path after training models:
--   Replace GRIZL_RECENT_ANOMALY_SIGNALS with GRIZL_RECENT_ANOMALY_SIGNALS_ML
--   in the Alert IF clause.
--
-- How to apply:
--   npm --prefix snowflake run sql:alert-queries:dry-run
--   npm --prefix snowflake run sql:alert-queries
--
-- Requires:
--   - Anomaly signal views from sql/grizl-anomaly-signals.sql
--   - GRIZL_ORCHESTRATOR_INTEGRATION external function (see provision.sh)
--   - Alert must be manually RESUMEd after creation (Snowflake default: SUSPENDED)
-- =============================================================================


-- ── EXTERNAL NETWORK ACCESS FOR ORCHESTRATOR WEBHOOK ─────────────────────────
-- Allows Snowflake to call the external incident orchestrator via HTTPS.
-- REQUIRES: Snowflake non-trial account (External Access not available on trial).
-- After upgrading, replace your-orchestrator.example.com and uncomment:
--
-- CREATE OR REPLACE NETWORK RULE GRIZL_ORCHESTRATOR_NETWORK_RULE
--   MODE = EGRESS TYPE = HOST_PORT
--   VALUE_LIST = ('your-orchestrator.example.com:443')
--   COMMENT = 'Allows Snowflake stored procedures to call the incident orchestrator webhook.';
--
-- CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GRIZL_ORCHESTRATOR_INTEGRATION
--   ALLOWED_NETWORK_RULES = (GRIZL_ORCHESTRATOR_NETWORK_RULE)
--   ENABLED = TRUE
--   COMMENT = 'External access integration for the GRIZL incident orchestrator webhook.';


-- ── NOTIFICATION STORED PROCEDURE (production — requires external access) ─────
-- Called by the Alert THEN clause on non-trial accounts. POSTs to the orchestrator.
-- Uncomment after creating GRIZL_ORCHESTRATOR_INTEGRATION and ORCHESTRATOR_WEBHOOK_SECRET.
--
-- CREATE OR REPLACE PROCEDURE GRIZL.OBSERVABILITY.SP_NOTIFY_ANOMALY_INCIDENT()
-- RETURNS VARCHAR LANGUAGE PYTHON RUNTIME_VERSION = '3.11'
-- PACKAGES = ('snowflake-snowpark-python', 'requests') HANDLER = 'handler'
-- EXTERNAL_ACCESS_INTEGRATIONS = (GRIZL_ORCHESTRATOR_INTEGRATION)
-- SECRETS = ('webhook_url' = GRIZL.OBSERVABILITY.ORCHESTRATOR_WEBHOOK_SECRET)
-- COMMENT = 'Fetches top anomaly signals and POSTs to the incident orchestrator webhook.'
-- AS $$
-- import requests, json, _snowflake
-- from datetime import datetime
-- def handler(session):
--     webhook_url = _snowflake.get_generic_secret_string('webhook_url')
--     signals_df = session.sql("""
--         SELECT SIGNAL_TYPE, SERVICE, ROUTE,
--           REQUESTS, ERRORS, ACTUAL, BASELINE_MEAN, BASELINE_STDDEV, ANOMALY_SCORE,
--           TO_CHAR(TIME_BIN, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS TIME_BIN
--         FROM GRIZL.OBSERVABILITY.GRIZL_RECENT_ANOMALY_SIGNALS
--         WHERE ANOMALY_SCORE >= 1.5 ORDER BY ANOMALY_SCORE DESC LIMIT 10
--     """).collect()
--     if not signals_df: return 'no_anomalies'
--     signals = [row.as_dict() for row in signals_df]
--     resp = requests.post(webhook_url,
--         json={'source':'snowflake_alert','fired_at':datetime.utcnow().isoformat()+'Z','signals':signals,'top_signal':signals[0]},
--         timeout=15, headers={'Content-Type':'application/json','X-Grizl-Source':'snowflake'})
--     resp.raise_for_status()
--     return f'notified: {resp.status_code}'
-- $$;
--
-- CREATE SECRET IF NOT EXISTS GRIZL.OBSERVABILITY.ORCHESTRATOR_WEBHOOK_SECRET
--   TYPE = GENERIC_STRING
--   SECRET_STRING = 'https://your-orchestrator.example.com/api/snowflake/incidents'
--   COMMENT = 'Incident orchestrator webhook URL. Replace with your actual endpoint.';


-- ── ALERT LOG TABLE (trial-safe fallback) ────────────────────────────────────
-- On trial accounts (no External Access), the alert writes fired events here
-- instead of calling the orchestrator. Replace the alert THEN clause with
-- SP_NOTIFY_ANOMALY_INCIDENT() after upgrading to a non-trial account.

CREATE TABLE IF NOT EXISTS GRIZL.OBSERVABILITY.ALERT_LOG (
  FIRED_AT      TIMESTAMP_LTZ NOT NULL,
  SIGNAL_COUNT  NUMBER,
  TOP_SIGNAL    VARCHAR,
  TOP_SERVICE   VARCHAR,
  TOP_ROUTE     VARCHAR,
  TOP_SCORE     FLOAT
)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Audit log of GRIZL_ANOMALY_ALERT firings. Used on trial accounts instead of the external orchestrator webhook.';


-- ── NOTIFICATION STORED PROCEDURE (trial fallback — SQL only, no external access) ──
-- Writes the top anomaly signal to ALERT_LOG. Swap for SP_NOTIFY_ANOMALY_INCIDENT
-- once External Access Integration is available on the account.

CREATE OR REPLACE PROCEDURE GRIZL.OBSERVABILITY.SP_NOTIFY_ANOMALY_INCIDENT()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Trial fallback: logs top anomaly to ALERT_LOG instead of calling the orchestrator.'
AS
$$
DECLARE
  fired_ts   TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP();
  sig_count  NUMBER        DEFAULT 0;
  top_signal VARCHAR;
  top_svc    VARCHAR;
  top_route  VARCHAR;
  top_score  FLOAT;
BEGIN
  SELECT COUNT(*), MAX_BY(SIGNAL_TYPE, ANOMALY_SCORE),
         MAX_BY(SERVICE, ANOMALY_SCORE), MAX_BY(ROUTE, ANOMALY_SCORE),
         MAX(ANOMALY_SCORE)
    INTO :sig_count, :top_signal, :top_svc, :top_route, :top_score
  FROM GRIZL.OBSERVABILITY.GRIZL_RECENT_ANOMALY_SIGNALS
  WHERE ANOMALY_SCORE >= 1.5;

  INSERT INTO GRIZL.OBSERVABILITY.ALERT_LOG
    (FIRED_AT, SIGNAL_COUNT, TOP_SIGNAL, TOP_SERVICE, TOP_ROUTE, TOP_SCORE)
  VALUES (:fired_ts, :sig_count, :top_signal, :top_svc, :top_route, :top_score);

  RETURN 'logged: ' || :sig_count || ' signals, top=' || COALESCE(:top_signal, 'none');
END;
$$;


-- ── MAIN ANOMALY ALERT ───────────────────────────────────────────────────────
-- Monitors GRIZL_RECENT_ANOMALY_SIGNALS every 5 minutes.
-- If any row with ANOMALY_SCORE >= 1.5 exists, calls the notification procedure.
-- Resume after creation: ALTER ALERT GRIZL.OBSERVABILITY.GRIZL_ANOMALY_ALERT RESUME;

CREATE OR REPLACE ALERT GRIZL.OBSERVABILITY.GRIZL_ANOMALY_ALERT
  WAREHOUSE = GRIZL_WH
  SCHEDULE  = '5 MINUTES'
  IF (EXISTS (
    SELECT 1
    FROM GRIZL.OBSERVABILITY.GRIZL_RECENT_ANOMALY_SIGNALS
    WHERE ANOMALY_SCORE >= 1.5
    LIMIT 1
  ))
  THEN CALL GRIZL.OBSERVABILITY.SP_NOTIFY_ANOMALY_INCIDENT();

-- After creation:
-- ALTER ALERT GRIZL.OBSERVABILITY.GRIZL_ANOMALY_ALERT RESUME;


-- ── SUPPLEMENTARY ALERTS ─────────────────────────────────────────────────────
-- Optional targeted alerts for individual signal types, using email notification.
-- Requires a Snowflake email notification integration (create once as ACCOUNTADMIN).
-- Replace <ALLOWED_EMAIL> with the recipient address before running.

-- CREATE OR REPLACE NOTIFICATION INTEGRATION GRIZL_EMAIL_INTEGRATION
--   TYPE = EMAIL
--   ENABLED = TRUE
--   ALLOWED_RECIPIENTS = ('<ALLOWED_EMAIL>');
--
-- After creating the integration, uncomment and run Alerts 2-4 below.


-- Alert 2: Backend error rate > 5% in last 15 minutes
-- CREATE OR REPLACE ALERT GRIZL.OBSERVABILITY.BACKEND_ERROR_RATE_ALERT
--   WAREHOUSE = GRIZL_WH
--   SCHEDULE  = '5 MINUTES'
--   IF (EXISTS (
--     SELECT 1
--     FROM GRIZL.OBSERVABILITY.DT_HTTP_ERROR_RATE
--     WHERE TIME_BIN >= CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
--       AND ERROR_RATE > 0.05
--       AND REQUESTS >= 20
--     LIMIT 1
--   ))
--   THEN CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
--     SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(
--       'GRIZL backend error rate exceeded 5% in the last 15 minutes. Check GRIZL.OBSERVABILITY.DT_HTTP_ERROR_RATE.'
--     ),
--     SNOWFLAKE.NOTIFICATION.EMAIL_INTEGRATION_CONFIG(
--       'GRIZL_EMAIL_INTEGRATION',
--       'GRIZL Observability: Backend Error Rate Alert'
--     )
--   );

-- Alert 3: P95 latency > 5000ms on any route in last 15 minutes
-- CREATE OR REPLACE ALERT GRIZL.OBSERVABILITY.ROUTE_LATENCY_HIGH_ALERT
--   WAREHOUSE = GRIZL_WH
--   SCHEDULE  = '5 MINUTES'
--   IF (EXISTS (
--     SELECT 1
--     FROM GRIZL.OBSERVABILITY.DT_ROUTE_LATENCY
--     WHERE TIME_BIN >= CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
--       AND P95_DURATION_MS > 5000
--       AND REQUESTS >= 10
--     LIMIT 1
--   ))
--   THEN CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
--     SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(
--       'GRIZL route p95 latency exceeded 5000ms in the last 15 minutes. Check GRIZL.OBSERVABILITY.DT_ROUTE_LATENCY.'
--     ),
--     SNOWFLAKE.NOTIFICATION.EMAIL_INTEGRATION_CONFIG(
--       'GRIZL_EMAIL_INTEGRATION',
--       'GRIZL Observability: High Latency Alert'
--     )
--   );

-- Alert 4: Forwarder freshness drop (no batch_sent events in last 15 minutes)
-- CREATE OR REPLACE ALERT GRIZL.OBSERVABILITY.FORWARDER_SILENT_ALERT
--   WAREHOUSE = GRIZL_WH
--   SCHEDULE  = '5 MINUTES'
--   IF (NOT EXISTS (
--     SELECT 1
--     FROM GRIZL.OBSERVABILITY.FORWARDER_HEALTH
--     WHERE INGEST_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '15 minutes'
--       AND EVENT_TYPE = 'batch_sent'
--     LIMIT 1
--   ))
--   THEN CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
--     SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(
--       'GRIZL log forwarder has not sent any batches in the last 15 minutes. Check grizl-log-forwarder.'
--     ),
--     SNOWFLAKE.NOTIFICATION.EMAIL_INTEGRATION_CONFIG(
--       'GRIZL_EMAIL_INTEGRATION',
--       'GRIZL Observability: Forwarder Silent Alert'
--     )
--   );
