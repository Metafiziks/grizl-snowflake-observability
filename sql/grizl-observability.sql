-- =============================================================================
-- GRIZL Snowflake Observability — GRIZL.OBSERVABILITY
-- Source table: GRIZL.OBSERVABILITY.RAW_LOGS (Snowpipe AUTO_INGEST from GCS)
-- Database: GRIZL  |  Schema: OBSERVABILITY
-- =============================================================================
-- How to apply:
--   snow --connection grizl sql -f sql/grizl-observability.sql
--   npm --prefix snowflake run sql:observability:dry-run
--   npm --prefix snowflake run sql:observability
--
-- Each DDL block is separated by a blank line. If your Snowflake edition does
-- not support multi-statement execution in a single snow sql call, run each
-- block individually or use the sql-exec.sh helper (--statement mode).
--
-- Requires: SYSADMIN or GRIZL_ROLE with CREATE DATABASE / CREATE SCHEMA /
-- CREATE TABLE / CREATE STAGE / CREATE PIPE / CREATE VIEW privileges.
-- Run provision.sh first to create the database, schema, warehouse, and roles.
-- =============================================================================


-- ── DATABASE AND SCHEMA ──────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS GRIZL
  DATA_RETENTION_TIME_IN_DAYS = 7
  COMMENT = 'GRIZL application observability and analytics.';

CREATE SCHEMA IF NOT EXISTS GRIZL.OBSERVABILITY
  DATA_RETENTION_TIME_IN_DAYS = 7
  COMMENT = 'GRIZL application observability — raw logs, views, and anomaly signals.';

CREATE SCHEMA IF NOT EXISTS GRIZL.KNOWLEDGE
  DATA_RETENTION_TIME_IN_DAYS = 30
  COMMENT = 'Runbooks, postmortems, and knowledge articles for Cortex Search.';


-- ── GCS STORAGE INTEGRATION ──────────────────────────────────────────────────
-- Required before creating the stage.
-- Must be created by ACCOUNTADMIN; the service account email emitted here must
-- be granted objectViewer on the GCS bucket (see provision.sh).
--
-- Replace <GCS_LOGS_BUCKET> with your bucket name before running.

CREATE OR REPLACE STORAGE INTEGRATION GRIZL_GCS_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://<GCS_LOGS_BUCKET>/logs/');


-- ── GCS STAGE ────────────────────────────────────────────────────────────────

CREATE OR REPLACE STAGE GRIZL.OBSERVABILITY.GCS_LOGS_STAGE
  URL = 'gcs://<GCS_LOGS_BUCKET>/logs/'
  STORAGE_INTEGRATION = GRIZL_GCS_INTEGRATION
  FILE_FORMAT = (
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
    NULL_IF = ('NULL', 'null', '')
  )
  COMMENT = 'GCS stage for Pub/Sub Cloud Storage export subscription JSONL files.';


-- ── RAW LOGS TABLE ───────────────────────────────────────────────────────────
-- Normally populated by the Snowpipe pipe below (AUTO_INGEST from GCS).
-- Run this DDL once to create the table before enabling the pipe.
-- Clustered by day for efficient time-range scans. VARIANT RAW_ENVELOPE stores
-- the full Pub/Sub envelope for message_skipped events and backfill debugging.

CREATE TABLE IF NOT EXISTS GRIZL.OBSERVABILITY.RAW_LOGS (
  INGEST_TIMESTAMP    TIMESTAMP_LTZ NOT NULL   COMMENT 'Timestamp when the event landed in the Snowflake table',
  SOURCE_TIMESTAMP    TIMESTAMP_LTZ            COMMENT 'Original event timestamp from the application',
  SERVICE             VARCHAR                  COMMENT 'Service name: grizl-backend, grizl-frontend, grizl-log-forwarder',
  ENVIRONMENT         VARCHAR                  COMMENT 'Deployment environment: production, staging',
  DEPLOYMENT_SHA      VARCHAR                  COMMENT 'Git commit SHA of the running deployment',
  SEVERITY            VARCHAR                  COMMENT 'Log severity: DEBUG, INFO, WARNING, ERROR, CRITICAL',
  EVENT_TYPE          VARCHAR                  COMMENT 'Structured event type: http_request, deployment, forwarder_start, batch_sent, ...',
  METHOD              VARCHAR                  COMMENT 'HTTP method for http_request events',
  ROUTE               VARCHAR                  COMMENT 'HTTP route path for http_request events',
  STATUS_CODE         NUMBER(5,0)              COMMENT 'HTTP status code for http_request events',
  DURATION_MS         FLOAT                    COMMENT 'Request duration in milliseconds for http_request events',
  TRACE_ID            VARCHAR                  COMMENT 'Distributed trace ID',
  REQUEST_ID          VARCHAR                  COMMENT 'HTTP request ID',
  ERROR_TYPE          VARCHAR                  COMMENT 'Exception class or error category',
  ERROR_MESSAGE       VARCHAR                  COMMENT 'Error message text',
  ERROR_SIGNATURE     VARCHAR                  COMMENT 'Stable error key: <errorType>:<route>',
  PAGE                VARCHAR                  COMMENT 'Browser page path for grizl-frontend events',
  API_STATUS          VARCHAR                  COMMENT 'API response status for frontend telemetry events',
  SOURCE              VARCHAR                  COMMENT 'Event source identifier',
  RAW_ENVELOPE        VARIANT                  COMMENT 'Raw Pub/Sub envelope (VARIANT) for message_skipped events',
  INSERT_ID           VARCHAR                  COMMENT 'Cloud Logging insertId for deduplication',
  PUBSUB_MESSAGE_ID   VARCHAR                  COMMENT 'Pub/Sub messageId'
)
CLUSTER BY (DATE_TRUNC('day', INGEST_TIMESTAMP))
DATA_RETENTION_TIME_IN_DAYS = 30
COMMENT = 'GRIZL application log events. Populated by Snowpipe AUTO_INGEST from GCS Pub/Sub export. Equivalent to the Databricks grizl.observability.raw_logs Delta table.';


-- ── SNOWPIPE (AUTO_INGEST) ────────────────────────────────────────────────────
-- AUTO_INGEST = TRUE listens for GCS Object Finalize notifications on the bucket.
-- This is a MANUAL STEP: create the pipe only after completing GCS setup:
--   1. Create a GCS bucket and Pub/Sub notification topic for Snowpipe
--   2. Grant the storage integration service account objectViewer on the bucket:
--        DESCRIBE INTEGRATION GRIZL_GCS_INTEGRATION;
--        -- copy STORAGE_GCP_SERVICE_ACCOUNT value, then:
--        gsutil iam ch serviceAccount:<SA>:objectViewer gs://<GCS_LOGS_BUCKET>
--   3. Grant the service account roles/pubsub.subscriber on the notification topic
--   4. Then run the CREATE PIPE statement below
--
-- See: snowflake/manifests/snowpipe-config.example.json for full setup steps.
--
-- CREATE OR REPLACE PIPE GRIZL.OBSERVABILITY.RAW_LOGS_PIPE
--   AUTO_INGEST = TRUE
--   COMMENT = 'Snowpipe AUTO_INGEST from GCS Pub/Sub Cloud Storage export subscription.'
-- AS
-- COPY INTO GRIZL.OBSERVABILITY.RAW_LOGS (
--   INGEST_TIMESTAMP, SOURCE_TIMESTAMP, SERVICE, ENVIRONMENT, DEPLOYMENT_SHA,
--   SEVERITY, EVENT_TYPE, METHOD, ROUTE, STATUS_CODE, DURATION_MS, TRACE_ID,
--   REQUEST_ID, ERROR_TYPE, ERROR_MESSAGE, ERROR_SIGNATURE, PAGE, API_STATUS,
--   SOURCE, INSERT_ID, PUBSUB_MESSAGE_ID
-- )
-- FROM (
--   SELECT
--     COALESCE(TRY_TO_TIMESTAMP_LTZ($1:ingestTimestamp::STRING), CURRENT_TIMESTAMP()) AS INGEST_TIMESTAMP,
--     TRY_TO_TIMESTAMP_LTZ($1:sourceTimestamp::STRING)    AS SOURCE_TIMESTAMP,
--     $1:service::VARCHAR                                 AS SERVICE,
--     $1:environment::VARCHAR                             AS ENVIRONMENT,
--     $1:deploymentSha::VARCHAR                           AS DEPLOYMENT_SHA,
--     $1:severity::VARCHAR                                AS SEVERITY,
--     $1:eventType::VARCHAR                               AS EVENT_TYPE,
--     $1:httpRequest:method::VARCHAR                      AS METHOD,
--     $1:httpRequest:route::VARCHAR                       AS ROUTE,
--     TRY_TO_NUMBER($1:httpRequest:statusCode::VARCHAR)   AS STATUS_CODE,
--     TRY_TO_DOUBLE($1:httpRequest:durationMs::VARCHAR)   AS DURATION_MS,
--     $1:traceId::VARCHAR                                 AS TRACE_ID,
--     $1:requestId::VARCHAR                               AS REQUEST_ID,
--     $1:error:errorType::VARCHAR                         AS ERROR_TYPE,
--     $1:error:message::VARCHAR                           AS ERROR_MESSAGE,
--     $1:error:errorSignature::VARCHAR                    AS ERROR_SIGNATURE,
--     $1:page::VARCHAR                                    AS PAGE,
--     $1:apiStatus::VARCHAR                               AS API_STATUS,
--     $1:source::VARCHAR                                  AS SOURCE,
--     $1:insertId::VARCHAR                                AS INSERT_ID,
--     $1:pubsubMessageId::VARCHAR                         AS PUBSUB_MESSAGE_ID
--   FROM @GRIZL.OBSERVABILITY.GCS_LOGS_STAGE
-- )
-- FILE_FORMAT = (TYPE = 'JSON');


-- ── KNOWLEDGE TABLE (Cortex Search source) ────────────────────────────────────
-- Stores runbooks, postmortems, and architecture notes for Cortex Search.
-- The external orchestrator queries this via the Cortex Search Service REST API
-- to enrich incident evidence with historical context.

CREATE TABLE IF NOT EXISTS GRIZL.KNOWLEDGE.ARTICLES (
  ARTICLE_ID    VARCHAR       NOT NULL  COMMENT 'Unique article identifier (slug or UUID)',
  TITLE         VARCHAR                 COMMENT 'Article title',
  CATEGORY      VARCHAR                 COMMENT 'postmortem, runbook, architecture, faq',
  SERVICE       VARCHAR                 COMMENT 'Related service (NULL = cross-service)',
  TAGS          ARRAY                   COMMENT 'Array of tag strings for retrieval',
  BODY          VARCHAR                 COMMENT 'Full article text — indexed by Cortex Search',
  AUTHORED_AT   TIMESTAMP_LTZ           COMMENT 'Original authoring timestamp',
  UPDATED_AT    TIMESTAMP_LTZ           COMMENT 'Last update timestamp',
  PRIMARY KEY (ARTICLE_ID)
)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Runbooks, postmortems, and knowledge articles. Source for the Cortex Search Service (GRIZL.KNOWLEDGE.ARTICLE_SEARCH_SVC).';


-- ── CORTEX SEARCH SERVICE ────────────────────────────────────────────────────
-- Indexes ARTICLES.BODY for semantic search over postmortems and runbooks.
-- Used by the external orchestrator Cortex Agent as the Search tool.
-- The TARGET_LAG controls how quickly new articles appear in search results.
--
CREATE OR REPLACE CORTEX SEARCH SERVICE GRIZL.KNOWLEDGE.ARTICLE_SEARCH_SVC
  ON BODY
  ATTRIBUTES ARTICLE_ID, TITLE, CATEGORY, SERVICE, TAGS
  WAREHOUSE = GRIZL_WH
  TARGET_LAG = '1 hour'
AS (
  SELECT ARTICLE_ID, TITLE, CATEGORY, SERVICE, TAGS, BODY
  FROM GRIZL.KNOWLEDGE.ARTICLES
  WHERE BODY IS NOT NULL
);


-- ── LOGICAL VIEWS ─────────────────────────────────────────────────────────────
-- These are the Snowflake equivalents of the KQL functions in grizl-fabric-observability
-- and the SQL views in grizl-databricks-observability. They query RAW_LOGS
-- and are used by anomaly-signal views, dashboard tiles, and Cortex Analyst.


CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.HTTP_REQUESTS
  COMMENT = 'HTTP request events from grizl-backend and grizl-frontend. Equivalent to KQL HttpRequests() and Databricks http_requests view.'
AS
SELECT
  INGEST_TIMESTAMP,
  SOURCE_TIMESTAMP,
  SERVICE,
  ENVIRONMENT,
  DEPLOYMENT_SHA,
  SEVERITY,
  METHOD,
  ROUTE,
  STATUS_CODE,
  DURATION_MS,
  TRACE_ID,
  REQUEST_ID
FROM GRIZL.OBSERVABILITY.RAW_LOGS
WHERE EVENT_TYPE = 'http_request'
  AND SERVICE IS NOT NULL
  AND ROUTE IS NOT NULL;


CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.APPLICATION_ERRORS
  COMMENT = 'Application error events with non-null error signatures. Equivalent to KQL ApplicationErrors() and Databricks application_errors view.'
AS
SELECT
  INGEST_TIMESTAMP,
  SOURCE_TIMESTAMP,
  SERVICE,
  ENVIRONMENT,
  DEPLOYMENT_SHA,
  SEVERITY,
  ERROR_TYPE,
  ERROR_MESSAGE,
  ERROR_SIGNATURE,
  ROUTE,
  TRACE_ID,
  REQUEST_ID
FROM GRIZL.OBSERVABILITY.RAW_LOGS
WHERE SEVERITY IN ('ERROR', 'CRITICAL')
  AND ERROR_SIGNATURE IS NOT NULL;


CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.FRONTEND_TELEMETRY
  COMMENT = 'Frontend browser telemetry from grizl-frontend. Equivalent to KQL FrontendTelemetry().'
AS
SELECT
  INGEST_TIMESTAMP,
  SOURCE_TIMESTAMP,
  SERVICE,
  ENVIRONMENT,
  PAGE,
  API_STATUS,
  DURATION_MS,
  TRACE_ID
FROM GRIZL.OBSERVABILITY.RAW_LOGS
WHERE SERVICE = 'grizl-frontend';


CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.DEPLOYMENTS
  COMMENT = 'Deployment events (service startup and version changes). Equivalent to KQL Deployments() and Databricks deployments view.'
AS
SELECT
  INGEST_TIMESTAMP,
  SOURCE_TIMESTAMP,
  SERVICE,
  ENVIRONMENT,
  DEPLOYMENT_SHA,
  SEVERITY,
  SOURCE
FROM GRIZL.OBSERVABILITY.RAW_LOGS
WHERE EVENT_TYPE = 'deployment'
   OR (EVENT_TYPE = 'forwarder_start' AND DEPLOYMENT_SHA IS NOT NULL);


CREATE OR REPLACE VIEW GRIZL.OBSERVABILITY.FORWARDER_HEALTH
  COMMENT = 'Log forwarder operational events: batch_sent, message_skipped, startup, errors. Equivalent to KQL ForwarderHealth() and Databricks forwarder_health view.'
AS
SELECT
  INGEST_TIMESTAMP,
  SOURCE_TIMESTAMP,
  SERVICE,
  ENVIRONMENT,
  EVENT_TYPE,
  SEVERITY,
  SOURCE
FROM GRIZL.OBSERVABILITY.RAW_LOGS
WHERE SERVICE = 'grizl-log-forwarder'
   OR EVENT_TYPE IN ('forwarder_start', 'batch_sent', 'message_skipped');
