#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "${SCRIPT_DIR}/lib.sh"

usage() {
  cat <<'USAGE'
Usage: bash snowflake/scripts/teardown.sh [options] [targets]

Removes GRIZL Snowflake observability resources.
DESTRUCTIVE. Requires --yes to execute.

Targets:
  --alerts          Suspend and drop all GRIZL alerts and tasks
  --views           Drop anomaly signal views
  --dynamic-tables  Drop Dynamic Tables (DT_*)
  --pipe            Pause and drop RAW_LOGS_PIPE
  --table           Drop RAW_LOGS table (data loss)
  --schema          Drop GRIZL.OBSERVABILITY schema (all objects, data loss)
  --all             All of the above (most destructive, requires --yes)

USAGE
  usage_common
}

RUN_ALERTS=false
RUN_VIEWS=false
RUN_DT=false
RUN_PIPE=false
RUN_TABLE=false
RUN_SCHEMA=false

while [ "$#" -gt 0 ]; do
  case "$1" in
    --config)        [ "$#" -ge 2 ] || die "--config requires a path"; CONFIG_FILE="$2"; shift 2 ;;
    --dry-run)       DRY_RUN=true; shift ;;
    --yes)           YES=true; shift ;;
    --alerts)        RUN_ALERTS=true; shift ;;
    --views)         RUN_VIEWS=true; shift ;;
    --dynamic-tables) RUN_DT=true; shift ;;
    --pipe)          RUN_PIPE=true; shift ;;
    --table)         RUN_TABLE=true; shift ;;
    --schema)        RUN_SCHEMA=true; shift ;;
    --all)           RUN_ALERTS=true; RUN_VIEWS=true; RUN_DT=true
                     RUN_PIPE=true; RUN_TABLE=true; RUN_SCHEMA=true; shift ;;
    -h|--help)       usage; exit 0 ;;
    *)               die "Unknown option: $1" ;;
  esac
done

load_env_file
require_snow
require_snow_auth
ensure_mutation_allowed

DATABASE="${SNOWFLAKE_DATABASE:-grizl}"
SCHEMA="${SNOWFLAKE_SCHEMA:-observability}"

if [ "${RUN_ALERTS}" = "true" ]; then
  snow_exec "
    ALTER ALERT IF EXISTS ${DATABASE}.${SCHEMA}.GRIZL_ANOMALY_ALERT SUSPEND;
    DROP ALERT IF EXISTS ${DATABASE}.${SCHEMA}.GRIZL_ANOMALY_ALERT;
    DROP ALERT IF EXISTS ${DATABASE}.${SCHEMA}.BACKEND_ERROR_RATE_ALERT;
    DROP ALERT IF EXISTS ${DATABASE}.${SCHEMA}.ROUTE_LATENCY_HIGH_ALERT;
    DROP ALERT IF EXISTS ${DATABASE}.${SCHEMA}.FORWARDER_SILENT_ALERT;
    ALTER TASK IF EXISTS ${DATABASE}.${SCHEMA}.TASK_DETECT_ERROR_RATE SUSPEND;
    DROP TASK IF EXISTS ${DATABASE}.${SCHEMA}.TASK_DETECT_ERROR_RATE;
    ALTER TASK IF EXISTS ${DATABASE}.${SCHEMA}.TASK_DETECT_ROUTE_LATENCY SUSPEND;
    DROP TASK IF EXISTS ${DATABASE}.${SCHEMA}.TASK_DETECT_ROUTE_LATENCY;
  " "Suspending and dropping alerts and tasks"
fi

if [ "${RUN_VIEWS}" = "true" ]; then
  snow_exec "
    DROP VIEW IF EXISTS ${DATABASE}.${SCHEMA}.GRIZL_RECENT_ANOMALY_SIGNALS;
    DROP VIEW IF EXISTS ${DATABASE}.${SCHEMA}.GRIZL_RECENT_ANOMALY_SIGNALS_ML;
    DROP VIEW IF EXISTS ${DATABASE}.${SCHEMA}.BACKEND_HTTP_ERROR_RATE_ANOMALIES;
    DROP VIEW IF EXISTS ${DATABASE}.${SCHEMA}.ROUTE_LATENCY_ANOMALIES;
    DROP VIEW IF EXISTS ${DATABASE}.${SCHEMA}.ERROR_SIGNATURE_SPIKE_ANOMALIES;
    DROP VIEW IF EXISTS ${DATABASE}.${SCHEMA}.FORWARDER_FRESHNESS_DROP_ANOMALIES;
    DROP VIEW IF EXISTS ${DATABASE}.${SCHEMA}.FORWARDER_DROP_FAILURE_ANOMALIES;
    DROP VIEW IF EXISTS ${DATABASE}.${SCHEMA}.POST_DEPLOYMENT_REGRESSION_ANOMALIES;
    DROP VIEW IF EXISTS ${DATABASE}.${SCHEMA}.HTTP_REQUESTS;
    DROP VIEW IF EXISTS ${DATABASE}.${SCHEMA}.APPLICATION_ERRORS;
    DROP VIEW IF EXISTS ${DATABASE}.${SCHEMA}.FRONTEND_TELEMETRY;
    DROP VIEW IF EXISTS ${DATABASE}.${SCHEMA}.DEPLOYMENTS;
    DROP VIEW IF EXISTS ${DATABASE}.${SCHEMA}.FORWARDER_HEALTH;
  " "Dropping views"
fi

if [ "${RUN_DT}" = "true" ]; then
  snow_exec "
    DROP DYNAMIC TABLE IF EXISTS ${DATABASE}.${SCHEMA}.DT_HTTP_ERROR_RATE;
    DROP DYNAMIC TABLE IF EXISTS ${DATABASE}.${SCHEMA}.DT_ROUTE_LATENCY;
    DROP DYNAMIC TABLE IF EXISTS ${DATABASE}.${SCHEMA}.DT_ERROR_SIGNATURES;
    DROP DYNAMIC TABLE IF EXISTS ${DATABASE}.${SCHEMA}.DT_FORWARDER_FRESHNESS;
    DROP DYNAMIC TABLE IF EXISTS ${DATABASE}.${SCHEMA}.DT_FORWARDER_DROPS;
    DROP DYNAMIC TABLE IF EXISTS ${DATABASE}.${SCHEMA}.DT_DEPLOYMENT_ERROR_RATE;
  " "Dropping Dynamic Tables"
fi

if [ "${RUN_PIPE}" = "true" ]; then
  snow_exec "
    ALTER PIPE IF EXISTS ${DATABASE}.${SCHEMA}.RAW_LOGS_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
    DROP PIPE IF EXISTS ${DATABASE}.${SCHEMA}.RAW_LOGS_PIPE;
    DROP STAGE IF EXISTS ${DATABASE}.${SCHEMA}.GCS_LOGS_STAGE;
  " "Pausing and dropping pipe and stage"
fi

if [ "${RUN_TABLE}" = "true" ]; then
  warn "Dropping RAW_LOGS table — DATA LOSS"
  snow_exec "DROP TABLE IF EXISTS ${DATABASE}.${SCHEMA}.RAW_LOGS;" "Dropping RAW_LOGS table"
fi

if [ "${RUN_SCHEMA}" = "true" ]; then
  warn "Dropping ${DATABASE}.${SCHEMA} schema — ALL DATA LOSS"
  snow_exec "DROP SCHEMA IF EXISTS ${DATABASE}.${SCHEMA} CASCADE;" "Dropping schema"
fi

ok "Teardown complete."
