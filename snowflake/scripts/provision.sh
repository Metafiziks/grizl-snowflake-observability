#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "${SCRIPT_DIR}/lib.sh"

usage() {
  cat <<'USAGE'
Usage: bash snowflake/scripts/provision.sh [options] [targets]

Targets:
  --all             Provision all resources
  --database        Create the GRIZL database and schemas
  --warehouse       Create the GRIZL_WH warehouse
  --role            Create GRIZL_ROLE and grant privileges
  --observability   Apply base DDL (sql/grizl-observability.sql)
  --dynamic-tables  Apply Dynamic Table DDL (sql/grizl-dynamic-tables.sql)
  --anomaly-signals Apply anomaly signal views (sql/grizl-anomaly-signals.sql)
  --alerts          Apply alert queries (sql/grizl-alert-queries.sql)

USAGE
  usage_common
}

RUN_DATABASE=false
RUN_WAREHOUSE=false
RUN_ROLE=false
RUN_OBSERVABILITY=false
RUN_DT=false
RUN_SIGNALS=false
RUN_ALERTS=false

while [ "$#" -gt 0 ]; do
  case "$1" in
    --config)         [ "$#" -ge 2 ] || die "--config requires a path"; CONFIG_FILE="$2"; shift 2 ;;
    --dry-run)        DRY_RUN=true; shift ;;
    --yes)            YES=true; shift ;;
    --all)            RUN_DATABASE=true; RUN_WAREHOUSE=true; RUN_ROLE=true
                      RUN_OBSERVABILITY=true; RUN_DT=true; RUN_SIGNALS=true; RUN_ALERTS=true; shift ;;
    --database)       RUN_DATABASE=true; shift ;;
    --warehouse)      RUN_WAREHOUSE=true; shift ;;
    --role)           RUN_ROLE=true; shift ;;
    --observability)  RUN_OBSERVABILITY=true; shift ;;
    --dynamic-tables) RUN_DT=true; shift ;;
    --anomaly-signals) RUN_SIGNALS=true; shift ;;
    --alerts)         RUN_ALERTS=true; shift ;;
    -h|--help)        usage; exit 0 ;;
    *)                die "Unknown option: $1" ;;
  esac
done

if [ "${RUN_DATABASE}" = "false" ] && [ "${RUN_WAREHOUSE}" = "false" ] && \
   [ "${RUN_ROLE}" = "false" ] && [ "${RUN_OBSERVABILITY}" = "false" ] && \
   [ "${RUN_DT}" = "false" ] && [ "${RUN_SIGNALS}" = "false" ] && \
   [ "${RUN_ALERTS}" = "false" ]; then
  RUN_DATABASE=true; RUN_WAREHOUSE=true; RUN_ROLE=true; RUN_OBSERVABILITY=true
fi

load_env_file

if [ "${DRY_RUN}" != "true" ]; then
  require_snow
fi
require_snow_auth
ensure_mutation_allowed

DATABASE="${SNOWFLAKE_DATABASE:-grizl}"
SCHEMA="${SNOWFLAKE_SCHEMA:-observability}"
WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-grizl_wh}"
ROLE="${SNOWFLAKE_ROLE:-grizl_role}"

if [ "${RUN_WAREHOUSE}" = "true" ]; then
  snow_exec "
    CREATE WAREHOUSE IF NOT EXISTS ${WAREHOUSE}
      WAREHOUSE_SIZE = 'X-SMALL'
      AUTO_SUSPEND   = 60
      AUTO_RESUME    = TRUE
      COMMENT        = 'GRIZL observability warehouse for anomaly signal queries and Cortex ML tasks.';
  " "Creating warehouse ${WAREHOUSE}"
fi

if [ "${RUN_DATABASE}" = "true" ]; then
  snow_exec "
    CREATE DATABASE IF NOT EXISTS ${DATABASE}
      DATA_RETENTION_TIME_IN_DAYS = 7
      COMMENT = 'GRIZL application observability and analytics.';
    CREATE SCHEMA IF NOT EXISTS ${DATABASE}.${SCHEMA}
      DATA_RETENTION_TIME_IN_DAYS = 7
      COMMENT = 'GRIZL application observability — raw logs, views, and anomaly signals.';
    CREATE SCHEMA IF NOT EXISTS ${DATABASE}.KNOWLEDGE
      DATA_RETENTION_TIME_IN_DAYS = 30
      COMMENT = 'Runbooks, postmortems, and knowledge articles for Cortex Search.';
  " "Creating database and schemas"
fi

if [ "${RUN_ROLE}" = "true" ]; then
  snow_exec "
    CREATE ROLE IF NOT EXISTS ${ROLE}
      COMMENT = 'GRIZL observability service role — read/write on GRIZL database.';
    GRANT USAGE ON DATABASE ${DATABASE} TO ROLE ${ROLE};
    GRANT USAGE ON SCHEMA ${DATABASE}.${SCHEMA} TO ROLE ${ROLE};
    GRANT USAGE ON SCHEMA ${DATABASE}.KNOWLEDGE TO ROLE ${ROLE};
    GRANT CREATE TABLE, CREATE VIEW, CREATE DYNAMIC TABLE,
          CREATE PIPE, CREATE ALERT, CREATE TASK, CREATE PROCEDURE,
          CREATE STAGE, CREATE CORTEX SEARCH SERVICE
      ON SCHEMA ${DATABASE}.${SCHEMA} TO ROLE ${ROLE};
    GRANT CREATE TABLE, CREATE CORTEX SEARCH SERVICE
      ON SCHEMA ${DATABASE}.KNOWLEDGE TO ROLE ${ROLE};
    GRANT USAGE ON WAREHOUSE ${WAREHOUSE} TO ROLE ${ROLE};
    GRANT DATABASE ROLE SNOWFLAKE.ML_USER TO ROLE ${ROLE};
  " "Creating role and granting privileges"
fi

if [ "${RUN_OBSERVABILITY}" = "true" ]; then
  snow_exec_file "${REPO_ROOT}/sql/grizl-observability.sql" "Applying base observability DDL"
fi

if [ "${RUN_DT}" = "true" ]; then
  snow_exec_file "${REPO_ROOT}/sql/grizl-dynamic-tables.sql" "Applying Dynamic Tables"
fi

if [ "${RUN_SIGNALS}" = "true" ]; then
  snow_exec_file "${REPO_ROOT}/sql/grizl-anomaly-signals.sql" "Applying anomaly signal views"
fi

if [ "${RUN_ALERTS}" = "true" ]; then
  snow_exec_file "${REPO_ROOT}/sql/grizl-alert-queries.sql" "Applying alert queries"
  warn "Alerts are created in SUSPENDED state."
  warn "After setting ORCHESTRATOR_WEBHOOK_SECRET, run:"
  warn "  snow sql --connection grizl -q \"ALTER ALERT GRIZL.OBSERVABILITY.GRIZL_ANOMALY_ALERT RESUME;\""
fi

ok "Snowflake observability package provisioned."
ok "See snowflake/manifests/manual-resources.json for remaining manual steps."
