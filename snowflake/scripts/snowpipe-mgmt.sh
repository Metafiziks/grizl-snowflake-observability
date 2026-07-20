#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "${SCRIPT_DIR}/lib.sh"

usage() {
  cat <<'USAGE'
Usage: bash snowflake/scripts/snowpipe-mgmt.sh [options] <command>

Commands:
  status    Show Snowpipe status and recent ingest history
  pause     Pause the RAW_LOGS_PIPE
  resume    Resume the RAW_LOGS_PIPE
  refresh   Manually refresh (re-scan GCS stage for new files)
  show-svc  Show the storage integration service account (grant this to GCS bucket)
  errors    Show recent COPY errors for RAW_LOGS_PIPE

USAGE
  usage_common
}

COMMAND=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --config)  [ "$#" -ge 2 ] || die "--config requires a path"; CONFIG_FILE="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --yes)     YES=true; shift ;;
    status|pause|resume|refresh|show-svc|errors)
      COMMAND="$1"; shift ;;
    -h|--help) usage; exit 0 ;;
    *)         die "Unknown option/command: $1" ;;
  esac
done

[ -n "${COMMAND}" ] || { usage; exit 1; }

load_env_file
require_snow
require_snow_auth

DATABASE="${SNOWFLAKE_DATABASE:-grizl}"
SCHEMA="${SNOWFLAKE_SCHEMA:-observability}"

case "${COMMAND}" in
  status)
    snow_exec "SHOW PIPES LIKE 'RAW_LOGS_PIPE' IN SCHEMA ${DATABASE}.${SCHEMA};" "Pipe status"
    snow_exec "
      SELECT SYSTEM\$PIPE_STATUS('${DATABASE}.${SCHEMA}.RAW_LOGS_PIPE');
    " "Pipe system status"
    ;;
  pause)
    ensure_mutation_allowed
    snow_exec "ALTER PIPE ${DATABASE}.${SCHEMA}.RAW_LOGS_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;" "Pausing pipe"
    ;;
  resume)
    ensure_mutation_allowed
    snow_exec "ALTER PIPE ${DATABASE}.${SCHEMA}.RAW_LOGS_PIPE SET PIPE_EXECUTION_PAUSED = FALSE;" "Resuming pipe"
    ;;
  refresh)
    ensure_mutation_allowed
    snow_exec "ALTER PIPE ${DATABASE}.${SCHEMA}.RAW_LOGS_PIPE REFRESH;" "Refreshing pipe (manual GCS scan)"
    ;;
  show-svc)
    snow_exec "DESC INTEGRATION GRIZL_GCS_INTEGRATION;" "Storage integration details"
    warn "Grant the STORAGE_GCP_SERVICE_ACCOUNT value objectViewer on your GCS bucket:"
    warn "  gsutil iam ch serviceAccount:<VALUE>:objectViewer gs://<GCS_LOGS_BUCKET>"
    ;;
  errors)
    snow_exec "
      SELECT
        LAST_LOAD_TIME,
        STATUS,
        FIRST_ERROR_MESSAGE,
        FILE_NAME,
        ROW_COUNT,
        ERROR_COUNT
      FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => '${DATABASE}.${SCHEMA}.RAW_LOGS',
        START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
      ))
      WHERE STATUS = 'Load failed'
      ORDER BY LAST_LOAD_TIME DESC
      LIMIT 25;
    " "Recent COPY errors"
    ;;
esac
