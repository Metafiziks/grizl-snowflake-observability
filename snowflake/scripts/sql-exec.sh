#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "${SCRIPT_DIR}/lib.sh"

usage() {
  cat <<'USAGE'
Usage: bash snowflake/scripts/sql-exec.sh [options]

Options:
  --statement <sql>   Execute a single SQL statement
  --file <path>       Execute a SQL file
  --dry-run           Print the statement without executing
  --yes               Required for live execution
USAGE
  usage_common
}

SQL_STATEMENT=""
SQL_FILE=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --config)    [ "$#" -ge 2 ] || die "--config requires a path"; CONFIG_FILE="$2"; shift 2 ;;
    --dry-run)   DRY_RUN=true; shift ;;
    --yes)       YES=true; shift ;;
    --statement) [ "$#" -ge 2 ] || die "--statement requires SQL text"; SQL_STATEMENT="$2"; shift 2 ;;
    --file)      [ "$#" -ge 2 ] || die "--file requires a path"; SQL_FILE="$2"; shift 2 ;;
    -h|--help)   usage; exit 0 ;;
    *)           die "Unknown option: $1" ;;
  esac
done

[ -n "${SQL_STATEMENT}" ] || [ -n "${SQL_FILE}" ] || die "Pass --statement or --file."

load_env_file

if [ "${DRY_RUN}" != "true" ]; then
  require_snow
  require_snow_auth
fi

if [ -n "${SQL_STATEMENT}" ]; then
  snow_exec "${SQL_STATEMENT}" "Executing statement"
elif [ -n "${SQL_FILE}" ]; then
  snow_exec_file "${SQL_FILE}" "Executing file"
fi
