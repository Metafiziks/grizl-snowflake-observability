#!/usr/bin/env bash

if [ -n "${GRIZL_SNOWFLAKE_LIB_SOURCED:-}" ]; then
  return 0
fi
GRIZL_SNOWFLAKE_LIB_SOURCED=1

SNOWFLAKE_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SNOWFLAKE_DIR="$(cd "${SNOWFLAKE_SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${SNOWFLAKE_DIR}/.." && pwd)"

CONFIG_FILE="${CONFIG_FILE:-${SNOWFLAKE_DIR}/config/grizl.snowflake.env}"
DRY_RUN="${DRY_RUN:-false}"
YES="${YES:-false}"

info() { printf '[INFO]  %s\n' "$*"; }
ok()   { printf '[OK]    %s\n' "$*"; }
warn() { printf '[WARN]  %s\n' "$*" >&2; }
die()  { printf '[ERROR] %s\n' "$*" >&2; exit 1; }

usage_common() {
  cat <<'USAGE'
Common options:
  --config <path>  Load env vars from a config file (default: snowflake/config/grizl.snowflake.env)
  --dry-run        Print SQL/API calls without executing them
  --yes            Required for live create/delete operations
USAGE
}

load_env_file() {
  if [ -f "${CONFIG_FILE}" ]; then
    info "Loading config ${CONFIG_FILE}"
    set -a
    # shellcheck source=/dev/null
    . "${CONFIG_FILE}"
    set +a
  else
    warn "Config file not found: ${CONFIG_FILE}"
    warn "Copy snowflake/config/grizl.snowflake.env.example to snowflake/config/grizl.snowflake.env"
  fi
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

require_snow() {
  require_cmd snow
}

require_snow_auth() {
  if [ "${DRY_RUN}" = "true" ]; then
    return 0
  fi
  snow connection test --connection "${SNOWFLAKE_CONNECTION:-grizl}" >/dev/null 2>&1 \
    || die "Snowflake connection test failed. Run: snow connection add grizl"
}

ensure_mutation_allowed() {
  if [ "${DRY_RUN}" != "true" ] && [ "${YES}" != "true" ]; then
    die "Pass --yes to confirm live create/delete operations, or --dry-run to preview."
  fi
}

snow_exec() {
  local sql="$1"
  local label="${2:-SQL}"
  info "${label}"
  if [ "${DRY_RUN}" = "true" ]; then
    printf '[DRY-RUN] Would execute:\n%s\n\n' "${sql}"
    return 0
  fi
  snow sql \
    --connection "${SNOWFLAKE_CONNECTION:-grizl}" \
    --query "${sql}"
}

snow_exec_file() {
  local file="$1"
  local label="${2:-SQL file}"
  info "${label}: ${file}"
  [ -f "${file}" ] || die "SQL file not found: ${file}"
  if [ "${DRY_RUN}" = "true" ]; then
    printf '[DRY-RUN] Would execute file: %s\n\n' "${file}"
    return 0
  fi
  # Substitute <PLACEHOLDER> tokens from environment before sending to Snowflake.
  local sql
  sql="$(sed \
    -e "s|<GCS_LOGS_BUCKET>|${GCS_LOGS_BUCKET:-<GCS_LOGS_BUCKET>}|g" \
    "${file}")"
  snow sql \
    --connection "${SNOWFLAKE_CONNECTION:-grizl}" \
    --query "${sql}"
}
