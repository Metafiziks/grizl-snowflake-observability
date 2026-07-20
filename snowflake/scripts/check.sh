#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "${SCRIPT_DIR}/lib.sh"

info "Checking shell syntax"
for script in "${SNOWFLAKE_DIR}"/scripts/*.sh; do
  bash -n "${script}"
done
ok "Shell syntax passed"

info "Checking JSON manifests"
for json_file in \
  "${SNOWFLAKE_DIR}/package.json" \
  "${SNOWFLAKE_DIR}/manifests/manual-resources.json" \
  "${SNOWFLAKE_DIR}/manifests/cortex-agent-config.example.json" \
  "${SNOWFLAKE_DIR}/manifests/snowpipe-config.example.json"; do
  node -e "const fs = require('fs'); JSON.parse(fs.readFileSync(process.argv[1], 'utf8'));" "${json_file}"
done
ok "JSON manifests parsed"

info "Checking referenced SQL source files"
for sql_file in \
  "${REPO_ROOT}/sql/grizl-observability.sql" \
  "${REPO_ROOT}/sql/grizl-dynamic-tables.sql" \
  "${REPO_ROOT}/sql/grizl-anomaly-signals.sql" \
  "${REPO_ROOT}/sql/grizl-cortex-ml.sql" \
  "${REPO_ROOT}/sql/grizl-alert-queries.sql" \
  "${REPO_ROOT}/sql/grizl-dashboard-tiles.sql"; do
  [ -f "${sql_file}" ] || die "Missing SQL source file: ${sql_file}"
done
ok "SQL source files exist"

info "Checking for committed credential material"
if find "${SNOWFLAKE_DIR}" -type f \
    ! -path "${SNOWFLAKE_DIR}/scripts/check.sh" \
    -print0 \
  | xargs -0 grep -E \
    'SNOWFLAKE_PASSWORD=.+[a-zA-Z0-9]|private_key_passphrase=.+[a-zA-Z0-9]|account=.*\.snowflakecomputing\.com[^>]' \
    >/dev/null 2>&1; then
  die "Potential Snowflake password or private key passphrase found under snowflake/"
fi
ok "No credential material detected"

info "Checking example files contain only placeholder values"
grep -E 'your-account|YOUR_ACCOUNT|<ACCOUNT>|your-orchestrator' \
  "${SNOWFLAKE_DIR}/config/grizl.snowflake.env.example" \
  >/dev/null 2>&1 || die "Example config missing placeholder values — check grizl.snowflake.env.example"
ok "Example config contains placeholder values"

ok "Snowflake package local checks passed"
