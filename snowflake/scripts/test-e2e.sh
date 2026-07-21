#!/usr/bin/env bash
# =============================================================================
# GRIZL Snowflake Observability — End-to-End Test
# =============================================================================
# Tests the full pipeline: RAW_LOGS → Dynamic Tables → Anomaly Signals → Alert.
#
# Usage:
#   bash snowflake/scripts/test-e2e.sh [--wait-alert]
#
# Options:
#   --wait-alert  After seeding data, poll until the GRIZL_ANOMALY_ALERT fires
#                 automatically (up to 10 min). Default: call the SP manually.
#   --yes         Skip confirmation prompt.
#   --config      Config file path (default: snowflake/config/grizl.snowflake.env)
#
# What this tests:
#   1. RAW_LOGS accepts inserts (baseline + anomaly spike)
#   2. Dynamic Tables DT_HTTP_ERROR_RATE and DT_ROUTE_LATENCY refresh correctly
#   3. GRIZL_RECENT_ANOMALY_SIGNALS returns rows with ANOMALY_SCORE >= 1.5
#   4. SP_NOTIFY_ANOMALY_INCIDENT writes to ALERT_LOG
#   5. [--wait-alert] GRIZL_ANOMALY_ALERT fires on schedule and logs to ALERT_LOG
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "${SCRIPT_DIR}/lib.sh"

WAIT_ALERT=false

while [ "$#" -gt 0 ]; do
  case "$1" in
    --config)      [ "$#" -ge 2 ] || die "--config requires a path"; CONFIG_FILE="$2"; shift 2 ;;
    --wait-alert)  WAIT_ALERT=true; shift ;;
    --yes)         YES=true; shift ;;
    -h|--help)     echo "Usage: bash $0 [--wait-alert] [--yes] [--config <path>]"; exit 0 ;;
    *)             die "Unknown option: $1" ;;
  esac
done

load_env_file
require_snow
require_snow_auth

PASS=0; FAIL=0
pass() { PASS=$((PASS+1)); printf '[PASS]  %s\n' "$*"; }
fail() { FAIL=$((FAIL+1)); printf '[FAIL]  %s\n' "$*" >&2; }
assert_nonempty() {
  local label="$1" result="$2"
  if echo "${result}" | grep -qE '[0-9]'; then
    pass "${label}"
  else
    fail "${label} — got: ${result}"
  fi
}

snow_q() {
  snow sql --connection "${SNOWFLAKE_CONNECTION:-grizl}" --query "$1" 2>&1
}

# ── STEP 0: clear previous test data and ensure alert is active ──────────────
info "Clearing previous test data from RAW_LOGS and ALERT_LOG"
snow_q "DELETE FROM GRIZL.OBSERVABILITY.RAW_LOGS
        WHERE SERVICE = 'grizl-backend' AND ROUTE = '/api/orders'
          AND DEPLOYMENT_SHA IN ('baseline-sha', 'spike-sha');" > /dev/null
snow_q "DELETE FROM GRIZL.OBSERVABILITY.ALERT_LOG;" > /dev/null
snow_q "ALTER ALERT GRIZL.OBSERVABILITY.GRIZL_ANOMALY_ALERT RESUME;" > /dev/null 2>&1 || true

# ── STEP 1: insert baseline traffic (2 days, ~2% error rate) ─────────────────
info "Inserting baseline traffic (10 000 rows over 2 days)"
snow_q "
INSERT INTO GRIZL.OBSERVABILITY.RAW_LOGS
  (INGEST_TIMESTAMP, SOURCE_TIMESTAMP, SERVICE, ENVIRONMENT, SEVERITY,
   EVENT_TYPE, METHOD, ROUTE, STATUS_CODE, DURATION_MS, DEPLOYMENT_SHA)
SELECT
  DATEADD('second', SEQ4() * 17, CURRENT_TIMESTAMP() - INTERVAL '2 days'),
  DATEADD('second', SEQ4() * 17, CURRENT_TIMESTAMP() - INTERVAL '2 days'),
  'grizl-backend', 'production',
  IFF(MOD(SEQ4(), 50) = 0, 'ERROR', 'INFO'),
  'http_request', 'GET', '/api/orders',
  IFF(MOD(SEQ4(), 50) = 0, 500, 200),
  100 + UNIFORM(0, 50, RANDOM()),
  'baseline-sha'
FROM TABLE(GENERATOR(ROWCOUNT => 10000));
" > /dev/null

result=$(snow_q "SELECT COUNT(*) FROM GRIZL.OBSERVABILITY.RAW_LOGS
                 WHERE DEPLOYMENT_SHA = 'baseline-sha';" | grep -E '^\| [0-9]' | tr -d '| ')
if [ "${result:-0}" -ge 10000 ]; then
  pass "Step 1 — baseline rows inserted (${result})"
else
  fail "Step 1 — expected >= 10000 baseline rows, got ${result}"
fi

# ── STEP 2: insert anomaly spike (last 10 min, ~60% error rate) ──────────────
info "Inserting anomaly spike (300 rows, last 10 minutes, 60% error rate)"
snow_q "
INSERT INTO GRIZL.OBSERVABILITY.RAW_LOGS
  (INGEST_TIMESTAMP, SOURCE_TIMESTAMP, SERVICE, ENVIRONMENT, SEVERITY,
   EVENT_TYPE, METHOD, ROUTE, STATUS_CODE, DURATION_MS, DEPLOYMENT_SHA)
SELECT
  DATEADD('second', SEQ4() * 2, CURRENT_TIMESTAMP() - INTERVAL '10 minutes'),
  DATEADD('second', SEQ4() * 2, CURRENT_TIMESTAMP() - INTERVAL '10 minutes'),
  'grizl-backend', 'production',
  IFF(MOD(SEQ4(), 10) < 6, 'ERROR', 'INFO'),
  'http_request', 'GET', '/api/orders',
  IFF(MOD(SEQ4(), 10) < 6, 500, 200),
  300 + UNIFORM(0, 200, RANDOM()),
  'spike-sha'
FROM TABLE(GENERATOR(ROWCOUNT => 300));
" > /dev/null

result=$(snow_q "SELECT COUNT(*) FROM GRIZL.OBSERVABILITY.RAW_LOGS
                 WHERE DEPLOYMENT_SHA = 'spike-sha';" | grep -E '^\| [0-9]' | tr -d '| ')
if [ "${result:-0}" -ge 300 ]; then
  pass "Step 2 — spike rows inserted (${result})"
else
  fail "Step 2 — expected >= 300 spike rows, got ${result}"
fi

# ── STEP 3: force Dynamic Table refresh ──────────────────────────────────────
info "Refreshing Dynamic Tables"
snow_q "ALTER DYNAMIC TABLE GRIZL.OBSERVABILITY.DT_HTTP_ERROR_RATE REFRESH;" > /dev/null
snow_q "ALTER DYNAMIC TABLE GRIZL.OBSERVABILITY.DT_ROUTE_LATENCY REFRESH;" > /dev/null

result=$(snow_q "SELECT COUNT(*) FROM GRIZL.OBSERVABILITY.DT_HTTP_ERROR_RATE;" | grep -E '^\| [0-9]' | tr -d '| ')
if [ "${result:-0}" -gt 0 ]; then
  pass "Step 3 — DT_HTTP_ERROR_RATE has ${result} rows after refresh"
else
  fail "Step 3 — DT_HTTP_ERROR_RATE empty after refresh"
fi

# ── STEP 4: verify anomaly signals ───────────────────────────────────────────
info "Querying GRIZL_RECENT_ANOMALY_SIGNALS"
result=$(snow_q "
  SELECT SIGNAL_TYPE, ROUND(ANOMALY_SCORE,1) AS SCORE
  FROM GRIZL.OBSERVABILITY.GRIZL_RECENT_ANOMALY_SIGNALS
  WHERE ANOMALY_SCORE >= 1.5
  ORDER BY ANOMALY_SCORE DESC LIMIT 3;
" | grep -E 'backend_http_error_rate|route_latency' | wc -l | tr -d ' ')

if [ "${result:-0}" -gt 0 ]; then
  pass "Step 4 — anomaly signals returned (${result} rows with score >= 1.5)"
else
  fail "Step 4 — no anomaly signals found"
fi

top_score=$(snow_q "
  SELECT MAX(ANOMALY_SCORE) FROM GRIZL.OBSERVABILITY.GRIZL_RECENT_ANOMALY_SIGNALS
  WHERE ANOMALY_SCORE >= 1.5;
" | grep -E '^\| [0-9]' | tr -d '| ' | head -1)
if [ -n "${top_score}" ] && awk "BEGIN{exit !( ${top_score:-0} >= 1.5 )}"; then
  pass "Step 4b — top anomaly score is ${top_score} (>= 1.5 threshold)"
else
  fail "Step 4b — top score ${top_score:-none} is below threshold"
fi

# ── STEP 5: invoke SP manually and verify ALERT_LOG ──────────────────────────
info "Calling SP_NOTIFY_ANOMALY_INCIDENT() directly"
sp_result=$(snow_q "CALL GRIZL.OBSERVABILITY.SP_NOTIFY_ANOMALY_INCIDENT();" | grep 'logged:' | head -1)
if echo "${sp_result}" | grep -q 'logged:'; then
  pass "Step 5 — stored procedure returned: ${sp_result}"
else
  fail "Step 5 — unexpected SP output: ${sp_result}"
fi

log_count=$(snow_q "SELECT COUNT(*) FROM GRIZL.OBSERVABILITY.ALERT_LOG;" | grep -E '^\| [0-9]' | tr -d '| ')
if [ "${log_count:-0}" -ge 1 ]; then
  pass "Step 5b — ALERT_LOG has ${log_count} row(s)"
else
  fail "Step 5b — ALERT_LOG is empty after SP call"
fi

# ── STEP 6: verify alert is active ───────────────────────────────────────────
info "Checking GRIZL_ANOMALY_ALERT state"
alert_state=$(snow_q "SHOW ALERTS LIKE 'GRIZL_ANOMALY_ALERT' IN SCHEMA GRIZL.OBSERVABILITY;" \
  | grep -i 'grizl_anomaly_alert' | grep -oE '\| [a-z]+ +\|' | head -3 | grep -v 'observability\|accountadmin\|grizl_wh\|grizl ' | head -1 | tr -d '| ' || true)
if [ -z "${alert_state}" ]; then
  # Fallback: look for 'started' anywhere in the alert row
  alert_state=$(snow_q "SHOW ALERTS LIKE 'GRIZL_ANOMALY_ALERT' IN SCHEMA GRIZL.OBSERVABILITY;" \
    | grep -i 'grizl_anomaly_alert' | grep -oi 'started\|suspended' | head -1 | tr '[:upper:]' '[:lower:]' || true)
fi
if [ "${alert_state}" = "started" ]; then
  pass "Step 6 — alert is STARTED (active, will fire on schedule)"
elif [ -n "${alert_state}" ]; then
  fail "Step 6 — alert state is '${alert_state}' (expected 'started')"
else
  warn "Step 6 — could not parse alert state; check Snowsight → Monitoring → Alerts"
fi

# ── STEP 7 (optional): wait for automatic alert fire ─────────────────────────
if [ "${WAIT_ALERT}" = "true" ]; then
  info "Waiting up to 10 minutes for GRIZL_ANOMALY_ALERT to fire automatically..."
  pre_count="${log_count:-0}"
  WAITED=0
  while [ "${WAITED}" -lt 600 ]; do
    sleep 30
    WAITED=$((WAITED + 30))
    new_count=$(snow_q "SELECT COUNT(*) FROM GRIZL.OBSERVABILITY.ALERT_LOG;" | grep -E '^\| [0-9]' | tr -d '| ')
    if [ "${new_count:-0}" -gt "${pre_count}" ]; then
      pass "Step 7 — alert fired automatically after ${WAITED}s (ALERT_LOG now has ${new_count} rows)"
      break
    fi
    info "  ${WAITED}s elapsed — ALERT_LOG still at ${new_count} rows, polling..."
  done
  if [ "${WAITED}" -ge 600 ]; then
    fail "Step 7 — alert did not fire within 10 minutes (check alert schedule in Snowsight)"
  fi
fi

# ── SUMMARY ──────────────────────────────────────────────────────────────────
printf '\n'
printf '[RESULT] %d passed, %d failed\n' "${PASS}" "${FAIL}"
if [ "${FAIL}" -gt 0 ]; then
  exit 1
fi
ok "End-to-end test passed."
