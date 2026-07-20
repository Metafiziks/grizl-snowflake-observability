#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "${SCRIPT_DIR}/lib.sh"

usage() {
  cat <<'USAGE'
Usage: bash snowflake/scripts/cortex-agent-mgmt.sh [options] <command>

Commands:
  test-analyst <question>   Send a natural language question to Cortex Analyst
  test-search  <query>      Run a semantic search query via Cortex Search
  test-agent   <question>   Send a question to the Cortex Agent (Analyst + Search combined)
  show-search-svc           Show Cortex Search Service status and indexing lag
  ask-incident <signal>     Template evidence query for an anomaly signal (JSON)

The Cortex Agent REST API is called from the external orchestrator, not from within
Snowflake. These test commands call the REST API directly via curl for local validation.
They require SNOWFLAKE_JWT or a valid OAuth token in the SNOWFLAKE_TOKEN env var.
See: snowflake/manifests/cortex-agent-config.example.json for the API payload format.

USAGE
  usage_common
}

COMMAND=""
ARG=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --config)  [ "$#" -ge 2 ] || die "--config requires a path"; CONFIG_FILE="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --yes)     YES=true; shift ;;
    test-analyst|test-search|test-agent|show-search-svc|ask-incident)
      COMMAND="$1"; shift
      if [ "$#" -gt 0 ] && [[ "$1" != --* ]]; then
        ARG="$1"; shift
      fi
      ;;
    -h|--help) usage; exit 0 ;;
    *)         die "Unknown option/command: $1" ;;
  esac
done

[ -n "${COMMAND}" ] || { usage; exit 1; }

load_env_file

ACCOUNT="${SNOWFLAKE_ACCOUNT:-your-account.snowflakecomputing.com}"
DATABASE="${SNOWFLAKE_DATABASE:-grizl}"
SCHEMA="${SNOWFLAKE_SCHEMA:-observability}"
TOKEN="${SNOWFLAKE_TOKEN:-}"

require_token() {
  [ -n "${TOKEN}" ] || die "Set SNOWFLAKE_TOKEN to a valid Snowflake JWT or OAuth token."
}

case "${COMMAND}" in
  show-search-svc)
    require_snow
    snow_exec "
      SHOW CORTEX SEARCH SERVICES IN SCHEMA ${DATABASE}.KNOWLEDGE;
    " "Cortex Search Service status"
    ;;

  test-analyst)
    [ -n "${ARG}" ] || die "Pass a natural language question as the argument."
    require_token
    SEMANTIC_MODEL_FILE="${SNOWFLAKE_DIR}/manifests/cortex-analyst-semantic-model.example.yaml"
    [ -f "${SEMANTIC_MODEL_FILE}" ] || die "Missing semantic model: ${SEMANTIC_MODEL_FILE}"
    QUESTION="${ARG}"
    info "Sending Cortex Analyst question: ${QUESTION}"
    if [ "${DRY_RUN}" = "true" ]; then
      info "[DRY-RUN] Would POST to https://${ACCOUNT}/api/v2/cortex/analyst/message"
      exit 0
    fi
    curl -sS -X POST "https://${ACCOUNT}/api/v2/cortex/analyst/message" \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Content-Type: application/json" \
      -d "$(node -e "
        const model = require('fs').readFileSync('${SEMANTIC_MODEL_FILE}', 'utf8');
        console.log(JSON.stringify({
          messages: [{ role: 'user', content: [{ type: 'text', text: process.argv[1] }] }],
          semantic_model: model,
        }));
      " "${QUESTION}")" | node -e "
      let buf = '';
      process.stdin.on('data', d => buf += d);
      process.stdin.on('end', () => {
        const r = JSON.parse(buf);
        if (r.message && r.message.content) {
          r.message.content.forEach(c => {
            if (c.type === 'text') console.log(c.text);
            if (c.type === 'sql') console.log('SQL:', c.statement);
          });
        } else {
          console.log(buf);
        }
      });
    "
    ;;

  test-search)
    [ -n "${ARG}" ] || die "Pass a search query as the argument."
    require_token
    info "Querying Cortex Search: ${ARG}"
    if [ "${DRY_RUN}" = "true" ]; then
      info "[DRY-RUN] Would POST to https://${ACCOUNT}/api/v2/cortex/search"
      exit 0
    fi
    curl -sS -X POST "https://${ACCOUNT}/api/v2/cortex/search" \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Content-Type: application/json" \
      -d "$(node -e "
        console.log(JSON.stringify({
          query: process.argv[1],
          database: '${DATABASE}',
          schema: 'knowledge',
          service: 'ARTICLE_SEARCH_SVC',
          limit: 5,
          columns: ['TITLE', 'CATEGORY', 'SERVICE', 'BODY'],
        }));
      " "${ARG}")" | node -e "
      let buf = '';
      process.stdin.on('data', d => buf += d);
      process.stdin.on('end', () => {
        const r = JSON.parse(buf);
        (r.results || []).forEach((res, i) => {
          console.log('---');
          console.log(\`[\${i+1}] \${res.TITLE} (\${res.CATEGORY})\`);
          console.log(res.BODY ? res.BODY.slice(0, 300) + '...' : '(no body)');
        });
      });
    "
    ;;

  test-agent)
    [ -n "${ARG}" ] || die "Pass a question as the argument."
    require_token
    info "Querying Cortex Agent: ${ARG}"
    if [ "${DRY_RUN}" = "true" ]; then
      info "[DRY-RUN] Would POST to https://${ACCOUNT}/api/v2/cortex/agent:run"
      exit 0
    fi
    AGENT_CONFIG="${SNOWFLAKE_DIR}/manifests/cortex-agent-config.example.json"
    curl -sS -X POST "https://${ACCOUNT}/api/v2/cortex/agent:run" \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Content-Type: application/json" \
      -d "$(node -e "
        const cfg = JSON.parse(require('fs').readFileSync('${AGENT_CONFIG}', 'utf8'));
        cfg.messages = [{ role: 'user', content: [{ type: 'text', text: process.argv[1] }] }];
        console.log(JSON.stringify(cfg));
      " "${ARG}")" | node -e "
      let buf = '';
      process.stdin.on('data', d => buf += d);
      process.stdin.on('end', () => {
        try {
          const r = JSON.parse(buf);
          if (r.choices && r.choices[0]) console.log(r.choices[0].message.content);
          else console.log(buf);
        } catch { console.log(buf); }
      });
    "
    ;;

  ask-incident)
    [ -n "${ARG}" ] || die "Pass a JSON signal object as the argument (or a signal_type string)."
    info "Evidence question for signal: ${ARG}"
    cat <<EOF
Cortex Agent evidence questions for incident response:

1. Cortex Analyst (structured SQL):
   "What are the most recent errors for service ${ARG} in the last 30 minutes?"
   "Which routes have the highest error rate in the last 15 minutes?"
   "What is the error rate trend for the current deployment SHA?"

2. Cortex Search (postmortem retrieval):
   "${ARG} failure root cause"
   "${ARG} runbook remediation steps"
   "similar incidents ${ARG}"

Use 'bash snowflake/scripts/cortex-agent-mgmt.sh test-analyst <question>' to try these.
See snowflake/manifests/cortex-agent-config.example.json for the full agent tool definition.
EOF
    ;;
esac
