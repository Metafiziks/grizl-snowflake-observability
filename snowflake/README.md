# Snowflake Package — Scripts and Configuration

Provisioning helpers, config templates, and manifests for the GRIZL Snowflake observability package.

## Directory layout

| Path | Purpose |
|---|---|
| `config/grizl.snowflake.env.example` | Config template — copy to `grizl.snowflake.env` and fill in |
| `manifests/manual-resources.json` | Manual-step checklist (GCS setup, storage integration, alerts resume) |
| `manifests/cortex-agent-config.example.json` | Cortex Agent REST API payload template |
| `manifests/cortex-analyst-semantic-model.example.yaml` | Cortex Analyst semantic model (upload to Snowflake stage) |
| `manifests/snowpipe-config.example.json` | Snowpipe AUTO_INGEST setup reference and GCS notification steps |
| `scripts/lib.sh` | Shared shell helpers (`snow_exec`, `load_env_file`, `require_snow`) |
| `scripts/provision.sh` | Provision database, schema, warehouse, role, and apply SQL |
| `scripts/teardown.sh` | Drop observability resources (destructive, requires `--yes`) |
| `scripts/sql-exec.sh` | Execute a SQL statement or file via Snowflake CLI |
| `scripts/snowpipe-mgmt.sh` | Pipe status, pause/resume, manual refresh, error query |
| `scripts/cortex-agent-mgmt.sh` | Test Cortex Analyst, Cortex Search, and Cortex Agent via CLI |
| `scripts/check.sh` | Local checks: shell syntax, JSON manifests, SQL file presence, no secrets |

## Prerequisites

- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) (`snow`) — install with `pip install snowflake-cli-labs`
- Node.js (for JSON manifest parsing in `check.sh`)
- A Snowflake account on Enterprise or higher for Dynamic Tables and Cortex ML
- Snowflake Standard edition supports SQL z-score views and Alerts, but not Dynamic Tables or Cortex ML ANOMALY_DETECTION

## Quick workflow

```bash
# 1. Configure
cp config/grizl.snowflake.env.example config/grizl.snowflake.env
# Fill in SNOWFLAKE_CONNECTION, GCS_LOGS_BUCKET, etc.

# 2. Add Snowflake CLI connection
snow connection add grizl --account <ACCOUNT> --user <USER> --authenticator externalbrowser

# 3. Local checks
npm run check

# 4. Dry-run
npm run provision:dry-run

# 5. Provision
npm run provision

# 6. Snowpipe setup (after granting GCS permissions)
bash scripts/snowpipe-mgmt.sh show-svc   # get service account email
bash scripts/snowpipe-mgmt.sh resume      # after granting access

# 7. Verify
snow sql --connection grizl -q "SELECT COUNT(*) FROM GRIZL.OBSERVABILITY.RAW_LOGS;"

# 8. Test anomaly signals
snow sql --connection grizl -q "SELECT * FROM GRIZL.OBSERVABILITY.GRIZL_RECENT_ANOMALY_SIGNALS LIMIT 5;"

# 9. Resume alert (after setting webhook secret)
snow sql --connection grizl -q "ALTER ALERT GRIZL.OBSERVABILITY.GRIZL_ANOMALY_ALERT RESUME;"
```

## Dry-run mode

All scripts support `--dry-run` — prints SQL statements without executing them. Safe to run without Snowflake credentials or connection.

```bash
bash scripts/provision.sh --dry-run --all
bash scripts/teardown.sh --dry-run --all
bash scripts/sql-exec.sh --dry-run --file ../sql/grizl-anomaly-signals.sql
```
