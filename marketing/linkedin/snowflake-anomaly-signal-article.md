# Part VII: I Ported the Panic Button to Snowflake and Now Incidents Arrive with Postmortem Citations

Subtitle: Dynamic Tables, Cortex ML ANOMALY_DETECTION, Cortex Agents with two tools, and the discovery that the evidence layer is the part that actually makes alerts worth receiving.

---

The Databricks version worked.

Which means, obviously, I had to port it to Snowflake.

I know what you're thinking. You're thinking: how many platforms does one observability pipeline need to work on before its author is satisfied?

The answer, apparently, is at least three. Possibly more. I cannot rule anything out.

The public sanitized package is here:

https://github.com/Metafiziks/grizl-snowflake-observability

## Where we are in this series

**Part V**: Fabric. Eventhouse, KQL, `series_decompose_anomalies()`, Activator, Fabric Data Agent.

**Part VI**: Databricks. Delta Lake, Auto Loader, SQL z-scores, Workflow notebook, Genie.

**Part VII** (this one): Snowflake. Snowpipe, Dynamic Tables, Cortex ML ANOMALY_DETECTION, Cortex Agents (Analyst + Search), Snowflake Alerts.

Same architecture. Different nouns. Increasingly unhinged commitment to the bit.

## The ingestion path: still zero changes to the application

The Pub/Sub → GCS export subscription that the Databricks version set up is still running. The Snowflake version just reads from the same GCS bucket:

```sql
CREATE STORAGE INTEGRATION GRIZL_GCS_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://your-gcs-logs-bucket/logs/');

CREATE PIPE GRIZL.OBSERVABILITY.RAW_LOGS_PIPE
  AUTO_INGEST = TRUE
AS COPY INTO GRIZL.OBSERVABILITY.RAW_LOGS ...
FROM @GRIZL.OBSERVABILITY.GCS_LOGS_STAGE;
```

Snowpipe AUTO_INGEST watches for GCS object finalize notifications, picks up new JSONL files within a minute or two, runs the COPY INTO, and the rows appear in `RAW_LOGS`. The Databricks Auto Loader pipeline keeps running in parallel, reading the same files. The application and the log forwarder have not changed. Not one line of production code touched.

It is one GCS bucket, two platforms, zero coordination drama.

## The baseline problem: views vs Dynamic Tables

In the Databricks version, the anomaly signal views did a fresh full scan of `raw_logs` on every query. A 2-day rolling window, re-aggregated into 5-minute bins, every time the Workflow ran. It worked because the SQL was fast enough and the Workflow only ran every 5 minutes.

Snowflake has a better option: **Dynamic Tables**.

```sql
CREATE OR REPLACE DYNAMIC TABLE GRIZL.OBSERVABILITY.DT_HTTP_ERROR_RATE
  TARGET_LAG = '5 minutes'
  WAREHOUSE = GRIZL_WH
AS
SELECT
  TO_TIMESTAMP_LTZ(FLOOR(DATE_PART('epoch_second', INGEST_TIMESTAMP) / 300) * 300) AS TIME_BIN,
  CONCAT(SERVICE, '::', ROUTE)                                                       AS SERIES_KEY,
  SERVICE, ROUTE,
  COUNT(*)                                                                            AS REQUESTS,
  DIV0(
    SUM(IFF(STATUS_CODE >= 500 OR SEVERITY IN ('ERROR', 'CRITICAL'), 1.0, 0.0)),
    COUNT(*)
  )                                                                                   AS ERROR_RATE
FROM GRIZL.OBSERVABILITY.HTTP_REQUESTS
WHERE INGEST_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '2 days'
GROUP BY 1, 2, 3, 4;
```

`TARGET_LAG = '5 minutes'` means the Dynamic Table refreshes automatically so results are at most 5 minutes stale. The anomaly signal views read from the cached materialized result instead of re-scanning `RAW_LOGS` every time.

The Alert evaluates the anomaly signal view. The anomaly signal view reads the Dynamic Table. The Dynamic Table has already done the 2-day aggregation in the background. The Alert gets a fast answer.

That is the pattern. Views on top of materialized baselines, rather than fresh scans on every query.

## The anomaly detection upgrade: SQL z-scores → Cortex ML

The Databricks version used SQL z-score arithmetic:
- `AVG` and `STDDEV` over the baseline window
- `DIV0(actual - mean, stddev)` for the anomaly score
- Threshold at 1.5

This works and the Snowflake version still includes it as the default path.

But Snowflake has something the Databricks version did not: **Cortex ML ANOMALY_DETECTION**.

```sql
-- One-time: train the model on the Dynamic Table (2 days of history)
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION GRIZL.OBSERVABILITY.HTTP_ERROR_RATE_ANOMALY_MODEL(
  INPUT_DATA      => SYSTEM$REFERENCE('DYNAMIC TABLE', 'GRIZL.OBSERVABILITY.DT_HTTP_ERROR_RATE'),
  SERIES_COLNAME  => 'SERIES_KEY',
  TIMESTAMP_COLNAME => 'TIME_BIN',
  TARGET_COLNAME  => 'ERROR_RATE',
  LABEL_COLNAME   => NULL  -- unsupervised
);

-- Scheduled Task: detect anomalies every 5 minutes
CALL GRIZL.OBSERVABILITY.HTTP_ERROR_RATE_ANOMALY_MODEL!DETECT_ANOMALIES(
  INPUT_DATA      => SYSTEM$REFERENCE('VIEW', 'GRIZL.OBSERVABILITY.TMP_ERROR_RATE_RECENT'),
  SERIES_COLNAME  => 'SERIES_KEY',
  TIMESTAMP_COLNAME => 'TIME_BIN',
  TARGET_COLNAME  => 'ERROR_RATE'
);
```

The output has columns I did not have to compute manually: `IS_ANOMALY`, `DISTANCE` (normalized deviation, equivalent to z-score), `FORECAST` (what the model expected), `LOWER_BOUND`, and `UPPER_BOUND` (confidence interval).

Instead of asking "is `(actual - mean) / stddev >= 1.5`?", the model asks "is this outside the confidence interval that a trained time-series model would predict?"

That is conceptually closer to `series_decompose_anomalies()` in KQL — the Fabric version's approach — than to the manual z-score arithmetic in the Databricks version. The Snowflake version can be the SQL version or the ML version depending on how much training data you have and whether you're on Enterprise tier.

The SQL z-score path works out of the box. The Cortex ML path is the upgrade once training data accumulates.

## The evidence layer: two tools instead of one

This is the part that changed the most.

The Fabric version used Fabric Data Agent — natural language to KQL over the Eventhouse database.

The Databricks version used Genie — natural language to SQL over Delta tables.

Both are single-tool evidence agents. They answer "what happened?" in SQL terms.

The Snowflake version uses **Cortex Agents**, which orchestrates two tools in a single API call:

**Tool 1: Cortex Analyst** — natural language to SQL over a Cortex Analyst semantic model. The semantic model is a YAML file that maps table names, column descriptions, and example verified queries. Analyst translates "What are the most recent errors for grizl-backend on /api/chat?" into SQL, runs it against `GRIZL.OBSERVABILITY.APPLICATION_ERRORS`, and returns a natural-language answer plus the generated SQL.

**Tool 2: Cortex Search** — semantic search over `GRIZL.KNOWLEDGE.ARTICLES`, a table of postmortems, runbooks, and architecture notes indexed by Cortex Search Service. "grizl-backend /api/chat timeout regression" retrieves the most semantically relevant articles.

```json
{
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "cortex_analyst_grizl"
      }
    },
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "cortex_search_grizl_knowledge"
      }
    }
  ]
}
```

The orchestrator sends one API call. The agent decides which tool to use for which part of the question. The GitHub issue body gets:

- **Cortex Analyst evidence**: "The most common error in the last 30 minutes is RuntimeError:/api/chat with 18 occurrences. All errors are associated with deployment SHA `e2e_spike_sha`."
- **Cortex Search context**: "**Related postmortem**: PM-2026-04-12 grizl-backend /api/chat timeout regression. Root cause: OpenAI client connection pool exhaustion under high concurrency. Remediation: increase MAX_CONNECTIONS."

That second piece — the postmortem citation — is what changes the quality of the incident experience. Not "here is a SQL row dump." More like: "here is the evidence, and here is the last time this happened and what fixed it."

Genie doesn't have this. Fabric Data Agent doesn't have this. The Snowflake version does.

## The trigger: Snowflake Alert + stored procedure + webhook

The Databricks Workflow called GitHub directly — self-contained, no webhook required.

The Snowflake version returns to the pattern used in the Fabric version: the alert fires a webhook to an external orchestrator, which handles the Cortex Agent call and the GitHub issue.

```sql
CREATE OR REPLACE ALERT GRIZL.OBSERVABILITY.GRIZL_ANOMALY_ALERT
  WAREHOUSE = GRIZL_WH
  SCHEDULE  = '5 MINUTES'
  IF (EXISTS (
    SELECT 1
    FROM GRIZL.OBSERVABILITY.GRIZL_RECENT_ANOMALY_SIGNALS
    WHERE ANOMALY_SCORE >= 1.5
    LIMIT 1
  ))
  THEN CALL GRIZL.OBSERVABILITY.SP_NOTIFY_ANOMALY_INCIDENT();
```

`SP_NOTIFY_ANOMALY_INCIDENT` is a Python stored procedure. It:
1. Runs the anomaly signal union view
2. Builds a payload with the top 10 signals
3. POSTs to the orchestrator webhook URL (read from a Snowflake secret via `_snowflake.get_generic_secret_string()`)

The orchestrator webhook receives the signals, calls Cortex Agents for evidence, creates the GitHub issue, and assigns Copilot if policy allows.

The Copilot assignment is the same GraphQL mutation as the other versions: `addAssigneesToAssignable` with bot node ID `BOT_kgDOC9w8XQ`.

The webhook URL is stored in `GRIZL.OBSERVABILITY.ORCHESTRATOR_WEBHOOK_SECRET` — never in environment variables or config files.

## What the platform differences actually mean

After building this three times:

**Fabric**: closest to the data primitives. `series_decompose_anomalies()` is a native time-series function. Activator is a managed alert runtime. Data Agent requires no SQL semantics file.

**Databricks**: most self-contained. The Workflow notebook handles everything end-to-end. Genie answers SQL questions without a semantic model file. The z-score arithmetic is SQL any engineer can read at 2 AM.

**Snowflake**: best evidence layer. Cortex Search over postmortems changes the quality of what arrives in the GitHub issue. Dynamic Tables are the right primitive for the baseline problem. Cortex ML ANOMALY_DETECTION is the cleanest ML-in-SQL experience of the three.

None of them is strictly better. They are the same philosophy — anomaly detection close to the data, incident payloads with receipts, GitHub issues with forensic evidence, Copilot only when the signal is code-actionable — implemented with whatever the platform makes easy.

## What I published

https://github.com/Metafiziks/grizl-snowflake-observability

It includes:

- Snowpipe AUTO_INGEST setup from GCS (reuses the Databricks Pub/Sub export subscription)
- SQL logical views (HTTP_REQUESTS, APPLICATION_ERRORS, DEPLOYMENTS, FORWARDER_HEALTH)
- Dynamic Tables for materialized 5-minute baseline aggregations
- SQL z-score anomaly signal views (default path, works immediately)
- Cortex ML ANOMALY_DETECTION models and detection Tasks (optional Enterprise+ upgrade path)
- Cortex Search Service over GRIZL.KNOWLEDGE.ARTICLES
- Cortex Agent configuration: semantic model + Analyst + Search tool definition
- Snowflake Alert + SP_NOTIFY_ANOMALY_INCIDENT stored procedure
- Snowsight dashboard tile queries
- Provisioning scripts, dry-run mode, config templates
- docs/snowflake-incident-orchestrator.md: Alert payload contract, Cortex Agent API call format, GitHub issue format, Copilot assignment policy
- No account identifiers, no keys, no webhook URLs

## What comes next

Three platforms. Same architecture. The system now watches production from Fabric, Databricks, and Snowflake simultaneously. 

I am not yet sure if this is a sophisticated observability strategy or a commitment problem that has escaped into infrastructure.

Possibly both.

Either way: the incident arrives with fingerprints and postmortem citations. That is the version of an alert worth building.

Good.
