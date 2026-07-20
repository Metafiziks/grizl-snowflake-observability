# GRIZL Snowflake Incident Orchestrator — Reference Architecture

This document describes the interface contract between the GRIZL Snowflake observability package and the external incident orchestrator that creates GitHub issues and assigns GitHub Copilot.

The external orchestrator is not part of this repo. This document specifies the webhook payload from Snowflake Alerts, the Cortex Agent API calls for evidence, the GitHub issue format, and the Copilot assignment policy.

## Architecture overview

```
Snowflake Alert (5 min)
  └─ GRIZL_ANOMALY_ALERT: IF EXISTS GRIZL_RECENT_ANOMALY_SIGNALS
  └─ THEN: SP_NOTIFY_ANOMALY_INCIDENT()
       └─ POST /api/snowflake/incidents (external orchestrator)
            ├─ Cortex Agent REST API (Analyst evidence + Search context)
            └─ GitHub issue + optional Copilot assignment
```

## Snowflake Alert webhook payload

`SP_NOTIFY_ANOMALY_INCIDENT()` (stored procedure in `sql/grizl-alert-queries.sql`) POSTs this payload to the orchestrator webhook URL:

```json
{
  "source": "snowflake_alert",
  "fired_at": "2026-07-20T14:32:05Z",
  "signals": [
    {
      "SIGNAL_TYPE": "backend_http_error_rate",
      "SERVICE": "grizl-backend",
      "ROUTE": "/api/chat",
      "REQUESTS": 47,
      "ERRORS": 18,
      "ACTUAL": 0.383,
      "BASELINE_MEAN": 0.021,
      "BASELINE_STDDEV": 0.008,
      "ANOMALY_SCORE": 45.25,
      "TIME_BIN": "2026-07-20T14:30:00Z"
    }
  ],
  "top_signal": { ... }
}
```

## Cortex Agent evidence query

The orchestrator calls the Cortex Agent REST API after receiving the webhook:

```
POST https://<account>.snowflakecomputing.com/api/v2/cortex/agent:run
Authorization: Bearer <snowflake_jwt>
Content-Type: application/json

{
  "model": "claude-snowflake-cortex-4",
  "tools": [ ... ],     // see snowflake/manifests/cortex-agent-config.example.json
  "messages": [
    {
      "role": "user",
      "content": [{
        "type": "text",
        "text": "What are the most recent errors for grizl-backend on route /api/chat in the last 30 minutes? Include error signature, affected deployment SHA, and error count."
      }]
    }
  ]
}
```

The agent orchestrates:
- **Cortex Analyst**: runs SQL against `GRIZL.OBSERVABILITY.APPLICATION_ERRORS` and related views using the semantic model (`cortex-analyst-semantic-model.example.yaml`)
- **Cortex Search**: retrieves relevant postmortems and runbooks from `GRIZL.KNOWLEDGE.ARTICLE_SEARCH_SVC` for the affected service/route

Response is a streaming SSE. Accumulate `content[].type == "text"` delta blocks into the evidence summary string.

### Authentication for the Cortex Agent call

The orchestrator authenticates to Snowflake using key-pair JWT auth:
1. Generate RSA key pair for the service account
2. Assign public key to the Snowflake service user
3. Generate JWT per request: `snow generate-jwt --connection grizl` or use the Snowflake Python connector `generate_jwt()`
4. Pass as `Authorization: Bearer <jwt>` with `X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT`

### Direct SQL fallback

If the Cortex Agent API is unavailable, fall back to direct SQL via the Snowflake SQL REST API:

```
POST https://<account>.snowflakecomputing.com/api/v2/statements
Authorization: Bearer <snowflake_jwt>

{
  "statement": "SELECT SERVICE, ERROR_SIGNATURE, COUNT(*) AS ERROR_COUNT, MAX(INGEST_TIMESTAMP) AS LAST_SEEN FROM GRIZL.OBSERVABILITY.APPLICATION_ERRORS WHERE SERVICE = 'grizl-backend' AND ROUTE = '/api/chat' AND INGEST_TIMESTAMP >= DATEADD('minute', -30, CURRENT_TIMESTAMP()) GROUP BY 1, 2 ORDER BY ERROR_COUNT DESC LIMIT 10",
  "timeout": 60,
  "warehouse": "grizl_wh",
  "database": "grizl",
  "schema": "observability"
}
```

## GitHub issue format

```markdown
## [Snowflake Anomaly] Backend HTTP Error Rate — /api/chat (z=45.25)

**Signal**: backend_http_error_rate
**Service**: grizl-backend  |  **Route**: /api/chat
**Anomaly score**: 45.25 (threshold 1.5)
**Actual error rate**: 38.3%  |  **Baseline mean**: 2.1%  |  **Baseline σ**: 0.8%
**Detection window**: last 15 min  |  **Baseline window**: last 2 days
**Fired at**: 2026-07-20T14:32:05Z

---

### Cortex Analyst evidence

> The most common error in the last 30 minutes is `RuntimeError:/api/chat` with 18 occurrences.
> All errors are associated with deployment SHA `e2e_spike_sha` (deployed at 14:18 UTC).
> No errors were recorded for the previous deployment SHA `abc123def`.
> The affected route is `/api/chat` on `grizl-backend` in the `production` environment.

### Cortex Search context

> **Related postmortem**: "PM-2026-04-12 grizl-backend /api/chat timeout regression"
> Root cause: OpenAI client connection pool exhaustion under high concurrency.
> Remediation: increase MAX_CONNECTIONS in the LangGraph runtime config.

---

**Labels**: observability, snowflake-anomaly, critical
**Deployment SHA**: e2e_spike_sha
```

## Copilot assignment policy

Assign GitHub Copilot Coding Agent (`BOT_kgDOC9w8XQ`) when all of the following are true:

| Condition | Rationale |
|---|---|
| `anomaly_score >= 3.0` | High-confidence signal, not noise |
| `signal_type` is `backend_http_error_rate` or `error_signature_spike` or `post_deployment_regression` | Code-actionable signal types |
| `deployment_sha` is not null | Known starting point for investigation |
| No open issue for the same `service` + `route` + `deployment_sha` | Deduplicate |
| `copilot_enabled = true` in orchestrator config | Operator gate |

Use the GraphQL `addAssigneesToAssignable` mutation:

```graphql
mutation AssignCopilot($issueId: ID!, $botId: ID!) {
  addAssigneesToAssignable(input: { assignableId: $issueId, assigneeIds: [$botId] }) {
    assignable {
      ... on Issue { number title }
    }
  }
}
```

Copilot bot node ID: `BOT_kgDOC9w8XQ`

## Comparison: Fabric vs Databricks vs Snowflake orchestrator pattern

| Aspect | Fabric | Databricks | Snowflake |
|---|---|---|---|
| Alert trigger | Activator / Reflex | Workflow (5-min cron) | Snowflake Alert (5-min schedule) |
| Alert action | Webhook to external orchestrator | Workflow notebook calls GitHub directly | Stored procedure → webhook to external orchestrator |
| Evidence agent | Fabric Data Agent (MCP) | Genie (AI/BI) | Cortex Agents (Analyst + Search) |
| SQL fallback | Direct Kusto REST API | Spark SQL / SQL warehouse | Snowflake SQL REST API |
| GitHub issue creation | External orchestrator | Workflow notebook | External orchestrator |
| Copilot assignment | External orchestrator | Workflow notebook (GraphQL) | External orchestrator |
| Evidence types | KQL structured + (no search) | SQL structured only | SQL structured + semantic postmortem/runbook search |

The Cortex Search layer (postmortem and runbook retrieval) is unique to the Snowflake version among the three. Fabric Data Agent and Genie are SQL-only evidence agents.
