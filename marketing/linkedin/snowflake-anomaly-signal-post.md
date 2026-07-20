# Supporting LinkedIn Post

I ported the panic button to Snowflake.

Same architecture. Third platform. Increasingly concerning life choices.

The Fabric version used KQL and Activator. The Databricks version used Delta, SQL z-scores, and a Workflow notebook that called GitHub directly. The Snowflake version uses Dynamic Tables, Cortex ML ANOMALY_DETECTION, Cortex Agents, and a Snowflake Alert that fires a webhook to an external orchestrator.

The key upgrade: the evidence layer is now two tools instead of one.

Cortex Analyst answers SQL questions over the telemetry tables. Cortex Search retrieves semantically relevant postmortems and runbooks from a knowledge base. The Cortex Agent runs both in a single API call. The GitHub issue arrives with what happened AND what probably caused it AND what other incidents looked like this.

That second layer — the postmortem search — is the part that changes how fast you can close the issue. "This looks like the April timeout regression" is more useful than a SQL row dump at 3 AM.

Snowflake anomaly detection is also different from the SQL z-score path. SNOWFLAKE.ML.ANOMALY_DETECTION is a trained time-series model. You give it a Dynamic Table as training data. It gives you IS_ANOMALY, a confidence interval, and a DISTANCE score instead of doing the z-score arithmetic yourself. The SQL fallback still works. The ML path is the upgrade once training data accumulates.

The Snowpipe side is completely non-invasive. Same GCS bucket. Same Pub/Sub export subscription. Snowflake just reads the same JSONL files.

Public sanitized package:

https://github.com/Metafiziks/grizl-snowflake-observability

No account identifiers. No keys. No webhook URLs. Just SQL, Dynamic Tables, Cortex ML models, a Snowflake Alert, and incidents that arrive with receipts and postmortem citations.

Part VII below 👇

#Snowflake #CortexML #CortexAgents #Observability #AnomalyDetection #GCP #GitHub #Copilot #DevOps #AIOps #DataEngineering
