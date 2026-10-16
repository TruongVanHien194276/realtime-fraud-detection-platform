# Fraud platform: 5–7 minute interview demo

## Before recording or presenting

Use the synthetic/curated fixture and check that no terminal exposes `.env`, raw account IDs, access tokens, or personal notifications. On the 8 GiB Mac, do not run Spark together with Grafana image rendering or ML training. Open the architecture, performance report, Grafana dashboard, and `/model-info` before starting. The Power BI Cloud page may be shown only if a real cloud report has been published; local Streamlit is labeled local validation.

| Time | Show | What to say |
|---|---|---|
| 0:00–0:45 | `README.md`, problem statement | PaySim is simulated mobile-money data; the objective is an auditable near-real-time fraud platform, not a production fraud claim. |
| 0:45–1:35 | `docs/architecture-final.md` | Batch Bronze/Silver/Gold, streaming rules path, and separate hybrid API. State explicitly which arrows are not connected. |
| 1:35–2:30 | `reports/week9_data_quality.json` and dbt graph | Show 100k reconciled rows and 28/28 tests; explain quality gate before Gold publication. |
| 2:30–3:40 | Producer cursor/log, stream batch log, decisions/alerts | Normal transaction, replay duplicate, late/malformed DLQ, then a fraud alert. Explain transaction-ID idempotency and at-least-once Kafka publish. |
| 3:40–4:30 | Grafana dashboard screenshot or live panel | Show health and decisions. A missing lag series is not zero lag. If Power BI Cloud exists, show its real report and `data_as_of`; otherwise call it a pending BI publication step. |
| 4:30–5:20 | `/model-info`, `/ready`, timeout test | Explain pinned model version, hard-rule precedence, degraded rule-only fallback, and local timeout limitation. |
| 5:20–6:20 | `reports/week10/performance.md` | Compare 100/500/1000 offered EPS, actual p50/p95/p99 delay, batch 100k/1m and API sequential latency. Do not claim burst rate is sustained capacity. |
| 6:20–7:00 | ADRs and runbook | Discuss trade-offs, 8 GiB Docker constraint, one broker/partition, and cloud scale direction. |

## Questions to rehearse

1. **Where is the exactly-once boundary?** Iceberg Silver merges by transaction ID are idempotent. Kafka output may be at-least-once; no end-to-end exactly-once claim.
2. **Why is 1,000-EPS p95 lower than 500-EPS p95?** These are short bursts with different scheduling/batch boundaries, not a monotonic load curve or sustained capacity measurement. Show the measured delay, sparse backlog samples, and resource cap; do not assert a causal bottleneck that was not isolated.
3. **How do you prevent leakage in ML?** Hourly feature windows exclude the current transaction and use temporal validation. PaySim balance shortcuts were rejected for the safe model.
4. **What happens if PostgreSQL is unavailable?** Incoming quality/warehouse step fails and blocks Gold publication; Bronze remains intact. Retry is idempotent.
5. **What does the model timeout do?** The API returns a degraded rule-only decision. A hung thread is bounded to one slot, not killed; a separate model-serving process would be needed in production.
6. **Is the Power BI page live?** Curated snapshots are not live. If no cloud report is present, say so and show the workbook/Streamlit validation only as local evidence.
