# KPI catalog — Week 6

All figures are from a versioned historical PaySim replay snapshot. Currency units are simulated PaySim amounts, not realized losses. `analytics_marts.fact_transactions` (Gold, 100k rows) and one selected `fraud_ops.decisions.run_id` (999 rows by default) are separate populations.

| KPI | Source | Definition / denominator |
| --- | --- | --- |
| Transaction value | `fact_fraud_daily` from `analytics_marts.fact_transactions` | Sum `transaction_value` over filtered Gold dates/types. |
| Suspected fraud amount proxy | `fact_alert_performance` from `fraud_ops.decisions` | Sum amount of REVIEW/BLOCK decisions. This is exposure alerted, not proven fraud or saved loss. |
| Approval/review/block rate | `fact_alert_performance` | Each decision count / all decisions in the selected run. |
| Alert precision | `fact_alert_performance` + delayed labels | TP / (TP+FP), on labeled decisions only. REVIEW and BLOCK count as alerts. Blank when denominator is zero. |
| Alert recall | same | TP / (TP+FN), labeled decisions only. |
| False-positive rate | same | FP / (FP+TN), labeled legitimate decisions only. This is different from false block rate. |
| Captured fraud amount proxy | same | Amount where label=1 and decision REVIEW/BLOCK. It is not money recovered. |
| Gold balance mismatch count | `fact_fraud_daily` | Count rows with origin or destination balance mismatch flag. PaySim may contain intentionally inconsistent balances. |
| Ingest-to-decision latency | `fraud_ops.decisions` → `fact_pipeline_health` | P50/P95 of `processing_latency_ms` for the selected run. Worker computes `decided_at - ingested_at`; historical replay may wait hours before the worker runs. Not online serving latency or Kafka end-to-end latency. |
| Data freshness | `fact_pipeline_health` | Historical event max and snapshot export time displayed separately. Do not interpret elapsed wall time as current streaming lag. |

No model version or ML KPI is available before Week 7. `is_fraud_actual` and delayed labels may be used only for retrospective evaluation, never as scoring inputs.
