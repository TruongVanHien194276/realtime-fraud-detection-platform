# Fraud platform local operations runbook

This runbook is for the 8 GB Apple M3 development machine with a roughly 4 GB Docker limit. Run Spark, ML training, and the monitoring demo in separate sessions when memory is tight. Airflow is local development infrastructure, not a production deployment.

## Check the pipeline before changing state

1. Open `http://127.0.0.1:8082` and inspect the failed DAG task and its attempt log. Run `bash scripts/start_week9_airflow.sh` if the local Airflow server is not running.
2. Check the warehouse with `docker compose --profile warehouse up -d --wait postgres`, then `docker compose --profile warehouse exec -T postgres pg_isready -U fraud -d fraud_dw`.
3. Run `python3 scripts/week9_data_quality_report.py` after a successful batch. This verifies volume, uniqueness, required schema, dimension relationships, 28 dbt tests, and the age of the latest checks. PaySim event dates are simulated; do not compare them with today's date.
4. During a monitoring demo, inspect `http://127.0.0.1:3009/d/fraud-operations-week9/fraud-operations-week-9` and the [Airflow status dashboard](http://127.0.0.1:3009/d/fraud-week9-airflow-runs/fraud-platform-airflow-dag-runs). The latter reads only the three project DAGs from Airflow metadata; it is not the Airflow UI. A missing metric is not zero: check the `up` panel and service status first.

## Broker unavailable or consumer lag growing

Detect a broker outage with `docker compose --profile streaming exec -T redpanda rpk cluster health`; in Prometheus, `up{job="redpanda"}=0` means the scrape target is unreachable. A rising `redpanda_kafka_consumer_group_lag_sum` means a known consumer group is behind; no series means the broker, metric configuration, or group may be absent. Redpanda exposes this gauge only when `enable_consumer_group_metrics` includes `consumer_lag` ([Redpanda monitoring guide](https://docs.redpanda.com/streaming/current/manage/monitoring/)).

1. Stop replay producers while the broker is unavailable. Do not reset a producer cursor or delete a Spark checkpoint.
2. Start only the broker: `docker compose --profile streaming up -d redpanda`, then repeat the health check.
3. Resume the consumer and producer with their original cursor/checkpoint paths. The transaction ID and insert-only lake merge make replays idempotent; verify the counts with `make verify-lakehouse SPARK_WAREHOUSE=data/tmp/week2-benchmark-100k-v1` when the batch lake is involved.
4. Confirm lag falls and the processed/decision counters advance. If they do not, inspect consumer logs and the dead-letter/quarantine path before sending more traffic.

## Spark checkpoint appears corrupted

Detect this through a repeatable Structured Streaming startup failure referring to checkpoint metadata, or a batch audit that no longer agrees with the checkpoint. Stop the stream job first. Keep the original checkpoint untouched for diagnosis; never remove it merely to make the job start.

1. Record the existing `--stream-id`, `--checkpoint`, topic offsets, and Iceberg audit rows. Copy the affected checkpoint to a uniquely named incident backup outside the active path.
2. Restore from a known-good backup when available. Otherwise start a new recovery stream with a **new** checkpoint path and stream ID, using `--starting-offsets earliest` only after confirming that the broker retains the required offsets. This may replay events; the Silver merge remains keyed by transaction ID.
3. Compare audit batch IDs, Silver IDs, and rejected counts before and after recovery. Do not publish downstream aggregates until reconciliation passes.
4. If retained offsets are insufficient, stop and document the irrecoverable gap; do not invent records or mark the stream healthy.

## PostgreSQL unavailable

Detect with `pg_isready`, Airflow task failures, `fraud_db_up=0`, or `up{job="fraud_exporter"}=0`. A failed warehouse or quality step must block merge and Gold build; Bronze data remains in Iceberg.

1. Run `docker compose --profile warehouse up -d --wait postgres` and inspect `docker compose --profile warehouse logs --tail=100 postgres`.
2. Do not clear the PostgreSQL volume. Check disk free space and container memory before retrying Airflow.
3. Retry the failed task/run using the same batch and input date. `ON CONFLICT (transaction_id) DO NOTHING` prevents duplicate warehouse rows.
4. Re-run the incoming quality gate and dbt build; confirm `reports/week9_data_quality.json` passes and Gold row count still matches raw.

## Schema mismatch or quality gate failure

Detect with CSV contract errors, Silver quarantine reason codes, `incoming-quality.json` failing, or dbt tests failing. The DAG orders `lake_quality` and `incoming_quality` before `merge_raw` and `build_gold`; a failure stops publication without deleting Bronze.

1. Pause new batch triggers and inspect the exact failing record/column and source hash. Keep the raw and Bronze files immutable.
2. If the schema truly changed, version the data contract and transformation, then test on an isolated warehouse. Do not simply disable the failing check.
3. Reprocess the affected input date with the same `input_date` and verify Bronze = valid Silver + quarantine. For a quality-policy change, plan a controlled Silver rebuild; insert-only replay alone will not move an existing ID between valid and quarantine.
4. After the checks pass, rerun the DAG and compare unique transaction IDs in incoming, raw, and Gold. Preserve the failed report for the incident record.

## Restart the monitoring demo

Run `docker compose --profile monitoring --profile warehouse up -d fraud_exporter prometheus grafana`. Add `--profile api` and `--profile streaming` services only when showing live API or broker panels. The exporter uses read-only SQL, Airflow metadata and the latest batch quality report; persisted-decision counts are historical snapshots, not a live throughput claim. For PNG capture only, run `docker compose --profile monitoring-render up -d grafana_renderer` and use Grafana's `/render/d/...` endpoint. The renderer uses up to 1 GB, so do not run it alongside Spark or retraining. When finished, stop only the demo services with `docker compose --profile monitoring --profile monitoring-render stop grafana_renderer grafana prometheus fraud_exporter` to free memory without touching PostgreSQL, Airflow, or broker data.

## Hybrid API model timeout or saturation

The `/score` response remains HTTP 200 when the model is too slow or its one local inference slot is occupied. Inspect `degraded=true`, `model_score=null`, and `reason_codes` containing `MODEL_INFERENCE_TIMEOUT` plus `FALLBACK_RULE_ONLY`; `/ready` only reports whether a model loaded at startup, not whether every later inference met its deadline. Do not interpret a fast fallback response as successful model inference. The local thread pool bounds outstanding inference work to one slot but cannot forcibly kill a hung Python thread. If timeout responses persist, stop new load, inspect `/model-info` and application logs, restart the API after checking the model artifact, and consider a separate model-serving process with a circuit breaker for production. Verify the fallback with `docker compose --profile api run --rm api pytest -q tests/unit/test_week10_api_timeout.py`.
