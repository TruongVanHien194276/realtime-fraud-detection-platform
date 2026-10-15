# Airflow task authentication failure in local batch demo

The first Week 9 batch DAG attempt on 15 October 2026 failed before any data task ran. The API server was listening on port 8082, while Airflow LocalExecutor resolved its execution API to the default port 8080. Another local service answered there and rejected the task token. The incident affected the local demo only; Bronze, warehouse, and Gold were unchanged by the failed run.

## Timeline in UTC

- 06:41:00: `week9-step4-initial` was triggered.
- 06:41:10: `preflight` moved to retry after `Invalid auth token` from the execution API.
- 06:43:20: the second attempt failed for the same reason; the DAG ended failed.
- 06:44: Airflow was restarted with `AIRFLOW__API__BASE_URL` and the worker execution API URL pointing to `127.0.0.1:8082`.
- 06:44:49: `week9-step4-fixed` was triggered; all eight tasks later succeeded.

## Cause and impact

Changing only `AIRFLOW__API__PORT` moved the UI/API listener but did not change LocalExecutor's execution API target. Token validation failed at the wrong endpoint. Because `preflight` was the first task, no Bronze, Silver, incoming, merge, or Gold write occurred during the failed run. Airflow retry correctly attempted the task a second time, but a configuration mismatch cannot be repaired by retry alone.

## Recovery and prevention

The startup script now sets the UI host/port, base URL, worker execution API URL, and a shared JWT secret from the ignored local Airflow configuration. The recovered run, a date backfill, and an identical replay each completed 8/8 tasks. The replay left Bronze, incoming, warehouse, and Gold at 100,000 unique transactions. A negative incoming-volume test returned failure while Gold remained unchanged. These checks are recorded in `reports/week9_batch_idempotency.json`.

Before future demo runs, verify `curl -I http://127.0.0.1:8082/`, check `airflow dags list-import-errors`, and run a preflight task before starting Spark. A future automated startup check should assert that the configured execution API URL uses the same port as the API server.
