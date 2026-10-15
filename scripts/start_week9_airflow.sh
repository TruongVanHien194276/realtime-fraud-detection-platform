#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
source .venv/airflow/bin/activate

export AIRFLOW_HOME="$PROJECT_ROOT/data/tmp/airflow-home"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_ROOT/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__PARALLELISM=1
export AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES=1
export AIRFLOW__API__HOST=127.0.0.1
export AIRFLOW__API__PORT=8082
export AIRFLOW__API__BASE_URL=http://127.0.0.1:8082
export AIRFLOW__WORKERS__EXECUTION_API_SERVER_URL=http://127.0.0.1:8082/execution/

# Every Airflow component must use the same signing key for task identity tokens.
# The key lives in the ignored local airflow.cfg; do not print or commit it.
export AIRFLOW__API_AUTH__JWT_SECRET="$(airflow config get-value api_auth jwt_secret)"

exec airflow standalone
