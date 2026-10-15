"""Small read-only Prometheus exporter for the local fraud demo."""

import json
import os
import sqlite3
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

LAKE_QUALITY = Path("data/tmp/week9/lake-quality.json")
AIRFLOW_DB = Path("data/tmp/airflow-home/airflow.db")
RUN_ID = os.environ.get("FRAUD_RUN_ID", "week5-smoke-v1")
AIRFLOW_DAGS = (
    "fraud_week9_batch_100k",
    "fraud_week9_daily_labels",
    "fraud_week9_candidate_retraining",
)

SQL = """
SELECT
  count(*)::bigint AS decisions,
  count(*) FILTER (WHERE decision = 'REVIEW')::bigint AS review,
  count(*) FILTER (WHERE decision = 'BLOCK')::bigint AS blocked,
  count(*) FILTER (WHERE decided_at >= now() - interval '5 minutes')::bigint AS recent,
  coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY processing_latency_ms), 0)
    AS latency_p95_ms,
  count(*) FILTER (WHERE features ->> 'seconds_since_previous' IS NULL)::bigint
    AS feature_nulls,
  (SELECT count(*) FROM fraud_ops.labels WHERE run_id = %s)::bigint AS labels,
  (SELECT count(*) FROM warehouse_raw.transactions_silver_incoming)::bigint
    AS incoming_rows
FROM fraud_ops.decisions
WHERE run_id = %s;
"""

HELP = {
    "fraud_db_up": "PostgreSQL query succeeded",
    "fraud_decisions_count": "Persisted decisions for the configured demo run",
    "fraud_review_count": "Persisted REVIEW decisions",
    "fraud_block_count": "Persisted BLOCK decisions",
    "fraud_decision_rate_per_second": "Decisions over the previous five minutes / 300",
    "fraud_e2e_latency_p95_seconds": "p95 ingestion-to-decision latency",
    "fraud_feature_null_ratio": "Share of decisions without seconds_since_previous",
    "fraud_labels_count": "Matured simulated labels",
    "fraud_incoming_rows": "Rows in the latest warehouse incoming export",
    "fraud_lake_quality_available": "Latest lake quality report exists",
    "fraud_lake_processed_count": "Bronze rows in the latest batch quality report",
    "fraud_lake_rejected_count": "Silver quarantine rows in the latest batch quality report",
    "fraud_lake_quality_passed": "Latest lake quality report passed",
}


def collect() -> dict[str, int | float]:
    metrics: dict[str, int | float] = {"fraud_db_up": 0, "fraud_lake_quality_available": 0}
    try:
        with psycopg.connect(
            host=os.environ.get("PGHOST", "postgres"),
            port=os.environ.get("PGPORT", "5432"),
            user=os.environ.get("PGUSER", "fraud"),
            password=os.environ["PGPASSWORD"],
            dbname=os.environ.get("PGDATABASE", "fraud_dw"),
            connect_timeout=2,
            row_factory=dict_row,
        ) as conn:
            row = conn.execute(SQL, (RUN_ID, RUN_ID)).fetchone()
        metrics.update(
            fraud_db_up=1,
            fraud_decisions_count=row["decisions"],
            fraud_review_count=row["review"],
            fraud_block_count=row["blocked"],
            fraud_decision_rate_per_second=row["recent"] / 300,
            fraud_e2e_latency_p95_seconds=float(row["latency_p95_ms"]) / 1000,
            fraud_feature_null_ratio=(
                row["feature_nulls"] / row["decisions"] if row["decisions"] else 0
            ),
            fraud_labels_count=row["labels"],
            fraud_incoming_rows=row["incoming_rows"],
        )
    except (psycopg.Error, KeyError) as error:
        # Keep the exporter scrapeable while making DB unavailability explicit.
        print(f"PostgreSQL metrics unavailable: {error}", flush=True)

    try:
        report = json.loads(LAKE_QUALITY.read_text(encoding="utf-8"))
        metrics.update(
            fraud_lake_quality_available=1,
            fraud_lake_processed_count=report["bronze_rows"],
            fraud_lake_rejected_count=report["rejected_rows"],
            fraud_lake_quality_passed=int(report["passed"]),
        )
    except (OSError, ValueError, KeyError) as error:
        print(f"Lake quality metrics unavailable: {error}", flush=True)
    return metrics


def collect_airflow() -> list[tuple[str, int, int, int]]:
    """Read only the latest run of each known project DAG; never expose user DAGs."""
    try:
        uri = AIRFLOW_DB.resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=1)) as conn:
            runs = conn.execute(
                "SELECT dag_id, run_id, state FROM dag_run "
                "WHERE dag_id IN (?, ?, ?) ORDER BY id DESC",
                AIRFLOW_DAGS,
            ).fetchall()
            latest: list[tuple[str, int, int, int]] = []
            seen: set[str] = set()
            for dag_id, run_id, state in runs:
                if dag_id in seen:
                    continue
                seen.add(dag_id)
                successful, total = conn.execute(
                    "SELECT count(*) FILTER (WHERE state='success'), count(*) "
                    "FROM task_instance WHERE dag_id=? AND run_id=?",
                    (dag_id, run_id),
                ).fetchone()
                latest.append((dag_id, int(state == "success"), successful, total))
            return latest
    except (OSError, sqlite3.Error) as error:
        print(f"Airflow metrics unavailable: {error}", flush=True)
        return []


def render(metrics: dict[str, int | float]) -> bytes:
    lines = []
    for name, value in metrics.items():
        lines.extend((f"# HELP {name} {HELP[name]}", f"# TYPE {name} gauge", f"{name} {value}"))
    lines.extend(
        (
            "# HELP fraud_airflow_latest_run_success Latest run state of a project DAG",
            "# TYPE fraud_airflow_latest_run_success gauge",
            "# HELP fraud_airflow_latest_task_success_count Successful tasks in the latest run",
            "# TYPE fraud_airflow_latest_task_success_count gauge",
            "# HELP fraud_airflow_latest_task_count All tasks in the latest run",
            "# TYPE fraud_airflow_latest_task_count gauge",
        )
    )
    for dag_id, success, successful, total in collect_airflow():
        label = f"{{dag_id={json.dumps(dag_id)}}}"
        lines.extend(
            (
                f"fraud_airflow_latest_run_success{label} {success}",
                f"fraud_airflow_latest_task_success_count{label} {successful}",
                f"fraud_airflow_latest_task_count{label} {total}",
            )
        )
    return ("\n".join(lines) + "\n").encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path != "/metrics":
            self.send_error(404)
            return
        body = render(collect())
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 9109), Handler).serve_forever()
