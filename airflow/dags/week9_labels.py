import subprocess
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, get_current_context, task

PROJECT = Path(__file__).resolve().parents[2]


@dag(
    dag_id="fraud_week9_daily_labels",
    start_date=pendulum.datetime(2026, 10, 15, tz="UTC"),
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=15),
    },
    tags=["fraud", "labels"],
)
def fraud_week9_daily_labels():
    @task
    def aggregate():
        subprocess.run(["make", "warehouse-up"], cwd=PROJECT, check=True)
        # Manual runs have logical_date=None in Airflow 3. run_after is stable
        # for this DagRun, so a retry uses exactly the same label cutoff.
        as_of = get_current_context()["dag_run"].run_after.isoformat()

        # Không truyền --publish: label aggregation không cần Redpanda đang bật.
        subprocess.run(
            [
                "docker",
                "compose",
                "--profile",
                "warehouse",
                "--profile",
                "rules",
                "run",
                "--rm",
                "rules",
                "python",
                "scripts/simulate_labels.py",
                "--run-id",
                "week5-smoke-v1",
                "--as-of",
                as_of,
            ],
            cwd=PROJECT,
            check=True,
        )

    aggregate()


fraud_week9_daily_labels()
