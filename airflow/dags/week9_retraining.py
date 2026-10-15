import hashlib
import json
import subprocess
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

PROJECT = Path(__file__).resolve().parents[2]
FEATURES = PROJECT / "data/ml/week7/features.parquet"
PRIOR_REPORT = PROJECT / "artifacts/week9/candidate_report.json"
EVALUATION = PROJECT / "reports/week9_model_evaluation.json"


@dag(
    dag_id="fraud_week9_candidate_retraining",
    start_date=pendulum.datetime(2026, 10, 15, tz="UTC"),
    schedule="0 2 1 * *",  # Monthly; unchanged feature cohorts are skipped.
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["fraud", "ml", "validation-only"],
)
def fraud_week9_candidate_retraining():
    @task.short_circuit
    def has_new_features() -> bool:
        if not FEATURES.is_file():
            raise FileNotFoundError(FEATURES)
        current = hashlib.sha256(FEATURES.read_bytes()).hexdigest()
        if PRIOR_REPORT.is_file() and EVALUATION.is_file():
            previous = json.loads(PRIOR_REPORT.read_text(encoding="utf-8"))
            evaluation = json.loads(EVALUATION.read_text(encoding="utf-8"))
            if (
                previous.get("feature_sha256") == current
                and evaluation.get("feature_sha256") == current
            ):
                print("Feature cohort unchanged; skip retraining")
                return False
        return True

    @task
    def train_candidate():
        subprocess.run(
            [
                "docker",
                "compose",
                "--profile",
                "ml",
                "run",
                "--rm",
                "ml",
                "python",
                "scripts/week9_train_candidate.py",
            ],
            cwd=PROJECT,
            check=True,
        )

    @task
    def evaluate_candidate():
        subprocess.run(
            [
                "docker",
                "compose",
                "--profile",
                "ml",
                "run",
                "--rm",
                "ml",
                "python",
                "scripts/week9_evaluate_candidate.py",
            ],
            cwd=PROJECT,
            check=True,
        )

    has_new_features() >> train_candidate() >> evaluate_candidate()


fraud_week9_candidate_retraining()
