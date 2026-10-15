import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, get_current_context, task

PROJECT = Path(__file__).resolve().parents[2]
WAREHOUSE = "data/tmp/week2-benchmark-100k-v1"


def run(*command: str) -> None:
    # Không dùng shell=True: task log rõ lệnh và không có shell injection.
    print("RUN:", " ".join(command), flush=True)
    subprocess.run(command, cwd=PROJECT, check=True)


def requested_input_date() -> str | None:
    """An optional simulated event date from Trigger DAG > Config."""
    value = (get_current_context()["dag_run"].conf or {}).get("input_date")
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("input_date must be a YYYY-MM-DD string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise ValueError("input_date must be YYYY-MM-DD") from error
    if parsed.isoformat() != value:
        raise ValueError("input_date must be YYYY-MM-DD")
    return value


@dag(
    dag_id="fraud_week9_batch_100k",
    start_date=pendulum.datetime(2026, 10, 15, tz="UTC"),
    schedule=None,  # Dataset demo cố định: trigger thủ công/backfill.
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,  # Không để Spark và dbt chạy đồng thời.
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["fraud", "batch", "quality"],
)
def fraud_week9_batch_100k():
    @task
    def preflight():
        requested_input_date()  # Fail before Spark starts if backfill config is invalid.
        sample = PROJECT / "data/samples/representative_100k.parquet"
        if not sample.is_file():
            raise FileNotFoundError(sample)
        run("docker", "compose", "--profile", "warehouse", "config", "--quiet")

    @task
    def bronze():
        # Stable transaction_id + insert-new logic của Tuần 2:
        # retry cùng sample không tạo thêm Bronze rows.
        input_date = requested_input_date()
        flags = (f"BRONZE_FLAGS=--input-date {input_date}",) if input_date else ()
        run("make", "bronze-100k", f"SPARK_WAREHOUSE={WAREHOUSE}", *flags)

    @task
    def silver():
        input_date = requested_input_date()
        flags = (f"SILVER_FLAGS=--input-date {input_date}",) if input_date else ()
        run("make", "silver", f"SPARK_WAREHOUSE={WAREHOUSE}", *flags)

    @task
    def lake_quality():
        run(sys.executable, "scripts/week9_lake_quality.py")

    @task
    def export_incoming():
        # Task này khởi động PostgreSQL rồi chạy Spark JDBC một lần.
        run("make", "warehouse-export-100k", f"WAREHOUSE_SAMPLE={WAREHOUSE}")

    @task
    def incoming_quality():
        run(
            sys.executable,
            "scripts/week9_quality_gate.py",
            "--min-rows",
            "90000",
            "--max-rows",
            "110000",
        )

    @task
    def merge_raw():
        # ON CONFLICT(transaction_id) DO NOTHING của Tuần 3.
        run("make", "warehouse-merge")

    @task
    def build_gold():
        # dbt build chạy models + tests; failure không được gọi là Gold success.
        run("make", "dbt-build")

    (
        preflight()
        >> bronze()
        >> silver()
        >> lake_quality()
        >> export_incoming()
        >> incoming_quality()
        >> merge_raw()
        >> build_gold()
    )


fraud_week9_batch_100k()
