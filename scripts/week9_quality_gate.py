import argparse
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]

# Chỉ đọc bảng incoming. Nếu nó lỗi, tuyệt đối không chạy merge/dbt.
SQL = """
SELECT row_to_json(q)
FROM (
    SELECT
        count(*)::bigint AS row_count,
        count(DISTINCT transaction_id)::bigint AS distinct_ids,
        count(*) FILTER (
            WHERE transaction_id IS NULL OR customer_id IS NULL
               OR recipient_id IS NULL OR event_time_utc IS NULL
               OR event_date IS NULL OR source_dataset_sha256 IS NULL
        )::bigint AS required_nulls,
        count(*) FILTER (
            WHERE amount IS NULL OR amount < 0
        )::bigint AS invalid_amounts,
        count(*) FILTER (
            WHERE transaction_type NOT IN
                ('CASH_IN', 'CASH_OUT', 'DEBIT', 'PAYMENT', 'TRANSFER')
        )::bigint AS invalid_types,
        count(DISTINCT source_dataset_sha256)::bigint AS source_hashes,
        min(event_date)::text AS first_event_date,
        max(event_date)::text AS last_event_date
    FROM warehouse_raw.transactions_silver_incoming
) AS q;
"""


def read_metrics() -> dict:
    command = [
        "docker",
        "compose",
        "--profile",
        "warehouse",
        "exec",
        "-T",
        "postgres",
        "psql",
        "-X",
        "-A",
        "-t",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        "fraud",
        "-d",
        "fraud_dw",
        "-c",
        SQL,
    ]
    completed = subprocess.run(
        command,
        cwd=PROJECT,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(completed.stdout.strip())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-rows", type=int, default=90_000)
    parser.add_argument("--max-rows", type=int, default=110_000)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/tmp/week9/incoming-quality.json"),
    )
    args = parser.parse_args()

    try:
        metrics = read_metrics()
    except subprocess.CalledProcessError as error:
        # Persist the negative gate result even when the warehouse cannot be
        # queried; Airflow must not continue to merge/build Gold on this path.
        report = {
            "checked_at_utc": datetime.now(UTC).isoformat(),
            "source": "warehouse_raw.transactions_silver_incoming",
            "passed": False,
            "failure_reason": "WAREHOUSE_UNAVAILABLE",
            "upstream_exit_code": error.returncode,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
        raise SystemExit("QUALITY GATE FAILED: PostgreSQL unavailable") from error
    checks = {
        "volume": args.min_rows <= metrics["row_count"] <= args.max_rows,
        "unique_transaction_id": (metrics["row_count"] == metrics["distinct_ids"]),
        "required_columns_not_null": metrics["required_nulls"] == 0,
        "nonnegative_amount": metrics["invalid_amounts"] == 0,
        "accepted_transaction_type": metrics["invalid_types"] == 0,
        "one_source_dataset": metrics["source_hashes"] == 1,
        "simulated_event_date_present": metrics["last_event_date"] is not None,
    }

    report = {
        "checked_at_utc": datetime.now(UTC).isoformat(),
        "source": "warehouse_raw.transactions_silver_incoming",
        "metrics": metrics,
        "checks": checks,
        "passed": all(checks.values()),
        "freshness_note": (
            "PaySim event dates are simulated; do not compare them with today's wall-clock date."
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))

    if not report["passed"]:
        raise SystemExit("QUALITY GATE FAILED; Gold was not published")


if __name__ == "__main__":
    main()
