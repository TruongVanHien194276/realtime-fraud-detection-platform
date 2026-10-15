"""Combine lake, incoming, dbt, and warehouse checks into one reviewable report."""

import argparse
import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
LAKE_REPORT = PROJECT / "data/tmp/week9/lake-quality.json"
INCOMING_REPORT = PROJECT / "data/tmp/week9/incoming-quality.json"
DBT_RESULTS = PROJECT / "dbt/target/run_results.json"

SQL = """
SELECT row_to_json(q) FROM (
  SELECT
    (SELECT count(*) FROM warehouse_raw.transactions_silver) AS raw_rows,
    (SELECT count(*) FROM analytics_marts.fact_transactions) AS gold_rows,
    (SELECT count(DISTINCT transaction_id)
       FROM analytics_marts.fact_transactions) AS gold_unique_ids,
    (SELECT count(*) FROM analytics_marts.fact_transactions
      WHERE transaction_id IS NULL OR customer_id IS NULL
         OR recipient_id IS NULL OR event_date IS NULL) AS gold_required_nulls,
    (SELECT count(*) FROM analytics_marts.fact_transactions f
      LEFT JOIN analytics_marts.dim_customer c USING (customer_id)
      WHERE c.customer_id IS NULL) AS missing_customers,
    (SELECT count(*) FROM analytics_marts.fact_transactions f
      LEFT JOIN analytics_marts.dim_recipient r USING (recipient_id)
      WHERE r.recipient_id IS NULL) AS missing_recipients,
    (SELECT count(*) FROM analytics_marts.fact_transactions f
      LEFT JOIN analytics_marts.dim_date d USING (event_date)
      WHERE d.event_date IS NULL) AS missing_dates,
    (SELECT count(*) FROM information_schema.columns
      WHERE table_schema='analytics_marts' AND table_name='fact_transactions'
        AND column_name IN
          ('transaction_id','customer_id','recipient_id','event_date','amount'))
      AS required_schema_columns
) q;
"""


def read_postgres() -> dict:
    result = subprocess.run(
        [
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
        ],
        cwd=PROJECT,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout.strip())


def recent(timestamp: str, now: datetime) -> bool:
    age = now - datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return timedelta(0) <= age <= timedelta(hours=24)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("reports/week9_data_quality.json"))
    args = parser.parse_args()

    lake = json.loads(LAKE_REPORT.read_text(encoding="utf-8"))
    incoming = json.loads(INCOMING_REPORT.read_text(encoding="utf-8"))
    dbt = json.loads(DBT_RESULTS.read_text(encoding="utf-8"))
    warehouse = read_postgres()
    now = datetime.now(UTC)
    dbt_tests = [r for r in dbt["results"] if r["unique_id"].startswith("test.")]

    checks = {
        "lake_reconciliation": lake["passed"],
        "incoming_contract": incoming["passed"],
        "required_gold_schema": warehouse["required_schema_columns"] == 5,
        "gold_unique": warehouse["gold_rows"] == warehouse["gold_unique_ids"],
        "gold_required_fields": warehouse["gold_required_nulls"] == 0,
        "gold_volume_matches_raw": warehouse["gold_rows"] == warehouse["raw_rows"],
        "customer_referential_integrity": warehouse["missing_customers"] == 0,
        "recipient_referential_integrity": warehouse["missing_recipients"] == 0,
        "date_referential_integrity": warehouse["missing_dates"] == 0,
        "dbt_tests_pass": bool(dbt_tests) and all(r["status"] == "pass" for r in dbt_tests),
        # PaySim event_time is simulated. Freshness here means recent pipeline
        # checks, not event_time compared with the wall clock.
        "lake_check_fresh": recent(lake["checked_at_utc"], now),
        "incoming_check_fresh": recent(incoming["checked_at_utc"], now),
        "dbt_run_fresh": recent(dbt["metadata"]["generated_at"], now),
    }
    report = {
        "checked_at_utc": now.isoformat(),
        "warehouse": warehouse,
        "lake": {
            "bronze_rows": lake["bronze_rows"],
            "valid_rows": lake["valid_rows"],
            "rejected_rows": lake["rejected_rows"],
            "duplicate_ids": lake["duplicate_ids"],
        },
        "incoming": incoming["metrics"],
        "dbt": {
            "generated_at": dbt["metadata"]["generated_at"],
            "test_count": len(dbt_tests),
            "passed_tests": sum(r["status"] == "pass" for r in dbt_tests),
        },
        "checks": checks,
        "passed": all(checks.values()),
        "freshness_note": "Checks are wall-clock fresh; PaySim event dates are simulated.",
    }
    output = PROJECT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    if not report["passed"]:
        raise SystemExit("WEEK9 DATA QUALITY FAILED")


if __name__ == "__main__":
    main()
