"""Export small, privacy-safe Gold/rules marts for Fabric or Power BI Service.

Run in the `bi` Compose service. Snapshot output is ignored by Git because it
contains row-level synthetic transactions and a local HMAC key is retained.
"""

import argparse
import hashlib
import json
import os
import secrets
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg.rows import dict_row

from fraud_platform.bi_snapshot import alert_confusion, masked_customer


def fetch(conn: psycopg.Connection, sql: str, params: tuple = ()) -> list[dict]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def local_key(path: Path) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        # The key stays in ignored data/tmp; do not put it in the workbook or manifest.
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w") as file:
            file.write(secrets.token_hex(32))
    return bytes.fromhex(path.read_text().strip())


def serializable(rows: list[dict]) -> list[dict]:
    return json.loads(json.dumps(rows, default=str))


def write_table(directory: Path, name: str, rows: list[dict]) -> dict:
    if not rows:
        raise ValueError(f"{name} is empty")
    table = pa.Table.from_pylist(rows)
    parquet_path = directory / f"{name}.parquet"
    json_path = directory / f"{name}.json"
    pq.write_table(table, parquet_path, compression="zstd")
    json_path.write_text(json.dumps(serializable(rows), ensure_ascii=False, indent=2))
    return {
        "rows": len(rows),
        "columns": table.column_names,
        "sha256": hashlib.sha256(parquet_path.read_bytes()).hexdigest(),
        "parquet": parquet_path.name,
        "json": json_path.name,
    }


def export(run_id: str, output_root: Path) -> Path:
    exported_at = datetime.now(UTC)
    snapshot_id = exported_at.strftime("%Y%m%dT%H%M%SZ") + f"-{run_id}"
    directory = output_root / snapshot_id
    directory.mkdir(parents=True, exist_ok=False)
    key = local_key(Path("data/tmp/bi_mask_key.hex"))

    with psycopg.connect() as conn:
        daily = fetch(
            conn,
            """SELECT event_date, transaction_type,
                      count(*)::int AS transaction_count,
                      sum(amount)::double precision AS transaction_value,
                      count(*) FILTER (WHERE is_fraud_actual=1)::int AS actual_fraud_count,
                      coalesce(sum(amount) FILTER (WHERE is_fraud_actual=1),0)::double precision
                        AS actual_fraud_amount,
                      count(*) FILTER (WHERE origin_balance_mismatch
                        OR destination_balance_mismatch)::int AS balance_mismatch_count
               FROM analytics_marts.fact_transactions
               GROUP BY 1,2 ORDER BY 1,2""",
        )
        gold_max_event_at = fetch(
            conn, "SELECT max(event_time) AS value FROM analytics_marts.fact_transactions"
        )[0]["value"]
        decisions = fetch(
            conn,
            """SELECT d.transaction_id, d.customer_id, d.source_event_time,
                      d.decided_at, d.decision, d.risk_score::double precision AS risk_score,
                      d.amount::double precision AS amount, d.processing_latency_ms,
                      d.rule_version, d.reason_codes::text AS reason_codes,
                      l.label, l.label_observed_at,
                      f.transaction_type, f.event_date
               FROM fraud_ops.decisions d
               LEFT JOIN fraud_ops.labels l ON l.run_id=d.run_id
                 AND l.transaction_id=d.transaction_id
               LEFT JOIN analytics_marts.fact_transactions f
                 ON f.transaction_id=d.transaction_id
               WHERE d.run_id=%s ORDER BY d.source_event_time,d.transaction_id""",
            (run_id,),
        )
        if not decisions:
            raise ValueError(f"No decisions for run_id={run_id}")
        customer_ids = sorted({row["customer_id"] for row in decisions})
        timeline = fetch(
            conn,
            """SELECT transaction_id, customer_id, event_time, event_date,
                      transaction_type, amount::double precision AS amount,
                      is_fraud_actual
               FROM analytics_marts.fact_transactions
               WHERE customer_id=ANY(%s)
               ORDER BY event_time,transaction_id""",
            (customer_ids,),
        )

    for row in decisions:
        row["customer_key"] = masked_customer(row.pop("customer_id"), key)
        row["confusion_class"] = alert_confusion(row["decision"], row["label"])
        row["alert_flag"] = row["decision"] != "APPROVE"
        row["labeled_flag"] = row["label"] is not None
        row["actual_fraud_alert_amount"] = (
            row["amount"] if row["label"] == 1 and row["alert_flag"] else 0.0
        )
    for row in timeline:
        row["customer_key"] = masked_customer(row.pop("customer_id"), key)

    performance: list[dict] = []
    by_day: dict[tuple, dict] = {}
    for row in decisions:
        day = str(row["event_date"] or row["source_event_time"].date())
        kind = row["transaction_type"] or "UNKNOWN"
        bucket = by_day.setdefault(
            (day, kind),
            {
                "event_date": day,
                "transaction_type": kind,
                "decision_count": 0,
                "approve_count": 0,
                "review_count": 0,
                "block_count": 0,
                "labeled_count": 0,
                "tp": 0,
                "fp": 0,
                "fn": 0,
                "tn": 0,
                "alert_amount_proxy": 0.0,
                "captured_fraud_amount_proxy": 0.0,
            },
        )
        bucket["decision_count"] += 1
        bucket[row["decision"].lower() + "_count"] += 1
        bucket["labeled_count"] += int(row["labeled_flag"])
        if row["confusion_class"]:
            bucket[row["confusion_class"].lower()] += 1
        if row["alert_flag"]:
            bucket["alert_amount_proxy"] += row["amount"]
        bucket["captured_fraud_amount_proxy"] += row["actual_fraud_alert_amount"]
    performance = list(by_day.values())

    date_values = sorted({str(row["event_date"]) for row in daily})
    dim_date = [{"event_date": day} for day in date_values]
    dim_type = [
        {"transaction_type": kind} for kind in sorted({row["transaction_type"] for row in daily})
    ]
    rule_codes = sorted({code for row in decisions for code in json.loads(row["reason_codes"])})
    dim_rule = [{"rule_code": code} for code in rule_codes] or [{"rule_code": "NO_RULE"}]
    latencies = sorted(row["processing_latency_ms"] for row in decisions)
    health = [
        {
            "run_id": run_id,
            "snapshot_at": exported_at,
            "gold_max_event_at": gold_max_event_at,
            "scored_max_event_at": max(row["source_event_time"] for row in decisions),
            "decision_count": len(decisions),
            "labeled_count": sum(row["labeled_flag"] for row in decisions),
            "latency_p50_ms": latencies[len(decisions) // 2],
            "latency_p95_ms": latencies[int(len(decisions) * 0.95)],
            "freshness_kind": "historical_replay_snapshot",
        }
    ]

    tables = {
        "fact_fraud_daily": daily,
        "fact_fraud_decisions": decisions,
        "fact_alert_performance": performance,
        "fact_customer_timeline": timeline,
        "fact_pipeline_health": health,
        "dim_date": dim_date,
        "dim_transaction_type": dim_type,
        "dim_rule": dim_rule,
    }
    manifest = {
        "snapshot_id": snapshot_id,
        "exported_at": exported_at.isoformat(),
        "run_id": run_id,
        "semantic_note": "Historical PaySim replay, not live production traffic",
        "tables": {name: write_table(directory, name, rows) for name, rows in tables.items()},
    }
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return directory


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="week5-smoke-v1")
    parser.add_argument("--output-root", type=Path, default=Path("data/exports/bi"))
    args = parser.parse_args()
    print(export(args.run_id, args.output_root))


if __name__ == "__main__":
    main()
