import argparse
import json
from pathlib import Path

from pyspark.sql import functions as F

from spark.lakehouse_common import build_spark


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--stream-id", required=True)
    parser.add_argument("--expected-valid", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    spark = build_spark("week10-stream-metrics", args.warehouse)
    try:
        silver = spark.table("lake.silver.stream_transactions")
        audit = spark.table("lake.audit.stream_batches").where(F.col("stream_id") == args.stream_id)

        count = silver.count()
        distinct_ids = silver.select("transaction_id").distinct().count()

        # Đây là delay từ producer ingested_at tới LÚC BẮT ĐẦU micro-batch;
        # không phải latency tới lúc Iceberg commit hoặc quyết định fraud.
        percentiles = silver.selectExpr(
            "percentile_approx(processing_delay_ms, array(0.5, 0.95, 0.99), 10000) AS p"
        ).first()["p"]

        first_ingested = silver.agg(F.min("ingested_at")).first()[0]
        last_completed = audit.agg(F.max("completed_at")).first()[0]

        if count != args.expected_valid:
            raise AssertionError(f"Expected {args.expected_valid} rows, got {count}")
        if distinct_ids != count:
            raise AssertionError("Duplicate transaction_id in streaming Silver")
        if last_completed is None:
            raise AssertionError("No completed micro-batch in audit table")

        report = {
            "stream_id": args.stream_id,
            "silver_rows": count,
            "distinct_transaction_ids": distinct_ids,
            "processing_delay_p50_ms": percentiles[0],
            "processing_delay_p95_ms": percentiles[1],
            "processing_delay_p99_ms": percentiles[2],
            "first_ingested_at": first_ingested.isoformat(),
            "last_audit_completed_at": last_completed.isoformat(),
            "measurement_note": (
                "processing_delay_ms ends at micro-batch start; "
                "it is not sink-commit or fraud-decision latency"
            ),
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
