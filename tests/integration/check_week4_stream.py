import argparse
import json
from pathlib import Path

from pyspark.sql import functions as F

from spark.lakehouse_common import build_spark


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--brokers", default="redpanda:9092")
    parser.add_argument("--expected-valid", type=int, default=999)
    parser.add_argument("--require-dlq", action="store_true")
    parser.add_argument("--replay-dir", type=Path)
    args = parser.parse_args()
    if args.require_dlq and args.replay_dir is None:
        parser.error("--require-dlq cần --replay-dir để xác nhận late probe cụ thể")

    spark = build_spark("verify-week4-stream", args.warehouse)
    try:
        silver = spark.table("lake.silver.stream_transactions")
        total = silver.count()
        unique = silver.select("transaction_id").distinct().count()

        assert total == args.expected_valid, f"Silver rows={total}, expected={args.expected_valid}"
        assert unique == total, f"Duplicate transaction_id: rows={total}, distinct={unique}"
        assert silver.select("event_id").distinct().count() == total
        assert silver.where(F.col("source_event_key").isNull()).count() == 0

        latency = silver.selectExpr(
            "percentile_approx(processing_delay_ms, array(0.5, 0.95)) as p"
        ).first()["p"]

        invalid = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", args.brokers)
            .option("subscribe", "transactions.invalid")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )
        dlq = invalid.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.get_json_object(F.col("value").cast("string"), "$.source_event_key").alias(
                "source_event_key"
            ),
            F.get_json_object(F.col("value").cast("string"), "$.raw_payload").alias("raw_payload"),
            F.get_json_object(F.col("value").cast("string"), "$.error_code").alias("error_code"),
        )
        reasons = {row["error_code"] for row in dlq.select("error_code").distinct().collect()}

        if args.require_dlq:
            assert {"LATE_EVENT", "UNSUPPORTED_SCHEMA_VERSION", "MALFORMED_JSON"}.issubset(
                reasons
            ), reasons
            bad_trace = dlq.where(
                F.col("source_event_key").isNull()
                | F.col("raw_payload").isNull()
                | (F.col("kafka_key") != F.col("source_event_key"))
            ).count()
            assert bad_trace == 0, f"DLQ rows without source trace: {bad_trace}"

            part_files = list(args.replay_dir.glob("part-*"))
            assert len(part_files) == 1, f"Expected one replay file: {part_files}"
            with part_files[0].open() as replay_file:
                first_row = json.loads(replay_file.readline())
            withheld_id = first_row["transaction_id"]
            assert silver.where(F.col("transaction_id") == withheld_id).count() == 0
            matching_late = dlq.where(
                (F.col("error_code") == "LATE_EVENT")
                & (F.get_json_object(F.col("raw_payload"), "$.transaction_id") == withheld_id)
            ).count()
            assert matching_late >= 1, f"Withheld row {withheld_id} not in late DLQ"

            raw = (
                spark.read.format("kafka")
                .option("kafka.bootstrap.servers", args.brokers)
                .option("subscribe", "transactions.raw")
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load()
            )
            raw_ids = raw.select(
                F.get_json_object(F.col("value").cast("string"), "$.transaction_id").alias(
                    "transaction_id"
                )
            ).where(F.col("transaction_id").isNotNull())
            assert raw_ids.count() > raw_ids.distinct().count(), "No duplicate input in raw topic"

        audit = spark.table("lake.audit.stream_batches")
        completed_batches = audit.count()
        assert completed_batches > 0

        print(
            {
                "silver_rows": total,
                "distinct_transaction_ids": unique,
                "completed_batches": completed_batches,
                "p50_processing_delay_ms": latency[0],
                "p95_processing_delay_ms": latency[1],
                "dlq_error_codes": sorted(reasons),
                "result": "PASS",
            }
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
