import argparse
from pathlib import Path

from pyspark.sql import functions as F

from spark.lakehouse_common import build_spark


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    if args.limit < 2:
        parser.error("--limit phải từ 2 trở lên")
    if args.output.exists():
        parser.error(f"Output đã tồn tại: {args.output}")

    spark = build_spark("prepare-week4-replay", args.warehouse)
    try:
        source = spark.table("lake.silver.transactions_valid")

        # Chọn ổn định theo event_time và transaction_id để lần chạy sau
        # tạo đúng cùng tập giao dịch, bất kể thứ tự file Parquet vật lý.
        sample = (
            source.orderBy("event_time", "transaction_id")
            .limit(args.limit)
            .repartition(1)
            .sortWithinPartitions("event_time", "transaction_id")
        )

        payload = sample.select(
            F.to_json(
                F.struct(
                    F.lit("1.0").alias("schema_version"),
                    F.col("transaction_id"),
                    F.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_time"),
                    F.col("step"),
                    F.col("transaction_type"),
                    F.col("amount").cast("string").alias("amount"),
                    F.col("nameOrig").alias("customer_id"),
                    F.col("nameDest").alias("recipient_id"),
                    F.col("oldbalanceOrg").cast("string").alias("old_balance_origin"),
                    F.col("newbalanceOrig").cast("string").alias("new_balance_origin"),
                    F.col("oldbalanceDest").cast("string").alias("old_balance_destination"),
                    F.col("newbalanceDest").cast("string").alias("new_balance_destination"),
                )
            ).alias("value")
        )

        # Spark tạo thư mục chứa một file part-*.txt.
        payload.write.mode("errorifexists").text(str(args.output))
        print(f"Prepared {args.limit} events at {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
