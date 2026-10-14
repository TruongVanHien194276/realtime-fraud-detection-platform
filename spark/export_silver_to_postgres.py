import argparse
import os
from pathlib import Path

from pyspark.sql import functions as F

from spark.lakehouse_common import build_spark


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=Path("data/tmp/week2-benchmark-100k-v1"),
    )
    args = parser.parse_args()

    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("POSTGRES_PASSWORD chưa được truyền vào Spark container")

    spark = build_spark("fraud-week3-export", args.warehouse)

    try:
        source = spark.table("lake.silver.transactions_valid")

        export = source.select(
            "transaction_id",
            "step",
            "transaction_type",
            "amount",
            F.col("nameOrig").alias("customer_id"),
            F.col("nameDest").alias("recipient_id"),
            # Đưa UTC vào chuỗi có hậu tố Z để PostgreSQL cast thành timestamptz rõ ràng.
            F.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_time_utc"),
            "event_date",
            F.col("isFraud").alias("is_fraud_actual"),
            F.col("isFlaggedFraud").alias("is_flagged_fraud"),
            "origin_balance_mismatch",
            "destination_balance_mismatch",
            F.col("batch_id").alias("silver_batch_id"),
            "source_dataset_sha256",
        )

        row_count = export.count()
        print(f"Silver rows selected: {row_count}")

        # Chỉ overwrite bảng incoming dùng để chuyển dữ liệu, KHÔNG overwrite
        # bảng raw bền vững. Sau đó PostgreSQL sẽ insert idempotent theo ID.
        (
            export.coalesce(2)
            .write.format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/fraud_dw")
            .option("dbtable", "warehouse_raw.transactions_silver_incoming")
            .option("user", "fraud")
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", "1000")
            .mode("overwrite")
            .save()
        )

        print(f"Exported {row_count} rows to incoming table")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
