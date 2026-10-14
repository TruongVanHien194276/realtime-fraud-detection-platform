import argparse
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from time import perf_counter

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.lakehouse_common import build_spark, insert_new_transactions

ALLOWED_TYPES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]


def add_quality_checks(bronze: DataFrame) -> DataFrame:
    tx_type = F.upper(F.trim(F.col("type")))
    amount = F.col("amount")
    old_origin = F.col("oldbalanceOrg")
    new_origin = F.col("newbalanceOrig")
    old_dest = F.col("oldbalanceDest")
    new_dest = F.col("newbalanceDest")

    # CASH_IN tăng số dư origin; các loại còn lại thường giảm.
    # Đây là kiểm tra tham khảo cho PaySim, không tự ý sửa số dư nguồn.
    expected_origin = F.when(tx_type == "CASH_IN", old_origin + amount).otherwise(
        old_origin - amount
    )

    origin_mismatch = F.when(
        old_origin.isNotNull() & new_origin.isNotNull() & amount.isNotNull(),
        F.abs(new_origin - expected_origin) > F.lit(Decimal("0.01")),
    ).otherwise(F.lit(False))

    # Kiểm tra destination chỉ áp dụng có điều kiện; một số loại giao dịch
    # trong PaySim không có diễn biến balance như transfer thông thường.
    destination_mismatch = F.when(
        tx_type.isin("TRANSFER", "CASH_OUT")
        & old_dest.isNotNull()
        & new_dest.isNotNull()
        & amount.isNotNull(),
        F.abs(new_dest - (old_dest + amount)) > F.lit(Decimal("0.01")),
    ).otherwise(F.lit(False))

    checked = (
        bronze.withColumn("transaction_type", tx_type)
        .withColumn("origin_balance_mismatch", origin_mismatch)
        .withColumn("destination_balance_mismatch", destination_mismatch)
    )

    # Mỗi điều kiện sai sinh một reason code. Một record có thể có nhiều lỗi.
    error_candidates = F.array(
        F.when(
            F.col("_corrupt_record").isNotNull(),
            F.lit("CSV_PARSE_ERROR"),
        ),
        F.when(
            F.col("step").isNull() | (F.col("step") < 0),
            F.lit("INVALID_STEP"),
        ),
        F.when(
            amount.isNull() | (amount < 0),
            F.lit("INVALID_AMOUNT"),
        ),
        F.when(
            F.col("transaction_type").isNull() | (~F.col("transaction_type").isin(*ALLOWED_TYPES)),
            F.lit("INVALID_TRANSACTION_TYPE"),
        ),
        F.when(
            F.col("nameOrig").isNull()
            | (F.length(F.trim(F.col("nameOrig"))) == 0)
            | F.col("nameDest").isNull()
            | (F.length(F.trim(F.col("nameDest"))) == 0),
            F.lit("MISSING_ACCOUNT_ID"),
        ),
        F.when(
            old_origin.isNull() | new_origin.isNull() | old_dest.isNull() | new_dest.isNull(),
            F.lit("MISSING_BALANCE"),
        ),
        F.when(
            F.col("isFraud").isNull()
            | (~F.col("isFraud").isin(0, 1))
            | F.col("isFlaggedFraud").isNull()
            | (~F.col("isFlaggedFraud").isin(0, 1)),
            F.lit("INVALID_LABEL"),
        ),
        F.when(
            F.col("event_time").isNull(),
            F.lit("INVALID_EVENT_TIME"),
        ),
    )

    return checked.withColumn(
        "quality_errors",
        F.filter(error_candidates, lambda item: item.isNotNull()),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-date",
        help="Chỉ xử lý event_date này (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--batch-id",
        help="Chỉ xử lý các transaction lần đầu nạp trong batch này",
    )
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=Path("data/lakehouse"),
    )
    args = parser.parse_args()
    try:
        input_date = date.fromisoformat(args.input_date) if args.input_date else None
    except ValueError as error:
        raise ValueError("--input-date phải có dạng YYYY-MM-DD") from error

    started = perf_counter()
    spark = build_spark("fraud-silver-batch", args.warehouse)

    try:
        if not spark.catalog.tableExists("lake.bronze.transactions"):
            raise RuntimeError("Chưa có Bronze; hãy chạy bronze_ingestion.py trước")

        bronze = spark.table("lake.bronze.transactions")
        selected = bronze
        if input_date is not None:
            selected = selected.filter(F.col("event_date") == F.lit(input_date))
        if args.batch_id is not None:
            selected = selected.filter(F.col("batch_id") == args.batch_id)
        # Bronze đã unique theo transaction_id; không shuffle 6,36 triệu dòng lần nữa.
        # Cache xuống disk để valid và quarantine không cùng đọc/transform Bronze hai lần.
        checked = add_quality_checks(selected).persist(StorageLevel.DISK_ONLY)
        try:
            valid = checked.filter(F.size("quality_errors") == 0).drop(
                "quality_errors", "_corrupt_record"
            )
            quarantine = checked.filter(F.size("quality_errors") > 0)

            selected_count = selected.count()
            valid_input_count = valid.count()
            quarantine_input_count = quarantine.count()
            if selected_count != valid_input_count + quarantine_input_count:
                raise AssertionError("Selected Bronze không khớp valid + quarantine")

            insert_new_transactions(
                spark,
                valid if valid_input_count else valid.limit(0),
                "lake.silver.transactions_valid",
                "silver_valid_stage",
            )

            # Với PaySim sạch, tránh MERGE toàn bộ Bronze để ghi 0 row quarantine.
            insert_new_transactions(
                spark,
                quarantine if quarantine_input_count else quarantine.limit(0),
                "lake.silver.transactions_quarantine",
                "silver_quarantine_stage",
            )
        finally:
            checked.unpersist()

        bronze_count = bronze.count()
        valid_count = spark.table("lake.silver.transactions_valid").count()
        quarantine_count = spark.table("lake.silver.transactions_quarantine").count()

        print(
            json.dumps(
                {
                    "layer": "silver",
                    "bronze_rows": bronze_count,
                    "selected_bronze_rows": selected_count,
                    "selected_valid_rows": valid_input_count,
                    "selected_quarantine_rows": quarantine_input_count,
                    "input_date": input_date.isoformat() if input_date else None,
                    "batch_id": args.batch_id,
                    "valid_rows": valid_count,
                    "quarantine_rows": quarantine_count,
                    "reconciled": bronze_count == valid_count + quarantine_count,
                    "elapsed_seconds": round(perf_counter() - started, 2),
                },
                indent=2,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
