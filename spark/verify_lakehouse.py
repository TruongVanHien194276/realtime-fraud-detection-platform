import argparse
from pathlib import Path

from pyspark.sql import functions as F

from spark.lakehouse_common import build_spark


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=Path("data/lakehouse"),
    )
    args = parser.parse_args()

    spark = build_spark("fraud-lakehouse-verify", args.warehouse)

    try:
        bronze = spark.table("lake.bronze.transactions")
        valid = spark.table("lake.silver.transactions_valid")
        quarantine = spark.table("lake.silver.transactions_quarantine")

        bronze_count = bronze.count()
        valid_count = valid.count()
        quarantine_count = quarantine.count()

        duplicate_count = (
            bronze.groupBy("transaction_id").count().filter(F.col("count") > 1).count()
        )

        overlap_count = (
            valid.select("transaction_id")
            .join(
                quarantine.select("transaction_id"),
                on="transaction_id",
                how="inner",
            )
            .count()
        )

        print(f"Bronze: {bronze_count}")
        print(f"Silver valid: {valid_count}")
        print(f"Silver quarantine: {quarantine_count}")
        print(f"Duplicate IDs in Bronze: {duplicate_count}")
        print(f"Valid/quarantine overlap: {overlap_count}")

        if quarantine_count:
            print("Quarantine reason codes:")
            (
                quarantine.select(F.explode("quality_errors").alias("reason_code"))
                .groupBy("reason_code")
                .count()
                .orderBy(F.desc("count"))
                .show(truncate=False)
            )

        assert bronze_count == valid_count + quarantine_count
        assert duplicate_count == 0
        assert overlap_count == 0
        print("RECONCILIATION PASSED")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
