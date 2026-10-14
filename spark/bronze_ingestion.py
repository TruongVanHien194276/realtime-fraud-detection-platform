import argparse
import csv
import json
from datetime import date, datetime
from pathlib import Path
from time import perf_counter

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark.lakehouse_common import build_spark, insert_new_transactions

RAW_COLUMNS = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
]

DECIMAL = DecimalType(20, 2)

RAW_TYPES = {
    "step": LongType(),
    "type": StringType(),
    "amount": DECIMAL,
    "nameOrig": StringType(),
    "oldbalanceOrg": DECIMAL,
    "newbalanceOrig": DECIMAL,
    "nameDest": StringType(),
    "oldbalanceDest": DECIMAL,
    "newbalanceDest": DECIMAL,
    "isFraud": IntegerType(),
    "isFlaggedFraud": IntegerType(),
}

CSV_SCHEMA = StructType(
    [StructField(name, RAW_TYPES[name], True) for name in RAW_COLUMNS]
    + [StructField("_corrupt_record", StringType(), True)]
)


def check_csv_header(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"CSV không tồn tại: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as file:
        actual = next(csv.reader(file))

    if actual != RAW_COLUMNS:
        raise ValueError(
            f"Header CSV không đúng contract.\nExpected: {RAW_COLUMNS}\nActual: {actual}"
        )


def read_input(spark, input_path: Path, input_format: str) -> DataFrame:
    if not input_path.exists():
        raise FileNotFoundError(f"Không tìm thấy input: {input_path}")

    if input_format == "csv":
        check_csv_header(input_path)

        source = (
            spark.read.schema(CSV_SCHEMA)
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .csv(str(input_path))
        )
    else:
        source = spark.read.parquet(str(input_path))
        source = source.withColumn("_corrupt_record", F.lit(None).cast("string"))

    # input_file_name() phải được lấy khi DataFrame còn gắn với file nguồn.
    source = source.withColumn("source_file", F.input_file_name())

    # CSV và Parquet đi qua cùng một schema logic trước khi tạo hash.
    return source.select(
        *[F.col(name).cast(RAW_TYPES[name]).alias(name) for name in RAW_COLUMNS],
        F.col("_corrupt_record").cast("string"),
        F.col("source_file"),
    )


def simulation_epoch(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("--simulation-origin phải có timezone, ví dụ +00:00")
    return int(parsed.timestamp())


def parse_input_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError("--input-date phải có dạng YYYY-MM-DD") from error


def add_bronze_columns(
    source: DataFrame,
    source_sha256: str,
    batch_id: str,
    origin_epoch: int,
) -> DataFrame:
    # Hash chỉ dựa trên nội dung nguồn, không dựa trên đường dẫn sample hay batch ID.
    # Như vậy cùng raw row trong sample 100k và 1m có cùng transaction_id.
    canonical_record = F.to_json(
        F.struct(*[F.col(name) for name in RAW_COLUMNS + ["_corrupt_record"]])
    )

    return (
        source.withColumn("record_hash", F.sha2(canonical_record, 256))
        .withColumn(
            "transaction_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit(source_sha256),
                    F.col("record_hash"),
                ),
                256,
            ),
        )
        # step là số giờ mô phỏng, không phải Unix timestamp thật.
        .withColumn(
            "event_time",
            F.to_timestamp(F.from_unixtime(F.lit(origin_epoch) + F.col("step") * F.lit(3600))),
        )
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("source_dataset_sha256", F.lit(source_sha256))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("loaded_at", F.current_timestamp())
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--format", required=True, choices=["csv", "parquet"])
    parser.add_argument("--batch-id", required=True)
    parser.add_argument(
        "--input-date",
        help="Chỉ nạp event_date này; dùng khi backfill từng ngày (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--source-sha256",
        help="Checksum nguồn cho fixture/test; mặc định lấy từ data/manifest.json",
    )
    parser.add_argument(
        "--simulation-origin",
        default="2025-01-01T00:00:00+00:00",
    )
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=Path("data/lakehouse"),
    )
    args = parser.parse_args()

    manifest = json.loads(Path("data/manifest.json").read_text(encoding="utf-8"))
    source_sha256 = args.source_sha256 or manifest["source"]["sha256"]
    if len(source_sha256) != 64 or any(char not in "0123456789abcdef" for char in source_sha256):
        raise ValueError("--source-sha256 phải là 64 ký tự hex viết thường")
    origin_epoch = simulation_epoch(args.simulation_origin)
    input_date = parse_input_date(args.input_date) if args.input_date else None

    started = perf_counter()
    spark = build_spark("fraud-bronze-batch", args.warehouse)

    try:
        source = read_input(spark, args.input, args.format)
        input_count = source.count()

        bronze = add_bronze_columns(
            source=source,
            source_sha256=source_sha256,
            batch_id=args.batch_id,
            origin_epoch=origin_epoch,
        )
        if input_date is not None:
            bronze = bronze.filter(F.col("event_date") == F.lit(input_date))
        bronze = bronze.dropDuplicates(["transaction_id"])

        batch_unique_count = bronze.count()

        insert_new_transactions(
            spark,
            bronze,
            "lake.bronze.transactions",
            "bronze_batch_stage",
        )

        total_count = spark.table("lake.bronze.transactions").count()

        print(
            json.dumps(
                {
                    "layer": "bronze",
                    "input_rows": input_count,
                    "input_date": input_date.isoformat() if input_date else None,
                    "unique_rows_in_batch": batch_unique_count,
                    "table_rows_after_merge": total_count,
                    "elapsed_seconds": round(perf_counter() - started, 2),
                },
                indent=2,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
