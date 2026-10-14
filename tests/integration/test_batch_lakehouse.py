import hashlib
from pathlib import Path

from pyspark.sql import functions as F

from spark.bronze_ingestion import add_bronze_columns, read_input, simulation_epoch
from spark.lakehouse_common import build_spark, insert_new_transactions
from spark.silver_transform import add_quality_checks


def test_csv_to_iceberg_with_quarantine_and_retry(tmp_path: Path) -> None:
    fixture = Path("tests/fixtures/paysim_quality.csv")
    source_sha256 = hashlib.sha256(fixture.read_bytes()).hexdigest()
    spark = build_spark("week2-integration-test", tmp_path / "warehouse")

    try:
        source = read_input(spark, fixture, "csv")
        bronze = add_bronze_columns(
            source,
            source_sha256,
            "fixture-batch",
            simulation_epoch("2025-01-01T00:00:00+00:00"),
        ).dropDuplicates(["transaction_id"])

        assert bronze.count() == 4
        assert bronze.filter(F.col("source_file") == "").count() == 0

        for _ in range(2):
            insert_new_transactions(
                spark,
                bronze,
                "lake.bronze.transactions",
                "bronze_fixture_stage",
            )
        assert spark.table("lake.bronze.transactions").count() == 4

        checked = add_quality_checks(spark.table("lake.bronze.transactions"))
        valid = checked.filter(F.size("quality_errors") == 0).drop(
            "quality_errors", "_corrupt_record"
        )
        quarantine = checked.filter(F.size("quality_errors") > 0)

        for _ in range(2):
            insert_new_transactions(
                spark,
                valid,
                "lake.silver.transactions_valid",
                "valid_fixture_stage",
            )
            insert_new_transactions(
                spark,
                quarantine,
                "lake.silver.transactions_quarantine",
                "quarantine_fixture_stage",
            )

        assert spark.table("lake.silver.transactions_valid").count() == 2
        assert spark.table("lake.silver.transactions_quarantine").count() == 2

        reason_rows = (
            spark.table("lake.silver.transactions_quarantine")
            .select("nameOrig", "quality_errors")
            .collect()
        )
        reasons = {row["nameOrig"]: row["quality_errors"] for row in reason_rows}
        assert "INVALID_AMOUNT" in reasons["C_NEGATIVE"]
        assert "CSV_PARSE_ERROR" in reasons["C_BAD_CSV"]

        mismatch_row = (
            spark.table("lake.silver.transactions_valid")
            .filter(F.col("nameOrig") == "C_MISMATCH")
            .first()
        )
        assert mismatch_row["origin_balance_mismatch"] is True
    finally:
        spark.stop()
