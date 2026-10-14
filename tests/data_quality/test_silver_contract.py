from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.silver_transform import add_quality_checks


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("silver-quality-test")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def make_row(
    name_orig: str,
    amount: str,
    new_origin: str,
) -> dict:
    return {
        "step": 1,
        "type": "TRANSFER",
        "amount": Decimal(amount),
        "nameOrig": name_orig,
        "oldbalanceOrg": Decimal("100.00"),
        "newbalanceOrig": Decimal(new_origin),
        "nameDest": "C_DEST",
        "oldbalanceDest": Decimal("0.00"),
        "newbalanceDest": Decimal("10.00"),
        "isFraud": 0,
        "isFlaggedFraud": 0,
        "event_time": datetime(2025, 1, 1, 1, tzinfo=UTC),
    }


def test_negative_amount_goes_to_quarantine(spark):
    frame = spark.createDataFrame(
        [
            make_row("C_VALID", "10.00", "90.00"),
            make_row("C_NEGATIVE", "-10.00", "110.00"),
        ]
    ).withColumn("_corrupt_record", F.lit(None).cast("string"))

    rows = {row["nameOrig"]: row for row in add_quality_checks(frame).collect()}

    assert rows["C_VALID"]["quality_errors"] == []
    assert "INVALID_AMOUNT" in rows["C_NEGATIVE"]["quality_errors"]


def test_balance_mismatch_is_flag_not_quarantine(spark):
    frame = spark.createDataFrame([make_row("C_MISMATCH", "10.00", "999.00")]).withColumn(
        "_corrupt_record", F.lit(None).cast("string")
    )

    row = add_quality_checks(frame).first()

    assert row["origin_balance_mismatch"] is True
    assert row["quality_errors"] == []
