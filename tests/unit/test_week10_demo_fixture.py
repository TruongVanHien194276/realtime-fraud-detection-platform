import csv
import sys

from fraud_platform.contracts import RAW_COLUMNS
from scripts.week10_demo_fixture import main


def test_synthetic_fixture_matches_raw_contract(tmp_path, monkeypatch):
    output = tmp_path / "demo.csv"
    monkeypatch.setattr(
        sys,
        "argv",
        ["week10_demo_fixture", "--output", str(output), "--rows", "10"],
    )
    main()

    with output.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        rows = list(reader)
        assert reader.fieldnames == RAW_COLUMNS

    assert len(rows) == 10
    assert len({row["nameOrig"] for row in rows}) == 10
    assert rows[0]["isFraud"] == "1"
