import csv
from pathlib import Path

import pytest

from fraud_platform.contracts import RAW_COLUMNS
from scripts.create_samples import create_samples


def make_dataset(path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=RAW_COLUMNS)
        writer.writeheader()
        for index in range(30):
            writer.writerow(
                {
                    "step": index + 1,
                    "type": "TRANSFER" if index < 5 else "PAYMENT",
                    "amount": float(index + 1),
                    "nameOrig": f"C{index}",
                    "oldbalanceOrg": 100,
                    "newbalanceOrig": 99,
                    "nameDest": f"M{index}",
                    "oldbalanceDest": 0,
                    "newbalanceDest": 1,
                    "isFraud": int(index < 5),
                    "isFlaggedFraud": 0,
                }
            )


def test_samples_are_reproducible_and_scenario_is_separate(tmp_path: Path) -> None:
    input_path = tmp_path / "tiny.csv"
    make_dataset(input_path)

    options = {
        "input_path": input_path,
        "seed": 42,
        "integration_size": 20,
        "development_size": 10,
        "unit_size": 4,
        "scenario_fraud_size": 2,
        "scenario_legitimate_size": 6,
    }
    first = create_samples(output_directory=tmp_path / "first", **options)
    second = create_samples(output_directory=tmp_path / "second", **options)

    assert [item["row_count"] for item in first["files"]] == [4, 10, 20, 8]
    assert first["files"][-1]["fraud_count"] == 2
    assert [item["sha256"] for item in first["files"]] == [
        item["sha256"] for item in second["files"]
    ]


def test_sample_input_must_be_csv_file(tmp_path: Path) -> None:
    with pytest.raises(IsADirectoryError):
        create_samples(
            input_path=tmp_path,
            output_directory=tmp_path / "samples",
            seed=42,
            integration_size=20,
            development_size=10,
            unit_size=4,
        )
