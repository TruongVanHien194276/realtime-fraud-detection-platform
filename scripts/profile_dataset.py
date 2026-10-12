import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from fraud_platform.contracts import RAW_COLUMNS


def sql_string(value: str | Path) -> str:
    """Escape a value used as a DuckDB SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file:
        while chunk := file.read(chunk_size):
            digest.update(chunk)

    return digest.hexdigest()


def rows_to_dicts(
    columns: list[str],
    rows: list[tuple[Any, ...]],
) -> list[dict[str, Any]]:
    return [dict(zip(columns, row, strict=True)) for row in rows]


def profile_dataset(input_path: Path) -> dict[str, Any]:
    if not input_path.exists():
        raise FileNotFoundError(f"Dataset không tồn tại: {input_path}")
    if not input_path.is_file():
        raise IsADirectoryError(f"Đường dẫn dataset phải là file CSV: {input_path.resolve()}")
    if input_path.suffix.lower() != ".csv":
        raise ValueError(f"Dataset phải là file CSV: {input_path}")

    temp_directory = Path("data/tmp/duckdb")
    temp_directory.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect()
    connection.execute("SET threads = 2")
    connection.execute("SET memory_limit = '768MB'")
    connection.execute(f"SET temp_directory = {sql_string(temp_directory.resolve())}")

    source = (
        f"read_csv({sql_string(input_path.resolve())}, "
        "header = true, auto_detect = true, sample_size = -1)"
    )

    schema_rows = connection.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()

    actual_columns = [row[0] for row in schema_rows]

    missing_columns = sorted(set(RAW_COLUMNS) - set(actual_columns))
    unexpected_columns = sorted(set(actual_columns) - set(RAW_COLUMNS))

    if missing_columns or unexpected_columns:
        raise ValueError(
            f"Schema không hợp lệ. Missing={missing_columns}, unexpected={unexpected_columns}"
        )

    summary_row = connection.execute(
        f"""
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
            SUM(
                CASE WHEN isFlaggedFraud = 1 THEN 1 ELSE 0 END
            ) AS flagged_fraud_count,
            MIN(step) AS min_step,
            MAX(step) AS max_step,
            MIN(amount) AS min_amount,
            AVG(amount) AS avg_amount,
            MAX(amount) AS max_amount,
            COUNT(*) - COUNT(
                DISTINCT hash(
                    step,
                    type,
                    amount,
                    nameOrig,
                    oldbalanceOrg,
                    newbalanceOrig,
                    nameDest,
                    oldbalanceDest,
                    newbalanceDest,
                    isFraud,
                    isFlaggedFraud
                )
            ) AS duplicate_row_count
        FROM {source}
        """
    ).fetchone()

    summary_columns = [
        "row_count",
        "fraud_count",
        "flagged_fraud_count",
        "min_step",
        "max_step",
        "min_amount",
        "avg_amount",
        "max_amount",
        "duplicate_row_count",
    ]
    summary = dict(zip(summary_columns, summary_row, strict=True))

    row_count = int(summary["row_count"])
    fraud_count = int(summary["fraud_count"])

    summary["fraud_rate"] = fraud_count / row_count if row_count else 0.0

    type_cursor = connection.execute(
        f"""
        SELECT
            type AS transaction_type,
            COUNT(*) AS transaction_count,
            SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
            AVG(amount) AS average_amount
        FROM {source}
        GROUP BY type
        ORDER BY transaction_count DESC
        """
    )

    type_columns = [item[0] for item in type_cursor.description]
    transaction_types = rows_to_dicts(
        type_columns,
        type_cursor.fetchall(),
    )

    null_expressions = [
        f'SUM(CASE WHEN "{column}" IS NULL THEN 1 ELSE 0 END) AS "{column}"'
        for column in RAW_COLUMNS
    ]

    null_row = connection.execute(
        f"""
        SELECT {", ".join(null_expressions)}
        FROM {source}
        """
    ).fetchone()

    null_counts = dict(zip(RAW_COLUMNS, null_row, strict=True))

    quality_row = connection.execute(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE amount < 0) AS negative_amount_count,
            COUNT(*) FILTER (WHERE step < 0) AS negative_step_count,
            COUNT(*) FILTER (WHERE isFraud NOT IN (0, 1)) AS invalid_fraud_label_count,
            COUNT(*) FILTER (WHERE isFlaggedFraud NOT IN (0, 1)) AS invalid_flag_count,
            COUNT(*) FILTER (
                WHERE type NOT IN ('CASH_IN', 'CASH_OUT', 'DEBIT', 'PAYMENT', 'TRANSFER')
            ) AS invalid_type_count,
            approx_count_distinct(nameOrig) AS approx_origin_cardinality,
            approx_count_distinct(nameDest) AS approx_destination_cardinality,
            approx_quantile(amount, 0.5) AS approx_median_amount,
            approx_quantile(amount, 0.95) AS approx_p95_amount
        FROM {source}
        """
    ).fetchone()
    quality_columns = [
        "negative_amount_count",
        "negative_step_count",
        "invalid_fraud_label_count",
        "invalid_flag_count",
        "invalid_type_count",
        "approx_origin_cardinality",
        "approx_destination_cardinality",
        "approx_median_amount",
        "approx_p95_amount",
    ]
    quality = dict(zip(quality_columns, quality_row, strict=True))

    schema = [
        {
            "name": row[0],
            "duckdb_type": row[1],
            "nullable": row[2] == "YES",
        }
        for row in schema_rows
    ]

    connection.close()

    return {
        "manifest_version": "1.0",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source": {
            "file_name": input_path.name,
            "file_size_bytes": input_path.stat().st_size,
            "sha256": sha256_file(input_path),
            "downloaded_at_utc": None,
            "download_date_status": "not_recorded",
            "source_url": ("https://www.kaggle.com/datasets/ealaxi/paysim1"),
        },
        "schema": schema,
        "summary": summary,
        "null_counts": null_counts,
        "quality": quality,
        "transaction_types": transaction_types,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/manifest.json"),
    )
    arguments = parser.parse_args()

    result = profile_dataset(arguments.input)

    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(result, indent=2, default=str),
        encoding="utf-8",
    )

    print(f"Manifest written to: {arguments.output}")
    print(f"Rows: {result['summary']['row_count']:,}")
    print(f"Fraud rows: {result['summary']['fraud_count']:,}")
    print(f"Fraud rate: {result['summary']['fraud_rate']:.6%}")
    print(f"SHA-256: {result['source']['sha256']}")


if __name__ == "__main__":
    main()
