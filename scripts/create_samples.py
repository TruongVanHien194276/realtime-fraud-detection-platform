import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb

from fraud_platform.contracts import RAW_COLUMNS


def sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file:
        while chunk := file.read(chunk_size):
            digest.update(chunk)

    return digest.hexdigest()


def inspect_parquet(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
) -> dict[str, Any]:
    row = connection.execute(
        f"""
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_count
        FROM read_parquet({sql_string(path.resolve())})
        """
    ).fetchone()

    row_count = int(row[0])
    fraud_count = int(row[1])

    return {
        "file_name": path.name,
        "row_count": row_count,
        "fraud_count": fraud_count,
        "fraud_rate": fraud_count / row_count if row_count else 0.0,
        "file_size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def create_samples(
    input_path: Path,
    output_directory: Path,
    seed: int,
    integration_size: int,
    development_size: int,
    unit_size: int,
    scenario_fraud_size: int = 1_000,
    scenario_legitimate_size: int = 9_000,
) -> dict[str, Any]:
    if not input_path.exists():
        raise FileNotFoundError(f"Không tìm thấy dataset: {input_path}")
    if not input_path.is_file():
        raise IsADirectoryError(f"Đường dẫn dataset phải là file CSV: {input_path.resolve()}")
    if input_path.suffix.lower() != ".csv":
        raise ValueError(f"Dataset phải là file CSV: {input_path}")

    if not 0 < unit_size <= development_size <= integration_size:
        raise ValueError("Cần unit_size <= development_size <= integration_size")
    if scenario_fraud_size < 1 or scenario_legitimate_size < 1:
        raise ValueError("Scenario cần ít nhất một fraud và một giao dịch hợp lệ")

    output_directory.mkdir(parents=True, exist_ok=True)

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

    actual_columns = [
        row[0] for row in connection.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
    ]
    if actual_columns != RAW_COLUMNS:
        raise ValueError(f"Schema không đúng thứ tự cột PaySim: {actual_columns}")

    fraud_count, legitimate_count = connection.execute(
        f"""SELECT
            COUNT(*) FILTER (WHERE isFraud = 1),
            COUNT(*) FILTER (WHERE isFraud = 0)
        FROM {source}"""
    ).fetchone()
    if fraud_count < scenario_fraud_size or legitimate_count < scenario_legitimate_size:
        raise ValueError("Dataset không đủ dòng để tạo fraud scenario sample")
    if fraud_count + legitimate_count < integration_size:
        raise ValueError("Dataset không đủ dòng để tạo representative sample lớn nhất")

    order_expression = f"""
        hash(
            step,
            type,
            amount,
            nameOrig,
            oldbalanceOrg,
            newbalanceOrig,
            nameDest,
            oldbalanceDest,
            newbalanceDest,
            {seed}
        )
    """

    sizes = {
        "representative_10k": unit_size,
        "representative_100k": development_size,
        "representative_1m": integration_size,
    }

    generated_files: list[dict[str, Any]] = []

    for sample_name, sample_size in sizes.items():
        output_path = output_directory / f"{sample_name}.parquet"

        connection.execute(
            f"""
            COPY (
                SELECT *
                FROM {source}
                ORDER BY {order_expression}
                LIMIT {sample_size}
            )
            TO {sql_string(output_path.resolve())}
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD
            )
            """
        )

        generated_files.append(inspect_parquet(connection, output_path))

    scenario_path = output_directory / "fraud_scenario_10k.parquet"

    connection.execute(
        f"""
        COPY (
            WITH fraud_rows AS (
                SELECT *
                FROM {source}
                WHERE isFraud = 1
                ORDER BY {order_expression}
                LIMIT {scenario_fraud_size}
            ),
            legitimate_rows AS (
                SELECT *
                FROM {source}
                WHERE isFraud = 0
                ORDER BY {order_expression}
                LIMIT {scenario_legitimate_size}
            )
            SELECT * FROM fraud_rows
            UNION ALL
            SELECT * FROM legitimate_rows
        )
        TO {sql_string(scenario_path.resolve())}
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
        """
    )

    generated_files.append(inspect_parquet(connection, scenario_path))

    connection.close()

    return {
        "sample_manifest_version": "1.0",
        "seed": seed,
        "source_file": input_path.name,
        "scenario_fraud_size": scenario_fraud_size,
        "scenario_legitimate_size": scenario_legitimate_size,
        "files": generated_files,
        "warning": (
            "fraud_scenario_10k is for pipeline demonstrations only. "
            "Do not use it to report model performance."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=Path("data/samples"),
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--integration-size",
        type=int,
        default=1_000_000,
    )
    parser.add_argument(
        "--development-size",
        type=int,
        default=100_000,
    )
    parser.add_argument(
        "--unit-size",
        type=int,
        default=10_000,
    )
    arguments = parser.parse_args()

    result = create_samples(
        input_path=arguments.input,
        output_directory=arguments.output_directory,
        seed=arguments.seed,
        integration_size=arguments.integration_size,
        development_size=arguments.development_size,
        unit_size=arguments.unit_size,
    )

    manifest_path = arguments.output_directory / "sample_manifest.json"
    manifest_path.write_text(
        json.dumps(result, indent=2),
        encoding="utf-8",
    )

    print(f"Sample manifest: {manifest_path}")

    for item in result["files"]:
        print(
            f"{item['file_name']}: "
            f"{item['row_count']:,} rows, "
            f"{item['fraud_count']:,} fraud, "
            f"{item['fraud_rate']:.4%}"
        )


if __name__ == "__main__":
    main()
