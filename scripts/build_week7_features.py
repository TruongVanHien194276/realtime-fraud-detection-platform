from pathlib import Path

import duckdb

SOURCE = Path("data/samples/representative_1m.parquet")
OUTPUT = Path("data/ml/week7/features.parquet")
TEMP = Path("data/tmp/week7_duckdb")


def sql_path(path: Path) -> str:
    #  Escape dấu nháy đơn trong đường dẫn trước khi ghép vào DuckDB SQL
    return "'" + str(path.resolve()).replace("'", "''") + "'"


def main() -> None:
    if not SOURCE.is_file():
        raise FileNotFoundError(f"Thiếu dữ liệu: {SOURCE}")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    TEMP.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("SET threads = 2")
    con.execute("SET memory_limit = '768MB'")
    con.execute(f"SET temp_directory = {sql_path(TEMP)}")

    # RANGE ... 1 PRECEDING chỉ lấy các step trước step hiện tại
    # Như vậy giao dịch cùng một giờ không được nhìn thấy nhau
    con.execute(
        f"""
        COPY (
            WITH history AS (
                SELECT
                    step,
                    type AS transaction_type,
                    amount,
                    nameOrig,
                    nameDest,
                    oldbalanceOrg,
                    isFraud,

                    COUNT(*) OVER (
                        PARTITION BY nameOrig
                        ORDER BY step
                        RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
                    ) AS customer_count_24h,

                    COUNT(*) OVER (
                        PARTITION BY nameDest
                        ORDER BY step
                        RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
                    ) AS recipient_count_24h,

                    COUNT(*) OVER (
                        PARTITION BY nameOrig, nameDest
                        ORDER BY step
                        RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
                    ) AS pair_count_24h,

                    AVG(amount) OVER (
                        PARTITION BY nameOrig
                        ORDER BY step
                        RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
                    ) AS customer_avg_amount_24h

                FROM read_parquet({sql_path(SOURCE)})
                WHERE step <= 408
            )
            SELECT
                -- Chỉ dùng để audit/tie-break, không đưa ID vào model.
                hash(step, nameOrig, nameDest, amount)::VARCHAR
                    AS transaction_id,
                step::INTEGER AS step,
                ((step - 1) // 24)::INTEGER AS event_day,
                (step % 24)::INTEGER AS event_hour,
                transaction_type,
                amount::DOUBLE AS amount,
                oldbalanceOrg::DOUBLE AS old_balance_origin,
                customer_count_24h::INTEGER AS customer_count_24h,
                recipient_count_24h::INTEGER AS recipient_count_24h,
                pair_count_24h::INTEGER AS pair_count_24h,

                CASE
                    WHEN oldbalanceOrg > 0
                    THEN (amount / oldbalanceOrg)::DOUBLE
                    ELSE NULL
                END AS amount_to_balance_ratio,

                CASE
                    WHEN customer_avg_amount_24h > 0
                    THEN (amount / customer_avg_amount_24h)::DOUBLE
                    ELSE NULL
                END AS amount_over_avg_24h,

                isFraud::INTEGER AS label
            FROM history
            ORDER BY step, transaction_id
        )
        TO {sql_path(OUTPUT)}
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    row = con.execute(
        f"""
        SELECT COUNT(*), SUM(label), MIN(step), MAX(step)
        FROM read_parquet({sql_path(OUTPUT)})
        """
    ).fetchone()
    con.close()

    print(f"Output: {OUTPUT}")
    print(f"Rows={row[0]:,}, fraud={row[1]:,}, step_min={row[2]}, step_max={row[3]}")


if __name__ == "__main__":
    main()
