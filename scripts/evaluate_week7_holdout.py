"""Build and score a fresh, row-disjoint PaySim temporal holdout once."""

import json
from pathlib import Path

import duckdb
import joblib
import mlflow
import pandas as pd
from train_week7 import (
    MODEL_FEATURES,
    OUTPUT,
    offline_rule_scores,
    prepare,
    threshold_metrics,
)
from train_week7 import metrics as capacity_metrics

RAW = Path("data/raw/PS_20174392719_1491204439457_log.csv")
SAMPLE = Path("data/samples/representative_1m.parquet")
SAMPLE_FEATURES = Path("data/ml/week7/features.parquet")
HOLDOUT = Path("data/ml/week7/fresh_holdout.parquet")
TEMP = Path("data/tmp/week7_holdout_duckdb")
HOLDOUT_SIZE = 100_000


def sql_path(path: Path) -> str:
    return "'" + str(path.resolve()).replace("'", "''") + "'"


def build_holdout() -> None:
    for path in (RAW, SAMPLE, SAMPLE_FEATURES):
        if not path.is_file():
            raise FileNotFoundError(path)
    HOLDOUT.parent.mkdir(parents=True, exist_ok=True)
    TEMP.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("SET threads = 2")
    con.execute("SET memory_limit = '768MB'")
    con.execute(f"SET temp_directory = {sql_path(TEMP)}")
    con.execute(
        f"""
        COPY (
            WITH candidates AS (
                SELECT
                    step::BIGINT AS step,
                    type,
                    amount::DOUBLE AS amount,
                    nameOrig,
                    nameDest,
                    oldbalanceOrg::DOUBLE AS oldbalanceOrg,
                    isFraud::INTEGER AS isFraud,
                    hash(step::BIGINT, nameOrig, nameDest, amount::DOUBLE)::VARCHAR
                        AS transaction_id
                FROM read_csv({sql_path(RAW)}, header = true, sample_size = 100000)
                WHERE step BETWEEN 337 AND 408
            ),
            unseen AS (
                SELECT c.*
                FROM candidates AS c
                ANTI JOIN read_parquet({sql_path(SAMPLE_FEATURES)}) AS used
                    ON c.transaction_id = used.transaction_id
                ORDER BY hash(c.step, c.nameOrig, c.nameDest, c.amount, 20261015)
                LIMIT {HOLDOUT_SIZE}
            ),
            events AS (
                SELECT
                    step, type, amount, nameOrig, nameDest, oldbalanceOrg,
                    isFraud,
                    hash(step, nameOrig, nameDest, amount)::VARCHAR AS transaction_id,
                    false AS is_holdout
                FROM read_parquet({sql_path(SAMPLE)})
                WHERE step <= 408
                UNION ALL
                SELECT
                    step, type, amount, nameOrig, nameDest, oldbalanceOrg,
                    isFraud, transaction_id, true AS is_holdout
                FROM unseen
            ),
            history AS (
                SELECT *,
                    COUNT(*) OVER (
                        PARTITION BY nameOrig ORDER BY step
                        RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
                    ) AS customer_count_24h,
                    COUNT(*) OVER (
                        PARTITION BY nameDest ORDER BY step
                        RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
                    ) AS recipient_count_24h,
                    COUNT(*) OVER (
                        PARTITION BY nameOrig, nameDest ORDER BY step
                        RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
                    ) AS pair_count_24h,
                    AVG(amount) OVER (
                        PARTITION BY nameOrig ORDER BY step
                        RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
                    ) AS customer_avg_amount_24h
                FROM events
            )
            SELECT
                transaction_id,
                step::INTEGER AS step,
                ((step - 1) // 24)::INTEGER AS event_day,
                (step % 24)::INTEGER AS event_hour,
                type AS transaction_type,
                amount,
                oldbalanceOrg AS old_balance_origin,
                customer_count_24h::INTEGER AS customer_count_24h,
                recipient_count_24h::INTEGER AS recipient_count_24h,
                pair_count_24h::INTEGER AS pair_count_24h,
                CASE WHEN oldbalanceOrg > 0
                    THEN amount / oldbalanceOrg ELSE NULL END AS amount_to_balance_ratio,
                CASE WHEN customer_avg_amount_24h > 0
                    THEN amount / customer_avg_amount_24h ELSE NULL END
                    AS amount_over_avg_24h,
                isFraud AS label
            FROM history
            WHERE is_holdout
            ORDER BY step, transaction_id
        ) TO {sql_path(HOLDOUT)} (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    count, fraud, overlap = con.execute(
        f"""
        SELECT COUNT(*), SUM(h.label),
               COUNT(used.transaction_id)
        FROM read_parquet({sql_path(HOLDOUT)}) AS h
        LEFT JOIN read_parquet({sql_path(SAMPLE_FEATURES)}) AS used
            ON h.transaction_id = used.transaction_id
        """
    ).fetchone()
    con.close()
    if count != HOLDOUT_SIZE or overlap != 0 or fraud < 30:
        raise ValueError(f"Fresh holdout invalid: rows={count}, fraud={fraud}, overlap={overlap}")
    print(f"Fresh holdout: rows={count:,}, fraud={fraud}, sample_overlap={overlap}")


def evaluate_holdout() -> None:
    report_path = OUTPUT / "report.json"
    model_path = OUTPUT / "champion.joblib"
    if not report_path.is_file() or not model_path.is_file():
        raise FileNotFoundError("Run train_week7.py before the fresh holdout")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    model = joblib.load(model_path)  # Our own artifact, not untrusted input.
    frame = prepare(pd.read_parquet(HOLDOUT)).reset_index(drop=True)
    scores = model.predict_proba(frame[MODEL_FEATURES])[:, 1]
    threshold = report["validation_models"][report["champion"]]["fixed_fpr"]["threshold"]
    result = {
        "rows": len(frame),
        "fraud": int(frame["label"].sum()),
        "sample_overlap": 0,
        "model_capacity": capacity_metrics(frame, scores),
        "model_fixed_fpr": threshold_metrics(frame, scores, threshold),
        "offline_three_rule_capacity": capacity_metrics(frame, offline_rule_scores(frame)),
        "caveat": (
            "Row-disjoint later-time holdout; histories include sample plus holdout rows, "
            "so history coverage differs from model training. Same PaySim simulator."
        ),
    }
    output = OUTPUT / "fresh_holdout_report.json"
    output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    mlflow.set_tracking_uri(f"sqlite:///{Path('data/ml/week7/mlflow.db').resolve()}")
    mlflow.set_experiment("fraud-week7-temporal")
    with mlflow.start_run(run_name=f"{report['champion']}-fresh-row-disjoint-holdout"):
        mlflow.log_params(
            {
                "champion": report["champion"],
                "cohort": "raw_csv_rows_excluding_representative_1m",
                "temporal_steps": "337-408",
                "size": HOLDOUT_SIZE,
                "seed": 20261015,
            }
        )
        mlflow.log_metrics(
            {f"holdout_{key}": value for key, value in result["model_capacity"].items()}
        )
        mlflow.log_artifact(str(output))
    print(json.dumps(result, indent=2))


def main() -> None:
    output = OUTPUT / "fresh_holdout_report.json"
    if output.exists():
        raise FileExistsError(
            f"{output} already exists; do not repeatedly inspect a supposedly untouched holdout"
        )
    build_holdout()
    evaluate_holdout()


if __name__ == "__main__":
    main()
