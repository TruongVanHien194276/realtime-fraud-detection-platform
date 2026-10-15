"""Retrain a validation-only candidate without touching the Week 7 champion/test."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import joblib
import mlflow
import pandas as pd
from sklearn.linear_model import LogisticRegression
from train_week7 import MODEL_FEATURES, make_pipeline, metrics, prepare

INPUT = Path("data/ml/week7/features.parquet")
OUTPUT = Path("artifacts/week9")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    if not INPUT.is_file():
        raise FileNotFoundError(INPUT)
    data = prepare(pd.read_parquet(INPUT))
    train = data.loc[data["step"].between(1, 216)]
    validation = data.loc[data["step"].between(241, 312)]
    if train.empty or validation.empty or int(validation["label"].sum()) < 30:
        raise ValueError("Training/validation split is empty or has too few fraud labels")
    if train["step"].max() >= validation["step"].min():
        raise ValueError("Temporal leakage between train and validation")

    model = make_pipeline(
        LogisticRegression(class_weight="balanced", max_iter=400, random_state=42)
    )
    model.fit(train[MODEL_FEATURES], train["label"])
    scores = model.predict_proba(validation[MODEL_FEATURES])[:, 1]
    evaluation = metrics(validation, scores)

    OUTPUT.mkdir(parents=True, exist_ok=True)
    report = {
        "trained_at_utc": datetime.now(UTC).isoformat(),
        "feature_sha256": sha256(INPUT),
        "model": "logistic_safe_week9_candidate",
        "features": MODEL_FEATURES,
        "train_rows": len(train),
        "validation_rows": len(validation),
        "train_steps": [1, 216],
        "validation_steps": [241, 312],
        "validation": evaluation,
        "test_evaluated": False,
        "source_note": (
            "Fixed PaySim synthetic cohort; refresh features and labels "
            "before production retraining."
        ),
    }
    report_path = OUTPUT / "candidate_report.json"
    model_path = OUTPUT / "candidate.joblib"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    joblib.dump(model, model_path)

    mlflow.set_tracking_uri(f"sqlite:///{Path('data/ml/week7/mlflow.db').resolve()}")
    experiment = "fraud-week9-retraining-demo"
    if mlflow.get_experiment_by_name(experiment) is None:
        mlflow.create_experiment(
            experiment,
            artifact_location=(OUTPUT.resolve() / "mlflow-artifacts").as_uri(),
        )
    mlflow.set_experiment(experiment)
    with mlflow.start_run(run_name=f"candidate-{report['feature_sha256'][:12]}"):
        mlflow.log_params(
            {
                "model": report["model"],
                "feature_sha256": report["feature_sha256"],
                "train_steps": "1-216",
                "validation_steps": "241-312",
                "test_evaluated": False,
            }
        )
        mlflow.log_metrics({f"validation_{key}": value for key, value in evaluation.items()})
        mlflow.log_artifact(str(report_path))
        mlflow.log_artifact(str(model_path))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
