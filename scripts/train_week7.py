import hashlib
import json
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import PrecisionRecallDisplay, average_precision_score, roc_curve
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

INPUT = Path("data/ml/week7/features.parquet")
SOURCE = Path("data/samples/representative_1m.parquet")
OUTPUT = Path("artifacts/week7")
CAPACITY_PER_DAY = 100
REVIEW_COST = 5.0
TARGET_FPR = 0.001

SAFE_NUMERIC_FEATURES = [
    "log_amount",
    "customer_count_24h",
    "recipient_count_24h",
    "pair_count_24h",
    "event_hour",
]
CATEGORICAL_FEATURES = ["transaction_type"]
MODEL_FEATURES = SAFE_NUMERIC_FEATURES + CATEGORICAL_FEATURES
FULL_FEATURES = MODEL_FEATURES + ["log_old_balance", "log_balance_ratio"]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def prepare(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()

    # log1p giảm ảnh hưởng của amount rất lớn và giữ giá trị 0 hợp lệ
    frame["log_amount"] = np.log1p(frame["amount"].clip(lower=0))
    frame["log_old_balance"] = np.log1p(frame["old_balance_origin"].clip(lower=0))
    frame["log_balance_ratio"] = np.log1p(frame["amount_to_balance_ratio"].fillna(0).clip(lower=0))
    frame["log_amount_over_avg"] = np.log1p(frame["amount_over_avg_24h"].fillna(0).clip(lower=0))
    return frame


def topk_flags(frame: pd.DataFrame, scores: np.ndarray) -> np.ndarray:
    """Tối đa 100 hồ sơ/ngày, tie-break cố định để tái lập được"""
    ranked = pd.DataFrame(
        {
            "event_day": frame["event_day"].to_numpy(),
            "score": scores,
            "amount": frame["amount"].to_numpy(),
            "transaction_id": frame["transaction_id"].to_numpy(),
        },
        index=frame.index,
    )
    selected = (
        ranked.sort_values(
            ["event_day", "score", "amount", "transaction_id"],
            ascending=[True, False, False, True],
        )
        .groupby("event_day", sort=False)
        .head(CAPACITY_PER_DAY)
        .index
    )
    return frame.index.isin(selected)


def metrics(frame: pd.DataFrame, scores: np.ndarray) -> dict:
    truth = frame["label"].to_numpy(dtype=bool)
    alert = topk_flags(frame, scores)
    amount = frame["amount"].to_numpy()

    tp = int(np.sum(alert & truth))
    fp = int(np.sum(alert & ~truth))
    fn = int(np.sum(~alert & truth))
    tn = int(np.sum(~alert & ~truth))

    total_fraud_amount = float(np.sum(amount[truth]))
    captured_amount = float(np.sum(amount[alert & truth]))

    return {
        "average_precision": float(average_precision_score(truth.astype(int), scores)),
        "precision_at_capacity": tp / max(tp + fp, 1),
        "recall_at_capacity": tp / max(tp + fn, 1),
        "fpr_at_capacity": fp / max(fp + tn, 1),
        "fraud_amount_captured_pct": (
            captured_amount / total_fraud_amount if total_fraud_amount > 0 else 0.0
        ),
        # Giả định minh họa: mỗi alert được review và fraud được bắt thì
        # toàn bộ số tiền được ngăn chặn. Chưa phải business cost thực tế.
        "illustrative_cost": float(np.sum(amount[truth & ~alert]) + REVIEW_COST * np.sum(alert)),
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "alerts": tp + fp,
    }


def threshold_at_fpr(frame: pd.DataFrame, scores: np.ndarray) -> float:
    """Chốt threshold trên validation; không tối ưu threshold trên test."""
    fpr, _, thresholds = roc_curve(frame["label"], scores, drop_intermediate=False)
    eligible = np.flatnonzero(fpr <= TARGET_FPR)
    return float(thresholds[eligible[-1]])


def threshold_metrics(frame: pd.DataFrame, scores: np.ndarray, threshold: float) -> dict:
    truth = frame["label"].to_numpy(dtype=bool)
    alert = scores >= threshold
    tp = int(np.sum(alert & truth))
    fp = int(np.sum(alert & ~truth))
    fn = int(np.sum(~alert & truth))
    tn = int(np.sum(~alert & ~truth))
    return {
        "threshold": threshold if np.isfinite(threshold) else None,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "recall": tp / max(tp + fn, 1),
        "fpr": fp / max(fp + tn, 1),
    }


def feature_importance(model: Pipeline) -> list[dict]:
    """Internal importance only, not causal importance or SHAP."""
    names = model.named_steps["preprocessing"].get_feature_names_out()
    estimator = model.named_steps["classifier"]
    values = (
        estimator.feature_importances_
        if hasattr(estimator, "feature_importances_")
        else np.abs(estimator.coef_[0])
    )
    return sorted(
        [
            {"feature": str(name), "importance": float(value)}
            for name, value in zip(names, values, strict=True)
        ],
        key=lambda item: item["importance"],
        reverse=True,
    )


def offline_rule_scores(frame: pd.DataFrame) -> np.ndarray:
    """Baseline offline gồm 3 rule có đủ feature: R001, R003, R004.

    Đây KHÔNG phải backtest đầy đủ 6 rule của worker tuần 5.
    """
    hard_r001 = (
        frame["transaction_type"].eq("TRANSFER")
        & frame["amount"].ge(200_000)
        & frame["amount_to_balance_ratio"].ge(0.90)
    )
    soft_r003 = frame["amount_over_avg_24h"].ge(5)
    soft_r004 = frame["pair_count_24h"].eq(0) & frame["amount"].ge(10_000)

    scores = 0.35 * soft_r003.to_numpy(dtype=float) + 0.35 * soft_r004.to_numpy(dtype=float)
    return np.where(hard_r001.to_numpy(), 1.0, scores)


def make_pipeline(estimator, numeric_features=SAFE_NUMERIC_FEATURES) -> Pipeline:
    preprocessing = ColumnTransformer(
        [
            (
                "numeric",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="median")),
                        ("scale", StandardScaler()),
                    ]
                ),
                numeric_features,
            ),
            (
                "type",
                OneHotEncoder(
                    handle_unknown="ignore",
                    sparse_output=False,
                ),
                CATEGORICAL_FEATURES,
            ),
        ]
    )
    return Pipeline([("preprocessing", preprocessing), ("classifier", estimator)])


def show_split(name: str, frame: pd.DataFrame) -> None:
    print(
        f"{name}: rows={len(frame):,}, "
        f"fraud={int(frame['label'].sum()):,}, "
        f"step={frame['step'].min()}..{frame['step'].max()}"
    )


def main() -> None:
    if not INPUT.is_file():
        raise FileNotFoundError(f"Thiếu {INPUT}; chạy build_week7_features.py trước")

    OUTPUT.mkdir(parents=True, exist_ok=True)
    source_sha256 = sha256_file(SOURCE) if SOURCE.is_file() else "source_file_unavailable"
    feature_sha256 = sha256_file(INPUT)
    data = prepare(pd.read_parquet(INPUT))

    # Baseline chính không dùng old_balance: PaySim gần như mã hóa nhãn bằng
    # amount == oldbalanceOrg. Số dư chỉ còn dùng cho rule/diagnostic.
    assert "label" not in MODEL_FEATURES
    assert "transaction_id" not in MODEL_FEATURES
    assert "step" not in MODEL_FEATURES
    assert "log_old_balance" not in MODEL_FEATURES
    assert "log_balance_ratio" not in MODEL_FEATURES

    train = data.loc[data["step"].between(1, 216)].reset_index(drop=True)
    validation = data.loc[data["step"].between(241, 312)].reset_index(drop=True)
    test = data.loc[data["step"].between(337, 408)].reset_index(drop=True)

    for name, frame in [
        ("train", train),
        ("validation", validation),
        ("test", test),
    ]:
        show_split(name, frame)
        if frame["label"].sum() < 30:
            raise ValueError(f"{name} có quá ít fraud; không nên công bố metric")

    assert train["step"].max() < validation["step"].min()
    assert validation["step"].max() < test["step"].min()

    mlflow.set_tracking_uri(f"sqlite:///{Path('data/ml/week7/mlflow.db').resolve()}")
    experiment_name = "fraud-week7-temporal"
    if mlflow.get_experiment_by_name(experiment_name) is None:
        mlflow.create_experiment(
            experiment_name,
            artifact_location=(OUTPUT.resolve() / "mlflow-artifacts").as_uri(),
        )
    mlflow.set_experiment(experiment_name)

    candidates = {
        "logistic_safe": make_pipeline(
            LogisticRegression(
                class_weight="balanced",
                max_iter=400,
                random_state=42,
            )
        ),
        "random_forest_safe": make_pipeline(
            RandomForestClassifier(
                n_estimators=80,
                max_depth=10,
                min_samples_leaf=20,
                class_weight="balanced_subsample",
                n_jobs=2,
                random_state=42,
            )
        ),
    }

    X_train = train[MODEL_FEATURES]
    y_train = train["label"]
    X_val = validation[MODEL_FEATURES]

    validation_results = {}
    fitted_models = {}
    validation_thresholds = {}

    # Chỉ validation được dùng để chọn model.
    for name, model in candidates.items():
        print(f"\nFitting {name} ...")
        model.fit(X_train, y_train)
        scores = model.predict_proba(X_val)[:, 1]
        result = metrics(validation, scores)
        threshold = threshold_at_fpr(validation, scores)

        validation_results[name] = {
            "capacity": result,
            "fixed_fpr": threshold_metrics(validation, scores, threshold),
        }
        fitted_models[name] = model
        validation_thresholds[name] = threshold
        print(name, json.dumps(validation_results[name], indent=2))

        with mlflow.start_run(run_name=f"{name}-validation"):
            mlflow.log_params(
                {
                    "model_name": name,
                    "feature_set": "no_origin_balance",
                    "train_max_step": 216,
                    "validation_min_step": 241,
                    "validation_max_step": 312,
                    "capacity_per_day": CAPACITY_PER_DAY,
                    "target_fpr": TARGET_FPR,
                    "seed": 42,
                    "source_sha256": source_sha256,
                }
            )
            mlflow.log_metrics({f"val_{key}": value for key, value in result.items()})

    # Tiêu chí chọn được chốt trước khi xem test:
    # recall tại cùng sức chứa 100 hồ sơ/ngày; AP phá hòa.
    champion_name = max(
        validation_results,
        key=lambda name: (
            validation_results[name]["capacity"]["recall_at_capacity"],
            validation_results[name]["capacity"]["average_precision"],
        ),
    )
    champion = fitted_models[champion_name]
    threshold = validation_thresholds[champion_name]
    print(f"\nSelected on validation: {champion_name}")

    # Diagnostic trên validation בלבד: full-balance model có thể khai thác
    # quy luật nhân tạo amount == oldbalanceOrg của PaySim.
    full_model = make_pipeline(
        RandomForestClassifier(
            n_estimators=80,
            max_depth=10,
            min_samples_leaf=20,
            class_weight="balanced_subsample",
            n_jobs=2,
            random_state=42,
        ),
        SAFE_NUMERIC_FEATURES + ["log_old_balance", "log_balance_ratio"],
    )
    full_model.fit(train[FULL_FEATURES], y_train)
    full_scores = full_model.predict_proba(validation[FULL_FEATURES])[:, 1]
    balance_equal = validation["amount"].sub(validation["old_balance_origin"]).abs().lt(0.01)
    diagnostic = {
        "full_balance_validation": metrics(validation, full_scores),
        "amount_equals_old_balance_validation": {
            "fraud": int((balance_equal & validation["label"].eq(1)).sum()),
            "legitimate": int((balance_equal & validation["label"].eq(0)).sum()),
        },
    }
    del full_model

    # Test được đánh giá đúng một lần sau khi đã chọn champion.
    test_scores = champion.predict_proba(test[MODEL_FEATURES])[:, 1]
    test_result = metrics(test, test_scores)
    test_fixed_fpr = threshold_metrics(test, test_scores, threshold)
    rule_val = metrics(validation, offline_rule_scores(validation))
    rule_test = metrics(test, offline_rule_scores(test))

    report = {
        "source_sha256": source_sha256,
        "feature_sha256": feature_sha256,
        "split": {
            "train": [1, 216],
            "validation": [241, 312],
            "test": [337, 408],
            "gap_1": [217, 240],
            "gap_2": [313, 336],
        },
        "capacity_per_day": CAPACITY_PER_DAY,
        "target_fpr": TARGET_FPR,
        "feature_set": MODEL_FEATURES,
        "champion": champion_name,
        "validation_models": validation_results,
        "validation_simulator_artifact_diagnostic": diagnostic,
        "offline_three_rule_validation": rule_val,
        "champion_test": test_result,
        "champion_test_fixed_fpr": test_fixed_fpr,
        "offline_three_rule_test": rule_test,
        "limitations": [
            "PaySim synthetic data, not real bank transactions",
            "24-hour label delay is an assumption, not observed metadata",
            "1m random sample has incomplete transaction history",
            "Only R001/R003/R004 are available for offline rule comparison",
            "Late PaySim tail excluded because its traffic distribution shifts",
            "PaySim balance equality nearly encodes fraud; full-balance model is an upper bound",
            "Original full-balance run scored this test; revised test is exploratory, not pristine",
        ],
    }

    report_path = OUTPUT / "report.json"
    report_path.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )
    joblib.dump(champion, OUTPUT / "champion.joblib")
    importance_path = OUTPUT / "feature_importance.json"
    importance_path.write_text(json.dumps(feature_importance(champion), indent=2), encoding="utf-8")

    PrecisionRecallDisplay.from_predictions(
        test["label"],
        test_scores,
        name=champion_name,
    )
    plt.title("Week 7 temporal test: precision–recall")
    plt.tight_layout()
    plt.savefig(OUTPUT / "test_precision_recall.png", dpi=150)
    plt.close()

    with mlflow.start_run(run_name=f"{champion_name}-final-test"):
        mlflow.log_params(
            {
                "champion": champion_name,
                "selection_metric": "validation_recall_at_capacity",
                "feature_set": "no_origin_balance",
                "capacity_per_day": CAPACITY_PER_DAY,
                "validation_fpr_threshold": threshold,
            }
        )
        mlflow.log_metrics({f"test_{key}": value for key, value in test_result.items()})
        mlflow.log_artifact(str(report_path))
        mlflow.log_artifact(str(OUTPUT / "champion.joblib"))
        mlflow.log_artifact(str(importance_path))
        mlflow.log_artifact(str(OUTPUT / "test_precision_recall.png"))

    print("\nTEST MODEL")
    print(json.dumps(test_result, indent=2))
    print("\nTEST MODEL AT VALIDATION-SELECTED FPR THRESHOLD")
    print(json.dumps(test_fixed_fpr, indent=2))
    print("\nTEST OFFLINE 3-RULE BASELINE")
    print(json.dumps(rule_test, indent=2))
    print(f"\nArtifacts: {OUTPUT}")


if __name__ == "__main__":
    main()
