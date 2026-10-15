import hashlib
from pathlib import Path

import joblib
import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient

MODEL_PATH = Path("artifacts/week7/champion.joblib")
TRACKING_DB = Path("data/ml/week7/mlflow.db")
ARTIFACT_DIR = Path("artifacts/week8/mlflow-artifacts")
MODEL_NAME = "fraud-risk-safe"
EXPERIMENT_NAME = "week8-serving"
REVIEW_AT = "0.9546722331383121"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    if not MODEL_PATH.is_file() or not TRACKING_DB.is_file():
        raise SystemExit("Thiếu artifact/MLflow DB của Tuần 7")

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(f"sqlite:///{TRACKING_DB.resolve()}")

    if mlflow.get_experiment_by_name(EXPERIMENT_NAME) is None:
        mlflow.create_experiment(
            EXPERIMENT_NAME,
            artifact_location=ARTIFACT_DIR.resolve().as_uri(),
        )
    mlflow.set_experiment(EXPERIMENT_NAME)

    digest = sha256(MODEL_PATH)
    client = MlflowClient()
    matching = [
        version
        for version in client.search_model_versions(f"name='{MODEL_NAME}'")
        if version.tags.get("source_sha256") == digest
    ]
    if matching:
        version = max(matching, key=lambda item: int(item.version)).version
        client.set_registered_model_alias(MODEL_NAME, "candidate", version)
        print(f"registered_model={MODEL_NAME}")
        print(f"version={version} (reused; matching SHA-256)")
        print("alias=candidate")
        print(f"source_sha256={digest}")
        return

    model = joblib.load(MODEL_PATH)  # Chỉ load artifact do chính project tạo.

    # Đúng thứ tự và kiểu input mà pipeline Tuần 7 đã train
    example = pd.DataFrame(
        [
            {
                "log_amount": 6.90875477931522,
                "customer_count_24h": 1,
                "recipient_count_24h": 1,
                "pair_count_24h": 1,
                "event_hour": 10,
                "transaction_type": "TRANSFER",
            }
        ]
    )
    signature = infer_signature(example, model.predict(example))

    with mlflow.start_run(run_name="register-week7-safe-model") as run:
        mlflow.set_tags(
            {
                "source_week": "7",
                "source_sha256": digest,
                "model_feature_version": "week7-hourly-safe-v1",
                "selection": "validation-only",
                "scope": "synthetic-PaySim-demo",
            }
        )
        mlflow.sklearn.log_model(
            sk_model=model,
            name="fraud-risk-safe",
            registered_model_name=MODEL_NAME,
            # MLflow 3 mặc định dùng skops; RandomForest tuần 7 chứa
            # sklearn.tree._tree.Tree nên skops chặn. Chỉ dùng cloudpickle
            # cho model tự train/tạo trong project này, không cho file lạ.
            serialization_format="cloudpickle",
            signature=signature,
            input_example=example,
        )
        run_id = run.info.run_id

    versions = [
        version
        for version in client.search_model_versions(f"name='{MODEL_NAME}'")
        if version.run_id == run_id
    ]
    if len(versions) != 1:
        raise RuntimeError(f"Không xác định được version vừa đăng ký: {versions}")

    version = versions[0].version
    client.set_model_version_tag(MODEL_NAME, version, "source_sha256", digest)
    client.set_model_version_tag(MODEL_NAME, version, "review_at", REVIEW_AT)
    client.set_registered_model_alias(MODEL_NAME, "candidate", version)

    print(f"registered_model={MODEL_NAME}")
    print(f"version={version}")
    print("alias=candidate")
    print(f"source_sha256={digest}")


if __name__ == "__main__":
    main()
