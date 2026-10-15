import hashlib
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from decimal import Decimal
from math import isfinite, log1p

import mlflow
import mlflow.sklearn
import pandas as pd
import redis
from fastapi import FastAPI, HTTPException
from mlflow.tracking import MlflowClient
from pydantic import BaseModel, ConfigDict, Field, model_validator

from fraud_platform.contracts import TransactionType
from fraud_platform.features import build_features
from fraud_platform.rules import evaluate, load_rules

MODEL_NAME = os.getenv("MODEL_NAME", "fraud-risk-safe")
MODEL_ALIAS = os.getenv("MODEL_ALIAS", "candidate")
REVIEW_AT = float(os.getenv("MODEL_REVIEW_AT", "0.9546722331383121"))
BLOCK_AT = float(os.getenv("MODEL_BLOCK_AT", "0.99"))
ALLOW_MODEL_BLOCK = os.getenv("ALLOW_MODEL_BLOCK", "false").lower() == "true"
FALLBACK_MODE = os.getenv("FALLBACK_MODE", "rule_only")
MODEL_FEATURE_VERSION = "week7-hourly-safe-v1"
POLICY_VERSION = "hybrid-v1"


class ScoreTransaction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    transaction_id: str = Field(min_length=1)
    event_time: datetime
    step: int = Field(ge=1)
    transaction_type: TransactionType
    amount: Decimal = Field(ge=0)
    customer_id: str = Field(min_length=1)
    recipient_id: str = Field(min_length=1)
    old_balance_origin: Decimal = Field(ge=0)


class HistoryTransaction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    transaction_id: str = Field(min_length=1)
    event_time: datetime
    step: int = Field(ge=1)
    amount: Decimal = Field(ge=0)
    customer_id: str = Field(min_length=1)
    recipient_id: str = Field(min_length=1)


class ScoreRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    transaction: ScoreTransaction
    history: list[HistoryTransaction] = Field(default_factory=list, max_length=5000)

    @model_validator(mode="after")
    def validate_snapshot(self):
        tx = self.transaction
        if tx.event_time.tzinfo is None:
            raise ValueError("event_time phải có timezone, ví dụ Z")

        ids = set()
        for row in self.history:
            if row.event_time.tzinfo is None:
                raise ValueError("history.event_time phải có timezone")
            if row.transaction_id in ids:
                raise ValueError("history có transaction_id trùng")
            ids.add(row.transaction_id)
            if row.transaction_id == tx.transaction_id:
                raise ValueError("history chứa chính giao dịch cần score")
            if (row.event_time, row.transaction_id) >= (
                tx.event_time,
                tx.transaction_id,
            ):
                raise ValueError("history chứa giao dịch chưa xảy ra")
            if row.step > tx.step:
                raise ValueError("history.step lớn hơn transaction.step")
            if row.event_time < tx.event_time - timedelta(hours=24):
                raise ValueError("history vượt quá cửa sổ 24 giờ")
        return self


class ScoreResponse(BaseModel):
    transaction_id: str
    decision: str
    risk_score: float
    model_score: float | None
    rule_score: float
    triggered_rules: list[str]
    reason_codes: list[str]
    model_name: str
    model_alias: str
    model_version: str | None
    rule_version: str
    rule_feature_version: str
    model_feature_version: str
    policy_version: str
    model_review_at: float
    model_block_at: float | None
    degraded: bool


def model_frame(request: ScoreRequest) -> pd.DataFrame:
    """Dùng đúng ngữ nghĩa SQL Tuần 7: step-24 ... step-1, không lấy cùng step."""
    tx = request.transaction
    prior_hour = [row for row in request.history if tx.step - 24 <= row.step < tx.step]
    customer = [row for row in prior_hour if row.customer_id == tx.customer_id]
    recipient = [row for row in prior_hour if row.recipient_id == tx.recipient_id]
    pair = [row for row in customer if row.recipient_id == tx.recipient_id]

    return pd.DataFrame(
        [
            {
                "log_amount": log1p(float(tx.amount)),
                "customer_count_24h": len(customer),
                "recipient_count_24h": len(recipient),
                "pair_count_24h": len(pair),
                "event_hour": tx.step % 24,
                "transaction_type": tx.transaction_type.value,
            }
        ]
    )


def rule_features(request: ScoreRequest) -> dict:
    """Rules Tuần 5 dùng event_time và cả giao dịch trước đó trong cùng giờ."""
    rows = [row.model_dump() for row in request.history]
    tx = request.transaction
    customer = [row for row in rows if row["customer_id"] == tx.customer_id]
    recipient = [row for row in rows if row["recipient_id"] == tx.recipient_id]
    return build_features(tx, customer, recipient)


def request_digest(
    request: ScoreRequest,
    model_version: str | None,
    rules: dict,
    model_available: bool,
) -> str:
    payload = {
        "request": request.model_dump(mode="json"),
        "model_version": model_version,
        "model_available": model_available,
        "model_alias": MODEL_ALIAS,
        "policy_version": POLICY_VERSION,
        "rules": rules,
        "review_at": REVIEW_AT,
        "block_at": BLOCK_AT,
        "allow_model_block": ALLOW_MODEL_BLOCK,
        "fallback_mode": FALLBACK_MODE,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.rules = load_rules(os.getenv("RULES_PATH", "config/rules-v1.yaml"))
    app.state.model = None
    app.state.model_version = None
    app.state.model_error = None
    app.state.redis = None

    # Pinned version: alias chỉ được resolve khi khởi động. Alias đổi giữa
    # các request không làm cùng process bất ngờ dùng model khác.
    try:
        mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
        client = MlflowClient()
        version = client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
        app.state.model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{version.version}")
        app.state.model_version = str(version.version)
    except Exception as exc:
        app.state.model_error = type(exc).__name__

    try:
        app.state.redis = redis.Redis.from_url(
            os.getenv("REDIS_URL", "redis://redis:6379/0"),
            socket_connect_timeout=0.2,
            socket_timeout=0.2,
        )
        app.state.redis.ping()
    except redis.RedisError:
        app.state.redis = None  # Cache không được là phụ thuộc correctness.

    yield


app = FastAPI(
    title="Fraud Hybrid Scoring API",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    return {"status": "alive"}


@app.get("/ready")
def ready():
    # Rules đã load thành công nên API vẫn sẵn sàng ở chế độ fallback.
    return {
        "status": "ready" if app.state.model is not None else "degraded",
        "model_loaded": app.state.model is not None,
        "fallback_mode": FALLBACK_MODE,
    }


@app.get("/model-info")
def model_info():
    return {
        "name": MODEL_NAME,
        "alias": MODEL_ALIAS,
        "version": app.state.model_version,
        "feature_version": MODEL_FEATURE_VERSION,
        "load_error": app.state.model_error,
    }


@app.post("/score", response_model=ScoreResponse)
def score(request: ScoreRequest):
    if request.transaction.step > 408:
        raise HTTPException(
            status_code=422,
            detail="Demo model chỉ được đánh giá trên horizon PaySim step <= 408",
        )

    version = app.state.model_version
    digest = request_digest(request, version, app.state.rules, app.state.model is not None)
    key = f"fraud:score:{digest}"
    cache = app.state.redis

    if cache is not None:
        try:
            cached = cache.get(key)
            if cached:
                return ScoreResponse.model_validate_json(cached)
        except redis.RedisError:
            pass

    features = rule_features(request)
    rule_result = evaluate(app.state.rules, features)
    hit_ids = set(rule_result["triggered_rules"])
    hard_ids = {rule["id"] for rule in app.state.rules["rules"] if rule["severity"] == "hard"}
    hard_hit = bool(hit_ids & hard_ids)
    rule_score = float(rule_result["risk_score"])
    reasons = list(rule_result["reason_codes"])

    model_score = None
    if app.state.model is not None:
        try:
            model_score = float(app.state.model.predict_proba(model_frame(request))[0, 1])
            if not isfinite(model_score) or not 0 <= model_score <= 1:
                raise ValueError("Model returned an invalid probability")
        except Exception:
            reasons.append("MODEL_INFERENCE_FAILED")
    else:
        reasons.append("MODEL_UNAVAILABLE")

    degraded = model_score is None

    if hard_hit:
        decision = "BLOCK"
    elif degraded:
        # rule-only mặc định: giữ kết quả rules Tuần 5.
        # Có thể chọn REVIEW toàn bộ bằng FALLBACK_MODE=review.
        decision = "REVIEW" if FALLBACK_MODE == "review" else rule_result["decision"]
        reasons.append("FALLBACK_" + FALLBACK_MODE.upper())
    elif ALLOW_MODEL_BLOCK and model_score >= BLOCK_AT:
        decision = "BLOCK"
        reasons.append("MODEL_ABOVE_BLOCK_THRESHOLD")
    elif rule_result["decision"] != "APPROVE" or model_score >= REVIEW_AT:
        decision = "REVIEW"
        if model_score >= REVIEW_AT:
            reasons.append("MODEL_ABOVE_REVIEW_THRESHOLD")
    else:
        decision = "APPROVE"

    result = ScoreResponse(
        transaction_id=request.transaction.transaction_id,
        decision=decision,
        risk_score=max(rule_score, model_score or 0.0),
        model_score=model_score,
        rule_score=rule_score,
        triggered_rules=rule_result["triggered_rules"],
        reason_codes=reasons,
        model_name=MODEL_NAME,
        model_alias=MODEL_ALIAS,
        model_version=version if not degraded else None,
        rule_version=app.state.rules["rule_version"],
        rule_feature_version=app.state.rules["feature_version"],
        model_feature_version=MODEL_FEATURE_VERSION,
        policy_version=POLICY_VERSION,
        model_review_at=REVIEW_AT,
        model_block_at=BLOCK_AT if ALLOW_MODEL_BLOCK else None,
        degraded=degraded,
    )

    # Lỗi inference có thể tạm thời; không giữ quyết định fallback trong cache.
    if cache is not None and not degraded:
        try:
            cache.setex(key, 3600, result.model_dump_json())
        except redis.RedisError:
            pass

    return result
