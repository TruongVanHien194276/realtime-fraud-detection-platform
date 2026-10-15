import json
from copy import deepcopy
from pathlib import Path

from fastapi.testclient import TestClient

from fraud_platform.rules import load_rules
from fraud_platform.scoring_api import (
    ScoreRequest,
    app,
    model_frame,
    request_digest,
    rule_features,
)

FIXTURE = Path("tests/fixtures/week8_golden.json")


def golden():
    return json.loads(FIXTURE.read_text())


def test_hourly_model_vs_event_time_rules():
    data = golden()
    request = ScoreRequest.model_validate(data["request"])

    model_row = model_frame(request).iloc[0]
    for name, expected in data["expected_model_counts"].items():
        assert model_row[name] == expected

    rule_row = rule_features(request)
    assert rule_row["customer_count_24h"] == data["expected_rule_customer_count_24h"]


def test_same_input_same_response():
    with TestClient(app) as client:
        body = golden()["request"]
        first = client.post("/score", json=body)
        second = client.post("/score", json=body)

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert first.json() == second.json()
    assert first.json()["model_feature_version"] == "week7-hourly-safe-v1"


def test_fallback_when_model_unavailable():
    with TestClient(app) as client:
        app.state.model = None
        app.state.model_version = None
        app.state.redis = None  # Không để cache che mất fallback.

        response = client.post("/score", json=golden()["request"])

    assert response.status_code == 200, response.text
    assert response.json()["degraded"] is True
    assert "MODEL_UNAVAILABLE" in response.json()["reason_codes"]


def test_reject_future_history():
    body = golden()["request"]
    body["history"][0]["event_time"] = "2025-01-01T11:00:00Z"

    with TestClient(app) as client:
        response = client.post("/score", json=body)

    assert response.status_code == 422


def test_hard_rule_blocks_with_or_without_model():
    body = golden()["request"]
    body["transaction"]["amount"] = "200000"
    body["transaction"]["old_balance_origin"] = "200000"
    body["history"] = []

    with TestClient(app) as client:
        response = client.post("/score", json=body)

    assert response.status_code == 200, response.text
    assert response.json()["decision"] == "BLOCK"
    assert "R001" in response.json()["triggered_rules"]


def test_cache_key_changes_with_rules_or_model_availability():
    request = ScoreRequest.model_validate(golden()["request"])
    rules = load_rules("config/rules-v1.yaml")
    changed_rules = deepcopy(rules)
    changed_rules["policy"]["review_at"] = "0.40"

    original = request_digest(request, "1", rules, True)
    assert original != request_digest(request, "1", changed_rules, True)
    assert original != request_digest(request, "1", rules, False)
