import json
import time
from pathlib import Path

from fastapi.testclient import TestClient

from fraud_platform import scoring_api

FIXTURE = Path("tests/fixtures/week8_golden.json")


class SlowModel:
    def predict_proba(self, _frame):
        time.sleep(0.1)
        return [[0.9, 0.1]]


def test_slow_model_falls_back_to_rules(monkeypatch):
    monkeypatch.setattr(scoring_api, "MODEL_TIMEOUT_SECONDS", 0.01)
    body = json.loads(FIXTURE.read_text())["request"]

    with TestClient(scoring_api.app) as client:
        scoring_api.app.state.model = SlowModel()
        scoring_api.app.state.model_version = "slow-test"
        scoring_api.app.state.redis = None  # Cache must not hide the timeout.
        response = client.post("/score", json=body)

    assert response.status_code == 200
    result = response.json()
    assert result["degraded"] is True
    assert result["model_score"] is None
    assert "MODEL_INFERENCE_TIMEOUT" in result["reason_codes"]
    assert "FALLBACK_RULE_ONLY" in result["reason_codes"]


def test_busy_model_worker_also_falls_back(monkeypatch):
    monkeypatch.setattr(scoring_api, "MODEL_TIMEOUT_SECONDS", 0.01)
    body = json.loads(FIXTURE.read_text())["request"]

    with TestClient(scoring_api.app) as client:
        scoring_api.app.state.model = SlowModel()
        scoring_api.app.state.model_version = "slow-test"
        scoring_api.app.state.redis = None
        first = client.post("/score", json=body)
        second = client.post("/score", json=body)

    assert first.status_code == second.status_code == 200
    assert first.json()["degraded"] is True
    assert second.json()["degraded"] is True
    assert "MODEL_INFERENCE_TIMEOUT" in second.json()["reason_codes"]
