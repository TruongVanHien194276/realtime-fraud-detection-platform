"""Small regression tests for temporal windows and evaluation policy."""

import importlib

import duckdb
import pytest

pytest.importorskip("mlflow")
np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")

week7 = importlib.import_module("scripts.train_week7")


def test_window_uses_only_prior_hours() -> None:
    rows = duckdb.sql(
        """
        SELECT step,
               COUNT(*) OVER (
                   PARTITION BY customer
                   ORDER BY step
                   RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING
               ) AS prior_count
        FROM (VALUES (1, 'A'), (2, 'A'), (2, 'A'), (27, 'A'))
             AS events(step, customer)
        ORDER BY step
        """
    ).fetchall()
    assert rows == [(1, 0), (2, 1), (2, 1), (27, 0)]


def test_topk_never_uses_label_and_is_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("scripts.train_week7.CAPACITY_PER_DAY", 1)
    frame = pd.DataFrame(
        {
            "event_day": [0, 0, 1, 1],
            "amount": [20.0, 10.0, 5.0, 6.0],
            "transaction_id": ["a", "b", "c", "d"],
            "label": [0, 1, 1, 0],
        }
    )
    scores = np.array([0.5, 0.5, 0.2, 0.9])
    assert week7.topk_flags(frame, scores).tolist() == [True, False, False, True]
    frame["label"] = 1 - frame["label"]
    assert week7.topk_flags(frame, scores).tolist() == [True, False, False, True]


def test_rule_subset_matches_known_weights() -> None:
    frame = pd.DataFrame(
        {
            "transaction_type": ["TRANSFER", "PAYMENT", "CASH_OUT"],
            "amount": [250_000.0, 20_000.0, 5_000.0],
            "amount_to_balance_ratio": [0.95, np.nan, np.nan],
            "amount_over_avg_24h": [0.0, 6.0, 0.0],
            "pair_count_24h": [0, 0, 0],
        }
    )
    assert week7.offline_rule_scores(frame).tolist() == [1.0, 0.7, 0.0]


def test_model_feature_allowlist_excludes_labels_and_balance() -> None:
    assert "label" not in week7.MODEL_FEATURES
    assert "transaction_id" not in week7.MODEL_FEATURES
    assert "step" not in week7.MODEL_FEATURES
    assert "log_old_balance" not in week7.MODEL_FEATURES
    assert "log_balance_ratio" not in week7.MODEL_FEATURES
