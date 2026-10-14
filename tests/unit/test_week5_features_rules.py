from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
import yaml

from fraud_platform.contracts import TransactionEvent
from fraud_platform.features import build_features
from fraud_platform.rules import evaluate, load_rules


def tx(transaction_id, event_time):
    return TransactionEvent(
        schema_version="1.0",
        event_id=uuid4(),
        transaction_id=transaction_id,
        event_time=event_time,
        ingested_at=datetime.now(UTC),
        step=1,
        transaction_type="TRANSFER",
        amount=Decimal("100"),
        customer_id="C1",
        recipient_id="R2",
        old_balance_origin=Decimal("200"),
        new_balance_origin=Decimal("100"),
        old_balance_destination=Decimal("0"),
        new_balance_destination=Decimal("100"),
    )


def test_feature_does_not_use_future_or_current_event():
    t = datetime(2025, 1, 1, 10, tzinfo=UTC)
    current = tx("b", t)
    history = [
        {
            "transaction_id": "a",
            "event_time": t - timedelta(minutes=1),
            "amount": Decimal("20"),
            "customer_id": "C1",
            "recipient_id": "R1",
        },
        {
            "transaction_id": "b",  # chính event hiện tại
            "event_time": t,
            "amount": Decimal("999"),
            "customer_id": "C1",
            "recipient_id": "R2",
        },
        {
            "transaction_id": "c",  # event tương lai
            "event_time": t + timedelta(minutes=1),
            "amount": Decimal("999"),
            "customer_id": "C1",
            "recipient_id": "R3",
        },
    ]

    features = build_features(current, history, [])
    assert features["customer_count_5m"] == 1
    assert features["customer_amount_sum_5m"] == "20"
    assert features["new_recipient_24h"] is True


def test_threshold_comes_from_yaml():
    config = load_rules("config/rules-v1.yaml")
    features = {
        "transaction_type": "TRANSFER",
        "amount": "300000",
        "amount_to_balance_ratio": "0.95",
        "customer_count_5m": 0,
        "amount_over_avg_24h": None,
        "new_recipient_24h": False,
        "recipient_fan_in_1h": 0,
        "customer_fan_out_1h": 0,
    }
    result = evaluate(config, features)
    assert result["decision"] == "BLOCK"
    assert "R001" in result["triggered_rules"]
    assert "LARGE_TRANSFER_NEAR_FULL_BALANCE" in result["reason_codes"]


def test_same_timestamp_uses_transaction_id_tie_breaker():
    event_time = datetime(2025, 1, 1, 10, tzinfo=UTC)
    rows = [
        {
            "transaction_id": transaction_id,
            "event_time": event_time,
            "amount": Decimal("10"),
            "customer_id": "C1",
            "recipient_id": "R1",
        }
        for transaction_id in ("a", "b", "c")
    ]

    features = build_features(tx("b", event_time), rows, [])
    assert features["customer_count_5m"] == 1
    assert features["customer_amount_sum_5m"] == "10"
    assert features["seconds_since_previous"] == 0


def test_policy_thresholds_change_without_source_edit():
    config = load_rules("config/rules-v1.yaml")
    features = {
        "transaction_type": "TRANSFER",
        "amount": "10000",
        "amount_to_balance_ratio": "0.1",
        "customer_count_5m": 0,
        "amount_over_avg_24h": None,
        "new_recipient_24h": True,
        "recipient_fan_in_1h": 0,
        "customer_fan_out_1h": 0,
    }
    assert evaluate(config, features)["decision"] == "REVIEW"

    stricter = deepcopy(config)
    stricter["policy"]["review_at"] = "0.40"
    assert evaluate(stricter, features)["decision"] == "APPROVE"

    more_aggressive = deepcopy(config)
    more_aggressive["policy"]["block_at"] = "0.35"
    assert evaluate(more_aggressive, features)["decision"] == "BLOCK"


def test_rule_config_rejects_invalid_weight(tmp_path):
    config = load_rules("config/rules-v1.yaml")
    config["rules"][0]["weight"] = "1.5"
    path = tmp_path / "invalid-rules.yaml"
    path.write_text(yaml.safe_dump(config))

    with pytest.raises(ValueError, match="weight"):
        load_rules(path)
