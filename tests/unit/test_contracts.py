from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from fraud_platform.contracts import (
    FraudDecision,
    FraudDecisionEvent,
    FraudLabelEvent,
    TransactionEvent,
    TransactionType,
)


def valid_transaction() -> TransactionEvent:
    now = datetime.now(UTC)

    return TransactionEvent(
        event_id=uuid4(),
        transaction_id="txn-000001",
        event_time=now,
        ingested_at=now,
        step=1,
        transaction_type=TransactionType.TRANSFER,
        amount=Decimal("250.50"),
        customer_id="customer-1",
        recipient_id="recipient-1",
        old_balance_origin=Decimal("1000.00"),
        new_balance_origin=Decimal("749.50"),
        old_balance_destination=Decimal("100.00"),
        new_balance_destination=Decimal("350.50"),
    )


def test_valid_transaction_event() -> None:
    event = valid_transaction()

    assert event.schema_version == "1.0"
    assert event.amount == Decimal("250.50")
    assert event.transaction_type == TransactionType.TRANSFER


def test_negative_amount_is_rejected() -> None:
    data = valid_transaction().model_dump()
    data["amount"] = Decimal("-1.00")

    with pytest.raises(ValidationError):
        TransactionEvent.model_validate(data)


def test_unknown_field_is_rejected() -> None:
    data = valid_transaction().model_dump()
    data["unknown_field"] = "unexpected"

    with pytest.raises(ValidationError):
        TransactionEvent.model_validate(data)


def test_valid_decision_event() -> None:
    decision = FraudDecisionEvent(
        event_id=uuid4(),
        transaction_id="txn-000001",
        decided_at=datetime.now(UTC),
        decision=FraudDecision.REVIEW,
        risk_score=0.72,
        triggered_rules=["velocity_5m_high"],
        rule_version="rules-v1",
        feature_version="features-v1",
        model_version=None,
        processing_latency_ms=420,
    )

    assert decision.decision == FraudDecision.REVIEW
    assert decision.risk_score == 0.72


def test_risk_score_above_one_is_rejected() -> None:
    with pytest.raises(ValidationError):
        FraudDecisionEvent(
            event_id=uuid4(),
            transaction_id="txn-000002",
            decided_at=datetime.now(UTC),
            decision=FraudDecision.BLOCK,
            risk_score=1.2,
            rule_version="rules-v1",
            feature_version="features-v1",
            processing_latency_ms=100,
        )


def test_label_event_rejects_values_other_than_zero_or_one() -> None:
    with pytest.raises(ValidationError):
        FraudLabelEvent(
            run_id="week5-smoke-v1",
            transaction_id="txn-000001",
            label=2,
            label_observed_at=datetime.now(UTC),
            label_source="paysim_simulated_chargeback",
        )
