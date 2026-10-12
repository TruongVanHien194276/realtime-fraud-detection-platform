from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

RAW_COLUMNS = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
]


class TransactionType(StrEnum):
    CASH_IN = "CASH_IN"
    CASH_OUT = "CASH_OUT"
    DEBIT = "DEBIT"
    PAYMENT = "PAYMENT"
    TRANSFER = "TRANSFER"


class FraudDecision(StrEnum):
    APPROVE = "APPROVE"
    REVIEW = "REVIEW"
    BLOCK = "BLOCK"


class TransactionEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"
    event_id: UUID
    transaction_id: str = Field(min_length=1)
    event_time: datetime
    ingested_at: datetime

    step: int = Field(ge=0)
    transaction_type: TransactionType
    amount: Decimal = Field(ge=0)

    customer_id: str = Field(min_length=1)
    recipient_id: str = Field(min_length=1)

    old_balance_origin: Decimal
    new_balance_origin: Decimal
    old_balance_destination: Decimal
    new_balance_destination: Decimal


class FraudDecisionEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"
    event_id: UUID
    transaction_id: str = Field(min_length=1)
    decided_at: datetime

    decision: FraudDecision
    risk_score: float = Field(ge=0, le=1)
    triggered_rules: list[str] = Field(default_factory=list)

    model_version: str | None = None
    rule_version: str
    feature_version: str
    processing_latency_ms: int = Field(ge=0)
