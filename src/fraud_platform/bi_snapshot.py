"""Small, testable helpers for the curated BI snapshot."""

import hashlib
import hmac


def masked_customer(customer_id: str, key: bytes) -> str:
    """Stable within this project, but hides the PaySim account identifier."""
    if not customer_id or not key:
        raise ValueError("customer_id and key are required")
    digest = hmac.new(key, customer_id.encode(), hashlib.sha256).hexdigest()
    return f"C-{digest[:16]}"


def alert_confusion(decision: str, label: int | None) -> str | None:
    """REVIEW and BLOCK are both alerts; unlabeled rows have no outcome."""
    if label is None:
        return None
    if decision not in {"APPROVE", "REVIEW", "BLOCK"} or label not in {0, 1}:
        raise ValueError("invalid decision or label")
    alert = decision != "APPROVE"
    return ("TP" if alert else "FN") if label == 1 else ("FP" if alert else "TN")
