from datetime import timedelta
from decimal import Decimal
from statistics import median

WINDOWS = {
    "5m": timedelta(minutes=5),
    "1h": timedelta(hours=1),
    "24h": timedelta(hours=24),
}


def _prior(rows: list[dict], tx) -> list[dict]:
    """Lọc lần nữa ở Python để phòng SQL query về sau bị sửa sai."""
    current_key = (tx.event_time, tx.transaction_id)
    return [
        row
        for row in rows
        if (row["event_time"], row["transaction_id"]) < current_key
        and row["event_time"] >= tx.event_time - timedelta(hours=24)
    ]


def build_features(tx, customer_rows: list[dict], recipient_rows: list[dict]) -> dict:
    customer = _prior(customer_rows, tx)
    recipient = _prior(recipient_rows, tx)
    result = {
        "transaction_type": tx.transaction_type.value,
        "amount": str(tx.amount),
        "amount_to_balance_ratio": None,
        "seconds_since_previous": None,
        "amount_over_avg_24h": None,
        "amount_over_median_24h": None,
    }

    for label, window in WINDOWS.items():
        cutoff = tx.event_time - window
        customer_window = [r for r in customer if r["event_time"] >= cutoff]
        recipient_window = [r for r in recipient if r["event_time"] >= cutoff]

        result[f"customer_count_{label}"] = len(customer_window)
        result[f"customer_amount_sum_{label}"] = str(
            sum((r["amount"] for r in customer_window), Decimal("0"))
        )
        result[f"recipient_count_{label}"] = len(recipient_window)
        result[f"recipient_amount_sum_{label}"] = str(
            sum((r["amount"] for r in recipient_window), Decimal("0"))
        )

    customer_1h = [r for r in customer if r["event_time"] >= tx.event_time - timedelta(hours=1)]
    recipient_1h = [r for r in recipient if r["event_time"] >= tx.event_time - timedelta(hours=1)]

    result["customer_distinct_recipients_24h"] = len({r["recipient_id"] for r in customer})
    result["new_recipient_24h"] = not any(r["recipient_id"] == tx.recipient_id for r in customer)
    result["recipient_fan_in_1h"] = len({r["customer_id"] for r in recipient_1h})
    result["customer_fan_out_1h"] = len({r["recipient_id"] for r in customer_1h})

    if customer:
        previous = max(customer, key=lambda r: (r["event_time"], r["transaction_id"]))
        result["seconds_since_previous"] = int(
            (tx.event_time - previous["event_time"]).total_seconds()
        )

        amounts = [r["amount"] for r in customer]
        average = sum(amounts, Decimal("0")) / len(amounts)
        middle = median(amounts)
        if average > 0:
            result["amount_over_avg_24h"] = str(tx.amount / average)
        if middle > 0:
            result["amount_over_median_24h"] = str(tx.amount / middle)

    if tx.old_balance_origin > 0:
        result["amount_to_balance_ratio"] = str(tx.amount / tx.old_balance_origin)

    return result
