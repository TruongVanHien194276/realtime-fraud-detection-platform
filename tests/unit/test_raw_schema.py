from fraud_platform.contracts import RAW_COLUMNS, TransactionEvent


def test_raw_schema_contains_expected_columns() -> None:
    assert RAW_COLUMNS == [
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


def test_labels_are_not_transaction_event_fields() -> None:
    transaction_event_fields = TransactionEvent.model_fields

    assert "isFraud" not in transaction_event_fields
    assert "isFlaggedFraud" not in transaction_event_fields
