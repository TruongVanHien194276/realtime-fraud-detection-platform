from fraud_platform.bi_snapshot import alert_confusion, masked_customer


def test_masked_customer_is_stable_and_keyed() -> None:
    first = masked_customer("C123", b"key-1")
    assert first == masked_customer("C123", b"key-1")
    assert first != masked_customer("C123", b"key-2")
    assert "C123" not in first


def test_confusion_uses_labeled_alerts_only() -> None:
    assert alert_confusion("REVIEW", 1) == "TP"
    assert alert_confusion("BLOCK", 0) == "FP"
    assert alert_confusion("APPROVE", 1) == "FN"
    assert alert_confusion("APPROVE", 0) == "TN"
    assert alert_confusion("BLOCK", None) is None
