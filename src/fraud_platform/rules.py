from decimal import Decimal

import yaml

NUMERIC_OPERATORS = {
    "gte": lambda left, right: left >= right,
    "gt": lambda left, right: left > right,
    "lte": lambda left, right: left <= right,
    "lt": lambda left, right: left < right,
}


def load_rules(path):
    with open(path) as handle:
        config = yaml.safe_load(handle)

    ids = [rule["id"] for rule in config["rules"]]
    if len(ids) != len(set(ids)):
        raise ValueError("Rule IDs phải duy nhất")
    for rule in config["rules"]:
        if not isinstance(rule.get("version"), int) or rule["version"] < 1:
            raise ValueError(f"Rule {rule['id']} cần version nguyên dương")
        if not rule.get("reason_code") or not rule.get("all"):
            raise ValueError(f"Rule {rule['id']} cần reason_code và conditions")
        if rule.get("severity") not in {"hard", "soft"}:
            raise ValueError(f"Rule {rule['id']} có severity không hợp lệ")
        if not 0 <= Decimal(str(rule.get("weight", -1))) <= 1:
            raise ValueError(f"Rule {rule['id']} có weight ngoài [0, 1]")
        for condition in rule["all"]:
            if not condition.get("field") or condition.get("op") not in {"eq", *NUMERIC_OPERATORS}:
                raise ValueError(f"Rule {rule['id']} có condition không hợp lệ")
    if not 0 <= Decimal(config["policy"]["review_at"]) < Decimal(config["policy"]["block_at"]) <= 1:
        raise ValueError("Policy thresholds không hợp lệ")
    return config


def condition_matches(features: dict, condition: dict) -> bool:
    value = features.get(condition["field"])
    if value is None:
        return False

    operator = condition["op"]
    threshold = condition["value"]

    if operator == "eq":
        return value == threshold

    if operator not in NUMERIC_OPERATORS:
        raise ValueError(f"Unsupported operator: {operator}")

    return NUMERIC_OPERATORS[operator](Decimal(str(value)), Decimal(str(threshold)))


def evaluate(config: dict, features: dict) -> dict:
    hits = []
    score = Decimal("0")

    for rule in config["rules"]:
        if all(condition_matches(features, item) for item in rule["all"]):
            hits.append(rule)
            if rule["severity"] == "soft":
                score += Decimal(str(rule["weight"]))

    score = min(score, Decimal("1"))
    hard_hit = any(rule["severity"] == "hard" for rule in hits)

    if hard_hit:
        decision = "BLOCK"
        score = Decimal("1")
    elif score >= Decimal(config["policy"]["block_at"]):
        decision = "BLOCK"
    elif score >= Decimal(config["policy"]["review_at"]):
        decision = "REVIEW"
    else:
        decision = "APPROVE"

    return {
        "decision": decision,
        "risk_score": float(score),
        "triggered_rules": [rule["id"] for rule in hits],
        "reason_codes": [rule["reason_code"] for rule in hits],
    }
