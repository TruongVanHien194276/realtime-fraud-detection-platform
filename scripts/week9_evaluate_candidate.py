"""Compare candidate validation metrics to Week 7; never auto-promote."""

import json
from datetime import UTC, datetime
from pathlib import Path

CANDIDATE = Path("artifacts/week9/candidate_report.json")
BASELINE = Path("artifacts/week7/report.json")
OUTPUT = Path("reports/week9_model_evaluation.json")


def main() -> None:
    candidate = json.loads(CANDIDATE.read_text(encoding="utf-8"))
    baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
    champion = baseline["champion"]
    baseline_validation = baseline["validation_models"][champion]["capacity"]
    candidate_validation = candidate["validation"]
    report = {
        "evaluated_at_utc": datetime.now(UTC).isoformat(),
        "candidate": candidate["model"],
        "feature_sha256": candidate["feature_sha256"],
        "baseline_champion": champion,
        "candidate_validation_ap": candidate_validation["average_precision"],
        "baseline_validation_ap": baseline_validation["average_precision"],
        "candidate_recall_at_capacity": candidate_validation["recall_at_capacity"],
        "baseline_recall_at_capacity": baseline_validation["recall_at_capacity"],
        "recommend_promotion": (
            candidate_validation["recall_at_capacity"] > baseline_validation["recall_at_capacity"]
            and candidate_validation["average_precision"]
            >= baseline_validation["average_precision"]
        ),
        "auto_promoted": False,
        "test_evaluated": False,
        "caveat": (
            "Same synthetic feature cohort; validation comparison is not an independent holdout."
        ),
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
