import json
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
WAREHOUSE = "data/tmp/week2-benchmark-100k-v1"
OUTPUT = PROJECT / "data/tmp/week9/lake-quality.json"


def number(label: str, output: str) -> int:
    match = re.search(rf"^{re.escape(label)}: ([0-9]+)$", output, re.MULTILINE)
    if match is None:
        raise ValueError(f"Không thấy metric {label} trong verify_lakehouse output")
    return int(match.group(1))


def main() -> None:
    result = subprocess.run(
        ["make", "verify-lakehouse", f"SPARK_WAREHOUSE={WAREHOUSE}"],
        cwd=PROJECT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    print(result.stdout, flush=True)

    report = {
        "checked_at_utc": datetime.now(UTC).isoformat(),
        "warehouse": WAREHOUSE,
        "bronze_rows": number("Bronze", result.stdout),
        "valid_rows": number("Silver valid", result.stdout),
        "rejected_rows": number("Silver quarantine", result.stdout),
        "duplicate_ids": number("Duplicate IDs in Bronze", result.stdout),
        "valid_quarantine_overlap": number("Valid/quarantine overlap", result.stdout),
        "passed": result.returncode == 0 and "RECONCILIATION PASSED" in result.stdout,
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))

    if not report["passed"]:
        raise SystemExit("LAKE QUALITY FAILED")


if __name__ == "__main__":
    main()
