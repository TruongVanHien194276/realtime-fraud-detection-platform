"""Create a deterministic PaySim-shaped smoke fixture, not benchmark data."""

import argparse
import csv
from pathlib import Path

from fraud_platform.contracts import RAW_COLUMNS


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=1000)
    args = parser.parse_args()
    if args.rows < 1:
        parser.error("--rows must be positive")
    if args.output.exists():
        parser.error(f"Output already exists: {args.output}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=RAW_COLUMNS)
        writer.writeheader()
        for index in range(args.rows):
            amount = 200_000 if index % 100 == 0 else 10 + index % 50
            writer.writerow(
                {
                    "step": index // 100 + 1,
                    "type": "TRANSFER" if index % 20 == 0 else "PAYMENT",
                    "amount": f"{amount:.2f}",
                    "nameOrig": f"C{index:08d}",
                    "oldbalanceOrg": "300000.00",
                    "newbalanceOrig": f"{300000 - amount:.2f}",
                    "nameDest": f"M{index:08d}",
                    "oldbalanceDest": "0.00",
                    "newbalanceDest": f"{amount:.2f}",
                    "isFraud": int(index % 100 == 0),
                    "isFlaggedFraud": 0,
                }
            )
    print(f"Wrote {args.rows} synthetic rows to {args.output}")


if __name__ == "__main__":
    main()
