"""Summarize sampled Docker stats; these maxima are not true process peaks."""

import argparse
import json
import re
from pathlib import Path

MEMORY = re.compile(r"^([0-9.]+)(B|KiB|MiB|GiB|TiB)$")
FACTORS = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def memory_bytes(value: str) -> float:
    match = MEMORY.fullmatch(value.split(" / ", 1)[0])
    if match is None:
        raise ValueError(f"Unrecognized Docker memory value: {value}")
    return float(match[1]) * FACTORS[match[2]]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    rows = []
    for raw_line in args.input.read_text().splitlines():
        line = ANSI_ESCAPE.sub("", raw_line).strip()
        if line.startswith("{"):
            row = json.loads(line)
            if row.get("CPUPerc") not in {None, "--"} and row.get("MemUsage") not in {
                None,
                "--",
            }:
                rows.append(row)
    if not rows:
        raise SystemExit("No Docker stats samples")

    report = {
        "source": str(args.input),
        "container": rows[0]["Name"],
        "samples": len(rows),
        "max_sampled_cpu_percent": round(max(float(row["CPUPerc"].rstrip("%")) for row in rows), 2),
        "max_sampled_memory_mib": round(
            max(memory_bytes(row["MemUsage"]) for row in rows) / 1024**2, 1
        ),
        "measurement_note": (
            "Maximum among Docker stats samples, not a guaranteed peak; "
            "CPU may exceed 100% on a multi-core container"
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
