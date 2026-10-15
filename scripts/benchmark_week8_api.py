import argparse
import json
import math
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8002/score")
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.requests < 1:
        parser.error("--requests must be at least 1")
    run_id = uuid4().hex[:12]

    fixture = json.loads(Path("tests/fixtures/week8_golden.json").read_text())["request"]

    latencies = []
    errors = 0

    for number in range(args.requests):
        payload = json.loads(json.dumps(fixture))
        # Khác transaction_id để đo inference thật, không chỉ Redis cache hit.
        payload["transaction"]["transaction_id"] = f"bench-{run_id}-{number:05d}"
        body = json.dumps(payload).encode()

        request = urllib.request.Request(
            args.url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        started = time.perf_counter()
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                response.read()
                if response.status != 200:
                    errors += 1
        except (urllib.error.URLError, TimeoutError):
            errors += 1

        latencies.append((time.perf_counter() - started) * 1000)

    report = {
        "measured_at_utc": datetime.now(UTC).isoformat(),
        "url": args.url,
        "method": "sequential, unique transaction_id per request, one API worker",
        "run_id": run_id,
        "requests": args.requests,
        "errors": errors,
        "error_rate": errors / args.requests,
        "p50_ms": round(percentile(latencies, 0.50), 2),
        "p95_ms": round(percentile(latencies, 0.95), 2),
    }
    print(json.dumps(report, indent=2))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
