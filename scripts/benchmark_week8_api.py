import argparse
import json
import math
import time
import urllib.error
import urllib.request
from collections import Counter
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
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.requests < 1:
        parser.error("--requests must be at least 1")
    if args.warmup < 0:
        parser.error("--warmup must be nonnegative")
    run_id = uuid4().hex[:12]

    fixture = json.loads(Path("tests/fixtures/week8_golden.json").read_text())["request"]

    latencies = []
    errors = 0
    degraded_responses = 0
    degraded_reasons: Counter[str] = Counter()
    window_started = None

    for number in range(-args.warmup, args.requests):
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

        if number == 0:
            window_started = time.perf_counter()
        started = time.perf_counter()
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                payload = json.loads(response.read())
                if number >= 0:
                    if response.status != 200:
                        errors += 1
                    else:
                        latencies.append((time.perf_counter() - started) * 1000)
                        degraded_responses += int(payload["degraded"])
                        if payload["degraded"]:
                            degraded_reasons.update(
                                reason
                                for reason in payload["reason_codes"]
                                if reason.startswith("MODEL_") or reason.startswith("FALLBACK_")
                            )
        except (urllib.error.URLError, TimeoutError, ValueError, KeyError):
            if number >= 0:
                errors += 1

    window_seconds = time.perf_counter() - window_started

    report = {
        "measured_at_utc": datetime.now(UTC).isoformat(),
        "url": args.url,
        "method": "sequential, unique transaction_id per request, one API worker",
        "run_id": run_id,
        "warmup_requests": args.warmup,
        "requests": args.requests,
        "successful_requests": len(latencies),
        "errors": errors,
        "error_rate": errors / args.requests,
        "degraded_responses": degraded_responses,
        "degraded_reasons": dict(sorted(degraded_reasons.items())),
        "window_seconds": round(window_seconds, 3),
        "successful_rps": round(len(latencies) / window_seconds, 2),
        "p50_ms": round(percentile(latencies, 0.50), 2) if latencies else None,
        "p95_ms": round(percentile(latencies, 0.95), 2) if latencies else None,
        "p99_ms": round(percentile(latencies, 0.99), 2) if latencies else None,
    }
    print(json.dumps(report, indent=2))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
