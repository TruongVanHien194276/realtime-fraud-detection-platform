import argparse
import hashlib
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from confluent_kafka import Producer

TOPIC = "transactions.raw"


def load_rows(directory: Path) -> list[dict]:
    files = list(directory.glob("part-*"))
    if len(files) != 1:
        raise ValueError(f"Cần đúng một part file trong {directory}; thấy {len(files)}")
    return [json.loads(line) for line in files[0].read_text().splitlines()]


def atomic_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(state, indent=2))
    temporary.replace(path)


def fingerprint(rows: list[dict], seed: int, duplicate_rate: float) -> str:
    content = json.dumps(
        {"rows": rows, "seed": seed, "duplicate_rate": duplicate_rate},
        sort_keys=True,
    )
    return hashlib.sha256(content.encode()).hexdigest()


def should_duplicate(seed: int, transaction_id: str, rate: float) -> bool:
    digest = hashlib.sha256(f"{seed}:{transaction_id}".encode()).digest()
    value = int.from_bytes(digest[:8], "big") / 2**64
    return value < rate


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument(
        "--mode", choices=["replay", "late", "invalid", "malformed"], default="replay"
    )
    parser.add_argument("--brokers", default="redpanda:9092")
    parser.add_argument("--rate", type=float, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--duplicate-rate", type=float, default=0.02)
    parser.add_argument("--cursor", type=Path, default=Path("data/tmp/week4/cursor.json"))
    parser.add_argument("--max-records", type=int, default=0)
    args = parser.parse_args()

    if args.rate <= 0 or not 0 <= args.duplicate_rate <= 1 or args.max_records < 0:
        parser.error("--rate phải > 0; --duplicate-rate nằm trong [0, 1]; --max-records >= 0")

    rows = load_rows(args.input)
    errors: list[str] = []
    producer = Producer(
        {
            "bootstrap.servers": args.brokers,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 10,
        }
    )

    def delivered(error, _message) -> None:
        if error is not None:
            errors.append(str(error))

    def send(payload: dict) -> None:
        event = dict(payload)
        event["event_id"] = str(uuid5(NAMESPACE_URL, event["transaction_id"]))
        event["ingested_at"] = datetime.now(UTC).isoformat()
        producer.produce(
            TOPIC,
            key=event["customer_id"],
            value=json.dumps(event, separators=(",", ":")),
            callback=delivered,
        )

    def confirm() -> None:
        remaining = producer.flush(30)
        if remaining or errors:
            raise RuntimeError(f"Kafka chưa xác nhận hết: remaining={remaining}, errors={errors}")

    if args.mode == "late":
        # Chỉ phát row đầu SAU KHI stream đã xử lý các row 1..999.
        send(rows[0])
        confirm()
        print("Sent first row as late-event probe")
        return

    if args.mode == "invalid":
        producer.produce(
            TOPIC,
            key="bad-demo",
            value='{"schema_version":"9.9","transaction_id":"bad-demo"}',
            callback=delivered,
        )
        confirm()
        print("Sent unsupported-schema probe")
        return

    if args.mode == "malformed":
        producer.produce(TOPIC, key="bad-demo", value="{not-json", callback=delivered)
        confirm()
        print("Sent malformed-JSON probe")
        return

    identity = fingerprint(rows, args.seed, args.duplicate_rate)
    state = (
        json.loads(args.cursor.read_text())
        if args.cursor.exists()
        else {"next_index": 1, "fingerprint": identity}
    )
    if state["fingerprint"] != identity:
        raise ValueError("Cursor không khớp input/seed/duplicate-rate; dùng cursor mới")

    index = state["next_index"]
    if not 1 <= index <= len(rows):
        raise ValueError(f"Cursor index không hợp lệ: {index}")

    # Dành row 0 để sau đó chứng minh late event. Replay row 1..999.
    stop = len(rows)
    if args.max_records:
        stop = min(stop, index + args.max_records)

    started = time.monotonic()
    sent_unique = 0

    while index < stop:
        batch_end = min(index + 100, stop)
        while index < batch_end:
            target_time = started + sent_unique / args.rate
            time.sleep(max(0, target_time - time.monotonic()))

            row = rows[index]
            send(row)
            if should_duplicate(args.seed, row["transaction_id"], args.duplicate_rate):
                send(row)
            sent_unique += 1
            index += 1

        # Chỉ tiến cursor sau khi Kafka xác nhận cả chunk.
        # Nếu dừng giữa chunk, replay có thể gửi lại tối đa 100 row;
        # consumer phải idempotent theo transaction_id.
        confirm()
        state["next_index"] = index
        atomic_state(args.cursor, state)
        print(f"confirmed next_index={index}/{len(rows)}", flush=True)

    print(f"Done: {sent_unique} unique source rows this invocation")


if __name__ == "__main__":
    main()
