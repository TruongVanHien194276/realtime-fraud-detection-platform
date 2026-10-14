import argparse
import json
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import psycopg
import redis
from confluent_kafka import Consumer, Producer
from psycopg.rows import dict_row

from fraud_platform.contracts import FraudDecisionEvent, TransactionEvent
from fraud_platform.features import build_features
from fraud_platform.rules import evaluate, load_rules


def pg_connection():
    return psycopg.connect(
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        dbname=os.environ["PGDATABASE"],
        autocommit=True,
        row_factory=dict_row,
    )


def history(conn, run_id: str, tx: TransactionEvent, entity: str) -> list[dict]:
    if entity not in {"customer", "recipient"}:
        raise ValueError(entity)

    column = "customer_id" if entity == "customer" else "recipient_id"
    entity_id = tx.customer_id if entity == "customer" else tx.recipient_id

    # Cặp (event_time, transaction_id) tạo thứ tự xác định cả khi
    # nhiều PaySim events có chung một giờ mô phỏng.
    query = f"""
        SELECT transaction_id, customer_id, recipient_id, event_time, amount
        FROM fraud_ops.feature_events
        WHERE run_id = %s
          AND {column} = %s
          AND event_time >= %s
          AND (event_time, transaction_id) < (%s, %s)
    """
    return conn.execute(
        query,
        (
            run_id,
            entity_id,
            tx.event_time - timedelta(hours=24),
            tx.event_time,
            tx.transaction_id,
        ),
    ).fetchall()


def cache_event(cache, run_id: str, tx: TransactionEvent, features: dict) -> None:
    # Redis là hot replica, không phải nguồn sự thật để chống trùng.
    customer_key = f"fraud:w5:{run_id}:customer:{tx.customer_id}"
    recipient_key = f"fraud:w5:{run_id}:recipient:{tx.recipient_id}"
    event_score = tx.event_time.timestamp()
    cutoff = event_score - 26 * 3600

    pipe = cache.pipeline()
    for key in (customer_key, recipient_key):
        pipe.zadd(key, {tx.transaction_id: event_score})
        pipe.zremrangebyscore(key, "-inf", cutoff)
        pipe.expire(key, 26 * 3600)
    pipe.setex(
        f"fraud:w5:{run_id}:last_features:{tx.customer_id}",
        26 * 3600,
        json.dumps(features),
    )
    pipe.execute()


def process_event(
    conn,
    cache,
    run_id: str,
    config: dict,
    tx: TransactionEvent,
    source_event_key: str,
    publish: bool,
) -> bool:
    rule_version = config["rule_version"]
    with conn.transaction():
        previous = conn.execute(
            """
            SELECT 1 FROM fraud_ops.decisions
            WHERE run_id = %s AND transaction_id = %s AND rule_version = %s
            """,
            (run_id, tx.transaction_id, rule_version),
        ).fetchone()
        if previous:
            return False

        customer_rows = history(conn, run_id, tx, "customer")
        recipient_rows = history(conn, run_id, tx, "recipient")
        features = build_features(tx, customer_rows, recipient_rows)
        feature_computed_at = datetime.now(UTC)
        result = evaluate(config, features)
        decided_at = datetime.now(UTC)

        decision_id = uuid5(
            NAMESPACE_URL,
            f"{run_id}:{tx.transaction_id}:{rule_version}",
        )
        latency_ms = max(0, int((decided_at - tx.ingested_at).total_seconds() * 1000))

        event = FraudDecisionEvent(
            schema_version="1.0",
            event_id=decision_id,
            run_id=run_id,
            transaction_id=tx.transaction_id,
            source_event_time=tx.event_time,
            feature_as_of_event_time=tx.event_time,
            feature_computed_at=feature_computed_at,
            decided_at=decided_at,
            decision=result["decision"],
            risk_score=result["risk_score"],
            triggered_rules=result["triggered_rules"],
            reason_codes=result["reason_codes"],
            model_version=None,
            rule_version=rule_version,
            feature_version=config["feature_version"],
            processing_latency_ms=latency_ms,
        ).model_dump(mode="json")

        # State event, decision và outbox cùng một DB transaction.
        conn.execute(
            """
            INSERT INTO fraud_ops.feature_events
              (run_id, transaction_id, customer_id, recipient_id,
               event_time, amount)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, transaction_id) DO NOTHING
            """,
            (
                run_id,
                tx.transaction_id,
                tx.customer_id,
                tx.recipient_id,
                tx.event_time,
                tx.amount,
            ),
        )

        inserted = conn.execute(
            """
            INSERT INTO fraud_ops.decisions
              (run_id, transaction_id, rule_version, decision_event_id,
               source_event_key, source_event_time, feature_as_of_event_time,
               feature_computed_at, decided_at, feature_version, decision,
               risk_score, amount, customer_id, triggered_rules, reason_codes,
               features, processing_latency_ms)
            VALUES
              (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
               %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s)
            ON CONFLICT (run_id, transaction_id, rule_version) DO NOTHING
            RETURNING transaction_id
            """,
            (
                run_id,
                tx.transaction_id,
                rule_version,
                decision_id,
                source_event_key,
                tx.event_time,
                tx.event_time,
                feature_computed_at,
                decided_at,
                config["feature_version"],
                result["decision"],
                result["risk_score"],
                tx.amount,
                tx.customer_id,
                json.dumps(result["triggered_rules"]),
                json.dumps(result["reason_codes"]),
                json.dumps(features),
                latency_ms,
            ),
        ).fetchone()

        if inserted is None:
            return False

        if publish:
            decision_key = f"{tx.transaction_id}:{rule_version}"
            conn.execute(
                """
                INSERT INTO fraud_ops.outbox
                  (id, run_id, topic, message_key, payload)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    f"{run_id}:decision:{decision_key}",
                    run_id,
                    "fraud.decisions",
                    decision_key,
                    json.dumps(event),
                ),
            )

            if result["decision"] in {"REVIEW", "BLOCK"}:
                alert = {
                    "schema_version": "1.0",
                    "run_id": run_id,
                    "decision_event_id": str(decision_id),
                    "transaction_id": tx.transaction_id,
                    "decision": result["decision"],
                    "risk_score": result["risk_score"],
                    "reason_codes": result["reason_codes"],
                    "created_at": decided_at.isoformat(),
                }
                conn.execute(
                    """
                    INSERT INTO fraud_ops.outbox
                      (id, run_id, topic, message_key, payload)
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        f"{run_id}:alert:{decision_key}",
                        run_id,
                        "fraud.alerts",
                        decision_key,
                        json.dumps(alert),
                    ),
                )

    # Nếu Redis lỗi sau DB commit, worker dừng trước khi commit Kafka offset.
    # Replay sẽ thấy decision trong DB; cache có thể dựng lại từ feature_events.
    cache_event(cache, run_id, tx, features)
    return True


def drain_outbox(conn, producer, run_id: str) -> int:
    published = 0
    errors: list[str] = []

    def delivered(error, _message):
        if error is not None:
            errors.append(str(error))

    while True:
        rows = conn.execute(
            """
            SELECT id, topic, message_key, payload
            FROM fraud_ops.outbox
            WHERE run_id = %s AND published_at IS NULL
            ORDER BY created_at, id
            LIMIT 100
            """,
            (run_id,),
        ).fetchall()
        if not rows:
            break

        errors.clear()

        for row in rows:
            producer.produce(
                row["topic"],
                key=row["message_key"],
                value=json.dumps(row["payload"]),
                callback=delivered,
            )

        remaining = producer.flush(30)
        if remaining or errors:
            raise RuntimeError(f"Outbox chưa publish xong: remaining={remaining}, errors={errors}")

        ids = [row["id"] for row in rows]
        conn.execute(
            """
            UPDATE fraud_ops.outbox
            SET published_at = now()
            WHERE id = ANY(%s)
            """,
            (ids,),
        )
        published += len(ids)

    return published


def offline_events(directory: Path):
    files = list(directory.glob("part-*"))
    if len(files) != 1:
        raise ValueError(f"Cần một part file, thấy {len(files)}")

    with files[0].open() as handle:
        for line_number, line in enumerate(handle, start=1):
            payload = json.loads(line)
            payload["event_id"] = str(uuid5(NAMESPACE_URL, payload["transaction_id"]))
            payload["ingested_at"] = datetime.now(UTC).isoformat()
            yield (
                TransactionEvent.model_validate(payload),
                f"offline:{directory.name}:{line_number}",
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--config", default="config/rules-v1.yaml")
    parser.add_argument("--group-id")
    parser.add_argument("--input-dir", type=Path)
    parser.add_argument("--idle-seconds", type=int, default=10)
    args = parser.parse_args()

    config = load_rules(args.config)
    conn = pg_connection()
    cache = redis.Redis.from_url(os.environ["REDIS_URL"])
    cache.ping()

    if args.input_dir:
        # Backtest offline: cùng code feature/rules, không phát Kafka output.
        count = 0
        for tx, source_key in offline_events(args.input_dir):
            if process_event(conn, cache, args.run_id, config, tx, source_key, publish=False):
                count += 1
            if count and count % 1000 == 0:
                print(f"offline decisions={count}", flush=True)
        print(f"offline new decisions={count}")
        return

    broker = os.environ["KAFKA_BOOTSTRAP"]
    producer = Producer(
        {
            "bootstrap.servers": broker,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": args.group_id or f"fraud-rules-{args.run_id}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    # Nếu lần chạy trước DB commit rồi dừng trước Kafka publish,
    # các message vẫn còn trong outbox.
    drain_outbox(conn, producer, args.run_id)
    consumer.subscribe(["transactions.validated"])
    last_message_at = time.monotonic()
    consumed = 0
    new_decisions = 0

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                if time.monotonic() - last_message_at >= args.idle_seconds:
                    break
                continue
            if message.error():
                raise RuntimeError(message.error())

            last_message_at = time.monotonic()
            tx = TransactionEvent.model_validate_json(message.value())
            source_key = f"{message.topic()}:{message.partition()}:{message.offset()}"
            if process_event(conn, cache, args.run_id, config, tx, source_key, publish=True):
                new_decisions += 1

            # Chỉ commit offset khi DB transaction và Redis update đã xong.
            consumer.commit(message=message, asynchronous=False)
            consumed += 1
            if consumed % 50 == 0:
                drain_outbox(conn, producer, args.run_id)
                print(
                    f"consumed={consumed} new_decisions={new_decisions}",
                    flush=True,
                )
    finally:
        consumer.close()
        drain_outbox(conn, producer, args.run_id)
        conn.close()

    print(f"done consumed={consumed} new_decisions={new_decisions}")


if __name__ == "__main__":
    main()
