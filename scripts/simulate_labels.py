import argparse
import json
import os
from datetime import datetime

from confluent_kafka import Producer

from fraud_platform.contracts import FraudLabelEvent
from fraud_platform.rules_worker import drain_outbox, pg_connection


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--rule-version", default="rules-v1")
    parser.add_argument("--as-of", help="UTC ISO timestamp; required unless --drain-only")
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--drain-only", action="store_true")
    args = parser.parse_args()

    if args.drain_only:
        if args.as_of or args.publish:
            parser.error("--drain-only cannot be combined with --as-of or --publish")
    elif not args.as_of:
        parser.error("--as-of is required unless --drain-only is used")

    simulated_now = None
    if args.as_of:
        simulated_now = datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
        if simulated_now.utcoffset() is None:
            parser.error("--as-of must include a timezone, for example 2025-01-09T00:00:00Z")

    conn = pg_connection()
    if args.drain_only:
        producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"]})
        print(f"published={drain_outbox(conn, producer, args.run_id)}")
        conn.close()
        return

    rows = conn.execute(
        """
        SELECT d.transaction_id,
               s.is_fraud_actual AS label,
               s.event_time + interval '7 days' AS label_observed_at
        FROM fraud_ops.decisions d
        JOIN warehouse_raw.transactions_silver s
          ON s.transaction_id = d.transaction_id
        WHERE d.run_id = %s
          AND d.rule_version = %s
          AND s.event_time + interval '7 days' <= %s
        """,
        (args.run_id, args.rule_version, simulated_now),
    ).fetchall()

    inserted = 0
    queued = 0
    for row in rows:
        with conn.transaction():
            result = conn.execute(
                """
                INSERT INTO fraud_ops.labels
                  (run_id, transaction_id, label,
                   label_observed_at, label_source)
                VALUES (%s, %s, %s, %s, 'paysim_simulated_chargeback')
                ON CONFLICT (run_id, transaction_id) DO NOTHING
                RETURNING transaction_id
                """,
                (
                    args.run_id,
                    row["transaction_id"],
                    row["label"],
                    row["label_observed_at"],
                ),
            ).fetchone()
            if result is not None:
                inserted += 1
            if args.publish:
                payload = FraudLabelEvent(
                    run_id=args.run_id,
                    transaction_id=row["transaction_id"],
                    label=row["label"],
                    label_observed_at=row["label_observed_at"],
                    label_source="paysim_simulated_chargeback",
                ).model_dump(mode="json")
                queued += conn.execute(
                    """
                    INSERT INTO fraud_ops.outbox
                      (id, run_id, topic, message_key, payload)
                    VALUES (%s, %s, 'fraud.labels', %s, %s::jsonb)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        f"{args.run_id}:label:{row['transaction_id']}",
                        args.run_id,
                        row["transaction_id"],
                        json.dumps(payload),
                    ),
                ).rowcount

    print(f"eligible={len(rows)} newly_inserted={inserted} newly_queued={queued}")
    conn.close()


if __name__ == "__main__":
    main()
