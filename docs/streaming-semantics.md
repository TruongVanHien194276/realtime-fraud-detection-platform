# Week 4 — streaming ingestion semantics

## Scope and machine profile

This is a single-node development pipeline for an M3 Mac with 8 GB RAM. Compose
caps Redpanda at 768 MB, the replay producer at 256 MB, and Spark at 2700 MB.
Run PostgreSQL/dbt and the Spark streaming job at different times. The broker
uses one partition and replication factor one; this is **not** a production
availability configuration. Data and checkpoints stay outside Git.

```text
Week 2 Silver sample → deterministic NDJSON → replay producer
    → transactions.raw → Spark Structured Streaming foreachBatch
       ├─ invalid or new late event → transactions.invalid (DLQ)
       └─ new timely transaction → transactions.validated
                               → lake.silver.stream_transactions (Iceberg)
```

`fraud.decisions`, `fraud.alerts`, and `fraud.labels` are created but not
populated by Week 4. Rule/decision semantics are a Week 5 responsibility.

## Contract and identifiers

`spark/prepare_replay.py` reads the first 1000 rows of the existing Week 2
Silver 100k table ordered by `(event_time, transaction_id)`. It preserves the
batch `transaction_id`, which was already derived from the canonical PaySim raw
row and source dataset hash. It omits `isFraud` and `isFlaggedFraud`, maps
account names to `customer_id`/`recipient_id`, and serializes money as decimal
strings. The producer adds deterministic UUIDv5 `event_id` from
`transaction_id` and current UTC `ingested_at`; the Kafka key is `customer_id`.
JSON is explicitly parsed as schema version `1.0` by Spark. Unsupported
versions, missing/bad fields, extra fields, bad JSON, bad transaction type,
negative step/amount, malformed event ID, and mismatched Kafka key are rejected.

`source_event_key = topic:partition:offset` identifies the original Kafka
record, including in DLQ output. The DLQ message key is this identifier; its
value includes `error_code` and the original raw payload. Codes include
`MALFORMED_JSON`, `UNSUPPORTED_SCHEMA_VERSION`, `MISSING_OR_BAD_FIELD`,
`UNKNOWN_FIELD`, `INVALID_TRANSACTION_ID`, `INVALID_EVENT_ID`, `INVALID_STEP`,
`INVALID_AMOUNT`, `INVALID_TRANSACTION_TYPE`, `KAFKA_KEY_MISMATCH`, and
`LATE_EVENT`.

## Deduplication, lateness, and restart

The producer cursor is advanced atomically only after Kafka confirms a chunk
of at most 100 source rows. Stopping mid-chunk may resend those rows on resume.
`--seed` makes duplicate injection reproducible; a new cursor deliberately
replays the whole prepared input. The first prepared row is withheld from the
ordinary replay so it can be sent later as a genuinely new late event.

For each Spark micro-batch, parse/validate first; deduplicate by
`transaction_id` within the batch; anti-join against the Iceberg Silver table;
then apply the application-level watermark. Checking whether a transaction is
already known **before** checking lateness prevents old replays from polluting
the DLQ. The watermark is the maximum accepted `event_time` across previously
completed batches minus two hours, persisted in `lake.audit.stream_batches`.
It is not Spark's `withWatermark()` state-store operator: the explicit check
lets us route late events to the DLQ instead of silently dropping them. A
two-hour bound is a demonstration policy, not a PaySim business fact.

The checkpoint lives in `checkpoints/week4/<stream-id>`, outside the Iceberg
warehouse and output topic. On restart, reuse the same warehouse, checkpoint,
and stream ID. `startingOffsets` matters only for a *new* checkpoint. For an
independent benchmark over a topic that already has history, use a new
warehouse/checkpoint/stream ID plus `--starting-offsets latest`, and start the
consumer before the producer. Never reset a checkpoint while retaining its
audit table under the same stream ID.

`foreachBatch` writes Kafka outputs, performs insert-only Iceberg `MERGE` by
`transaction_id`, and appends the audit row last. If it fails before audit,
Spark can retry the batch. Iceberg rows remain unique, but Kafka outputs may
be published again. Both `transactions.validated` and
`transactions.invalid` are therefore **at-least-once** outputs; downstream
consumers must deduplicate by `transaction_id` or `source_event_key` as
appropriate. This is not an end-to-end exactly-once transaction.

## Verification performed on 2026-10-14

The smoke run used `data/tmp/week4/replay_1000`,
`data/tmp/week4/stream-lakehouse-v1`, and `checkpoints/week4/smoke-v1`.
The producer completed its cursor at `next_index=1000`: 999 ordinary source
transactions (row 0 reserved), plus deterministic duplicate messages. Spark
was stopped and restarted with the same checkpoint, and the source was also
replayed. The integration assertion reported 999 Silver rows and 999 distinct
transaction IDs. It found `LATE_EVENT`, `UNSUPPORTED_SCHEMA_VERSION`, and
`MALFORMED_JSON` in the DLQ, with matching Kafka key and source-event trace.
The smoke Silver latency was p50 176495 ms, p95 183820 ms after a backlog;
those numbers are not a steady-state service latency.

There were historical DLQ records from an earlier version that checked
lateness before deduplicating replayed transactions. Those records were not
deleted; the corrected code now filters known transactions first. Do not
interpret the historical DLQ count as the current error rate.

Independent 999-transaction benchmark runs used separate Iceberg warehouses
and checkpoints, `maxOffsetsPerTrigger=200`, 10-second processing trigger,
one Kafka partition, and the same 2% deterministic duplicate injection:

| Producer target | Producer wall time | Silver rows / distinct | p50 ingest-to-batch | p95 ingest-to-batch |
|---|---:|---:|---:|---:|
| 100 source rows/s | 11.6 s | 999 / 999 | 29118 ms | 47964 ms |
| 500 source rows/s | 3.6 s | 999 / 999 | 32356 ms | 50009 ms |

The configured rate applies to unique source rows, not the extra duplicate
messages. `processing_delay_ms` is from producer `ingested_at` to the start
of the Spark micro-batch's processing; it does **not** include the final Kafka
and Iceberg sink commit. The 500/s run raises backlog without improving sink
throughput. These measurements are one local run, not an SLA or statistically
representative benchmark.

## Reproduce the smoke assertion

Start Redpanda with `docker compose --profile streaming up -d redpanda`, and
initialize topics with `bash scripts/init_streaming_topics.sh`. Prepare the
input once with `spark/prepare_replay.py` against the Week 2 Silver 100k
warehouse:

```bash
docker compose run --rm spark spark-submit \
  --jars /opt/iceberg/iceberg-spark-runtime.jar \
  spark/prepare_replay.py \
  --warehouse data/tmp/week2-benchmark-100k-v1 \
  --output data/tmp/week4/replay_1000 --limit 1000
```

The output directory must not already exist. Start `spark/stream_transactions.py`
in a terminal and leave it running while producing events in a second terminal:

```bash
docker compose --profile streaming run --rm spark \
  spark-submit --driver-memory 1500m \
  --jars /opt/iceberg/iceberg-spark-runtime.jar \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
  --conf spark.jars.ivy=/workspace/data/tmp/ivy \
  spark/stream_transactions.py \
  --warehouse data/tmp/week4/stream-lakehouse-v1 \
  --checkpoint checkpoints/week4/smoke-v1 \
  --stream-id smoke-v1 --max-offsets 200
```

Use the same cursor in both producer calls; the second resumes at row 401:

```bash
docker compose --profile streaming run --rm producer \
  python src/producer/replay_transactions.py \
  --input data/tmp/week4/replay_1000 \
  --cursor data/tmp/week4/cursor-smoke-v1.json \
  --rate 100 --seed 42 --duplicate-rate 0.02 --max-records 400

docker compose --profile streaming run --rm producer \
  python src/producer/replay_transactions.py \
  --input data/tmp/week4/replay_1000 \
  --cursor data/tmp/week4/cursor-smoke-v1.json \
  --rate 100 --seed 42 --duplicate-rate 0.02
```

After Silver reaches 999 rows and Spark has processed the Kafka backlog, send
each probe separately with the same `--input` and `--mode late`, `--mode invalid`,
or `--mode malformed`. Stop Spark before starting the verification process on
an 8 GB machine:

```bash
docker compose --profile streaming run --rm spark \
  spark-submit --driver-memory 1500m \
  --jars /opt/iceberg/iceberg-spark-runtime.jar \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
  --conf spark.jars.ivy=/workspace/data/tmp/ivy \
  tests/integration/check_week4_stream.py \
  --warehouse data/tmp/week4/stream-lakehouse-v1 \
  --expected-valid 999 --require-dlq \
  --replay-dir data/tmp/week4/replay_1000
```

The check asserts unique transaction and event IDs, a source trace on Silver
and DLQ records, an actual duplicate in raw Kafka, the withheld first row in
the late DLQ but not Silver, three required DLQ codes, and at least one
completed audit batch. Its 999-row expectation assumes the documented
1000-row fixture with the first row withheld, not an arbitrary input dataset.
