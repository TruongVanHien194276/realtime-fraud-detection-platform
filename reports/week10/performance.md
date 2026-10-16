# Week 10 local benchmark and hardening evidence

Measured on an Apple M3 Mac with 8 GiB host RAM and an approximately 3.83 GiB Docker VM. Spark ran `local[2]` with a 1 GiB driver for batch, a 1.5 GiB driver for streaming, and a 2.637 GiB container cap. Tests were **sequential**, not a combined full-stack load test. PaySim is simulated data. Every warehouse/checkpoint listed below is isolated or explicitly preserved for the restart test.

## Batch cold runs

| Input | Bronze rows | Silver valid | Quarantine | Bronze wall | Silver wall | Combined wall | Combined rows/s | Max sampled Spark RAM |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 100k | 100,000 | 100,000 | 0 | 37.50 s | 34.74 s | 72.24 s | 1,384 | Not measured reliably; one early Silver sample only |
| 1m | 1,000,000 | 1,000,000 | 0 | 288.56 s | 97.13 s | 385.69 s | 2,593 | 1,408 MiB Bronze; 1,252 MiB Silver |

Both runs passed `spark/verify_lakehouse.py`: Bronze = valid + quarantine, zero duplicate Bronze IDs, zero valid/quarantine overlap. The 1m input SHA-256 is in [the structured batch report](batch-1m-r1.json); the [100k report](batch-100k-r1.json) holds its source hash and per-stage timings. The 100k resource sample was too early to estimate a peak. The `1m` RAM figures are maxima among Docker stats samples, not process-memory peaks; CPU sampling on Docker Desktop may show transient values above the configured two-CPU cap. Source terminal logs remain local under ignored `data/tmp/week10/`.

The full 6.36m-row raw benchmark was not repeated in Week 10: the 1m cold run was enough to demonstrate integration scale on the 8 GiB machine, and a full rerun would consume much more time and disk. Earlier Week 2 full-data evidence is documented separately in `docs/lakehouse-design.md`, using a different workload/code state and **not** included in the table above.

## Streaming burst replay

Each run used the same 1,000-row Silver-derived fixture, one Kafka partition, `maxOffsetsPerTrigger=200`, a 10-second trigger, a new checkpoint/warehouse, seed 42, and 2% intentional duplicate sends. The producer withholds row 0 for a late-event probe; therefore **999 unique source rows** is the expected steady replay output. The offered EPS column is the producer configuration, not measured sustained platform capacity.

| Offered EPS | Unique Silver IDs | p50 processing delay | p95 | p99 | Max sampled Spark RAM |
|---|---:|---:|---:|---:|---:|
| 100 | 999 | 27,766 ms | 41,667 ms | 49,266 ms | Not sampled across the full run |
| 500 | 999 | 35,599 ms | 71,706 ms | 82,363 ms | 1,265 MiB (4 samples) |
| 1,000 | 999 | 29,517 ms | 49,003 ms | 59,392 ms | 1,165 MiB (6 samples) |

The p50/p95/p99 values are `processing_delay_ms` from producer `ingested_at` until the **start of the Spark micro-batch**. They exclude Iceberg commit, rules decisions, and alert publication, so they are not end-to-end fraud-decision latency. Runs were short bursts; the 1,000-EPS p95 happened to be below 500-EPS p95 because scheduling and batch boundaries varied. Do not infer sustained 1,000-EPS capacity or a latency SLA from these results. Structured artifacts: [100 EPS](stream-100-r1.json), [500 EPS](stream-500-r1.json), [1,000 EPS](stream-1000-r1.json). Resource samples: [500](stream-500-resources.json), [1,000](stream-1000-resources.json).

Sparse, read-only Spark backlog observations used broker partition-0 high watermark minus the end offset of the latest committed Spark checkpoint batch: **21 records during** the 1,000-EPS run, **0 after drain**. These are not a peak-lag time series. The Grafana consumer-group lag panel belongs to the rules consumer, not Spark. See [offset samples](spark-backlog-samples.json).

## Failure and correctness checks

- Stream restart reused the original 1,000-EPS checkpoint/warehouse/stream ID. Ten duplicate source rows produced zero new Silver rows. Late, unsupported-schema, and malformed probes produced three invalid records; independent verifier returned 999/999 unique IDs, nine completed batches, all three DLQ reasons, and `PASS`. [Machine-readable evidence](stream-failure.json).
- PostgreSQL was stopped after Gold held 100,000 unique IDs. The incoming quality gate exited non-zero and persisted `WAREHOUSE_UNAVAILABLE`. After restart it passed and Gold remained 100,000 unique IDs. [Before](pg-before-quality.json), [paused](pg-paused-quality.json), [recovered](pg-recovered-quality.json).
- Hybrid API slow-inference timeout and saturation are covered by `tests/unit/test_week10_api_timeout.py`; the local thread bound is not process isolation.

## API sequential loopback benchmark

The rebuilt API reported `model_loaded=true`, `fraud-risk-safe` alias `champion` version `2` before measurement ([model info](api-model-info.json)). Each run used 10 unique-ID warmup requests and 200 measured requests, one Uvicorn worker, local loopback, Redis available, no concurrent clients. Each request ID differed to avoid scoring-cache hits. Percentiles cover successful HTTP requests, including any degraded rule-only responses.

| Run | HTTP 200 / measured | Degraded | p50 | p95 | p99 | Successful RPS |
|---|---:|---:|---:|---:|---:|---:|
| [1](api-run1.json) | 200/200 | 0 | 19.88 ms | 42.56 ms | 57.08 ms | 41.23 |
| [2](api-run2.json) | 200/200 | 7 | 46.19 ms | 158.10 ms | 1,070.24 ms | 12.02 |
| [3](api-run3.json) | 200/200 | 0 | 21.08 ms | 60.76 ms | 84.33 ms | 36.22 |
| [4](api-run4.json) | 200/200 | 0 | 17.72 ms | 25.13 ms | 36.36 ms | 52.85 |

Across four runs: 800/800 HTTP successes, seven degraded responses, zero HTTP errors. The seven are **not** model successes; the initial benchmark script recorded only the degraded count, not reason codes, so their exact cause is unknown. Reason-code capture was added before run 4, which had none. Run 2 is an unexplained outlier and is retained rather than discarded. The 250 ms model-inference deadline is not a deadline on queueing, JSON parsing, rule evaluation, network transport, or the full HTTP request; therefore a client p99 above 250 ms is possible. See [sampled API resources](api-resources.json) for the first three runs. This is not a concurrent-load test or production SLA.

## Known limitations

One Redpanda broker/partition/replica; no HA. Kafka publication is at-least-once and Iceberg Silver is idempotent by transaction ID, not end-to-end exactly-once. API hybrid scoring is a separate path from the Kafka rules worker. Power BI Cloud report/PDF remains unverified until a real cloud artifact is published; the curated workbook and Grafana/local validation are not substitutes. The report is local-laptop evidence, not production sizing guidance.
