# Week 5 — point-in-time features, rules and baseline

## Scope and resource budget

This local run uses the Week 4 `transactions.validated` topic, PostgreSQL for
durable event/decision/outbox state, and Redis as a small hot replica. On the
8 GB Mac, Compose caps PostgreSQL at 384 MB, Redpanda at 768 MB, Redis at
128 MB, and each rules worker at 384 MB. Do not keep the Spark job running
while evaluating rules. The single-node broker and database are for portfolio
demonstration, not a production availability design.

## Feature specification

`src/fraud_platform/features.py` computes all values from the incoming
transaction and **previous** events in the same `run_id`. Previous means
`(event_time, transaction_id)` is lexicographically smaller than the current
pair. The lower window boundary is inclusive; the current event and every
later event are excluded. Source `isFraud` and `isFlaggedFraud` are absent from
the `TransactionEvent` contract and never enter rule features.

| Feature | Meaning | Empty-history behavior |
|---|---|---|
| `customer_count_{5m,1h,24h}` / `customer_amount_sum_{5m,1h,24h}` | Number and total amount of prior customer transactions in the window | `0` / `"0"` |
| `recipient_count_{5m,1h,24h}` / `recipient_amount_sum_{5m,1h,24h}` | Prior transactions received by this recipient | `0` / `"0"` |
| `seconds_since_previous` | Event-time seconds since the most recent prior customer transaction | `null` |
| `customer_distinct_recipients_24h` | Unique recipients paid by the customer | `0` |
| `new_recipient_24h` | No prior customer payment to this recipient within 24 h | `true` |
| `amount_over_avg_24h` / `amount_over_median_24h` | Current amount divided by prior customer average/median | `null` if no history or denominator is zero |
| `amount_to_balance_ratio` | Current amount / incoming old origin balance | `null` if old balance is nonpositive |
| `recipient_fan_in_1h` | Distinct prior senders to this recipient | `0` |
| `customer_fan_out_1h` | Distinct prior recipients of this customer | `0` |

Money and ratios are kept as decimal strings in the feature JSON. The
current transaction's type and amount are permitted at decision time. Redis
stores recent IDs and last feature JSON with 26-hour TTL, but PostgreSQL
`fraud_ops.feature_events` is the source of truth for feature calculation and
deduplication. This avoids treating an evictable Redis cache as durable state.

## Rules and event correctness

`config/rules-v1.yaml` holds the versioned rule IDs, reason codes, hard/soft
severity, weights and policy thresholds. A hard hit blocks with score `1`;
soft weights sum to a score capped at `1`. Scores below `review_at` approve,
scores at or above `review_at` review, and scores at or above `block_at` block.
Change the YAML, not Python, to test a threshold; use a new `rule_version` for
a new policy in a comparable run.

The worker writes `feature_events`, `decisions` and the Kafka outbox in one
PostgreSQL transaction. Its decision key is `(run_id, transaction_id,
rule_version)` and UUIDv5 ID is deterministic. Kafka offset is committed only
after DB and Redis update. On restart, the worker drains the outbox before
reading new records. Outbox publish and marking `published_at` are not atomic
with Kafka, so downstream consumers still need to deduplicate by the stable
message key. Every decision retains the feature JSON, rule IDs, reason codes,
source event reference, feature timestamps and processing latency for audit.

This is point-in-time safe with respect to events *already seen by this run*.
Out-of-order arrival can omit an earlier event not yet ingested; it cannot
introduce a later event into an earlier score. An offline backtest should feed
events in event-time order to get reproducible historical features. PaySim
event time is simulated hourly, so equal-time events use transaction ID as a
deterministic tie-breaker, not proof of their true within-hour order.

## Reproduce on the Week 4 smoke data

Run from the repository root. The Week 3 Silver 100k warehouse and Week 4
validated topic must already exist. Build images and apply the idempotent
schema once (or again after a schema update):

```bash
docker compose --profile warehouse --profile streaming --profile rules up -d postgres redpanda redis
docker compose --profile rules build rules
docker compose --profile warehouse exec -T postgres psql -v ON_ERROR_STOP=1 -U fraud -d fraud_dw < sql/warehouse/004_rules.sql
```

Process the validated topic. Rerunning with the same `run_id` should add zero
new decisions even if Kafka replays duplicate records:

```bash
docker compose --profile rules run --rm rules \
  python -m fraud_platform.rules_worker \
  --run-id week5-smoke-v1 --config config/rules-v1.yaml --idle-seconds 10
```

Simulate labels becoming visible seven days after their Silver event time.
`--as-of` is a simulated UTC clock, not today's wall-clock date. The label
table is separate from online features. `--publish` queues `fraud.labels` in
the outbox; `--drain-only` actually publishes any pending outbox records.
Both commands are safe to rerun with the same `run_id`:

```bash
docker compose --profile rules run --rm rules \
  python scripts/simulate_labels.py \
  --run-id week5-smoke-v1 --rule-version rules-v1 \
  --as-of 2025-01-09T00:00:00Z --publish
docker compose --profile rules run --rm rules \
  python scripts/simulate_labels.py --run-id week5-smoke-v1 --drain-only
```

Evaluate only labels observed by that simulated date. The SQL prints label
coverage, the alert confusion matrix, precision, recall and illustrative
cost. `REVIEW` and `BLOCK` are alerts; `APPROVE` is non-alert. All metrics
and cost use only the labeled subset; `total_decisions` shows coverage.

```bash
docker compose --profile warehouse exec -T postgres \
  psql -v ON_ERROR_STOP=1 -v run_id=week5-smoke-v1 \
  -v rule_version=rules-v1 -v as_of=2025-01-09T00:00:00Z \
  -U fraud -d fraud_dw < sql/warehouse/005_rule_baseline.sql
```

## Verified local result and interpretation

For the 999 unique transactions in the Week 4 smoke stream, 999 have delayed
labels by `2025-01-09T00:00:00Z`: 2 fraud and 997 legitimate. Decisions are
301 APPROVE, 665 REVIEW and 33 BLOCK. As alerts, TP=2, FP=696, FN=0,
TN=301, precision=0.0029 and recall=1.0000. All 2 fraud cases were REVIEW,
not BLOCK; all 33 BLOCK decisions were false blocks. The assumed cost is
`0 missed fraud amount + 5 × 665 reviews + 10 × 33 false blocks = 3655`.
This is a comparison metric only: PaySim amounts are simulated, while the
5/10 cost coefficients are hypothetical business assumptions from
`docs/business-requirements.md`. They are not measured dollars or profit.

The 100% recall is not evidence of a strong policy: it comes with 698 alerts
for 999 transactions and only two positive labels. It is particularly
unstable because the smoke fixture contains only two fraud cases. Week 7
should compare ML with this rule-only baseline on a larger, time-separated
evaluation set and report alert capacity and uncertainty.

An independent offline verification used `--run-id week5-verification-v1` and
the 1000-row `data/tmp/week4/replay_1000` input (including the row deliberately
withheld from the streaming smoke run). It produced 1000 decisions; rerunning
produced 0 new decisions. At `2025-01-08T03:00:00Z`, 68 delayed labels were
eligible. At `2025-01-09T00:00:00Z`, all 1000 were eligible: 932 additional
labels/outbox records were inserted, `--drain-only` published all 1000, and
a repeat label run inserted/queued 0. Its alert precision was 0.0029, recall
1.0000, and illustrative cost 3665. This verifies the pipeline but does not
make the small sample statistically representative.

`event_time` and `label_observed_at` are on PaySim's virtual 2025 clock;
`ingested_at` and `decided_at` are actual replay wall-clock timestamps. Do not
subtract them to claim a real seven-day operational delay. The simulator
demonstrates temporal label availability on the virtual clock only.
