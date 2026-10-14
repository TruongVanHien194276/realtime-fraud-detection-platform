#!/usr/bin/env bash
set -euo pipefail

topics=(
  transactions.raw
  transactions.validated
  transactions.invalid
  fraud.decisions
  fraud.alerts
  fraud.labels
)

for topic in "${topics[@]}"; do
  docker compose --profile streaming exec -T redpanda \
    rpk topic create "$topic" \
    --partitions 1 \
    --replicas 1 \
    --if-not-exists
done

docker compose --profile streaming exec -T redpanda rpk topic list