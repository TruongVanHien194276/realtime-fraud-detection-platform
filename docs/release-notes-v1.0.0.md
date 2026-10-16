# v1.0.0 — local portfolio release

This release packages the ten-week fraud-platform project for reproducible local review on an Apple M3, 8 GiB Mac. It is a portfolio release, not a production service certification.

## Included

- PySpark/Iceberg batch Bronze/Silver and Redpanda replay ingestion with restart/deduplication/DLQ checks.
- PostgreSQL/dbt Gold, rules decisions, temporal ML baseline, MLflow registry, and a separate FastAPI hybrid-scoring endpoint.
- Week 10 cold batch, streaming-burst, failure, and sequential API evidence in [`reports/week10`](../reports/week10/performance.md).
- [Architecture](architecture-final.md), [ADRs](adr/001-spark.md), [5–7 minute interview guide](interview-guide.md), [CV bullets](cv-bullets.md), [GIF evidence montage](../demo/week10-evidence.gif), and a synthetic-data [clean-checkout quickstart](../README.md).

## Verification boundary

Local lint, unit tests, Compose validation, streaming verifier, 1m-row lakehouse reconciliation, and a clean-checkout smoke run are recorded as release checks. GitHub Actions is configured but a hosted run is not claimed without a remote. Power BI Cloud publishing remains unverified; the curated snapshot and local BI validation are not a cloud report. The Kafka rules worker does not invoke the hybrid API on each event. Offered replay EPS is not sustained throughput, and measured processing delay stops at Spark batch start, not a fraud decision.

The source archive is made from the `v1.0.0` Git tag after the clean-checkout smoke test; ignored datasets, checkpoints, `.env`, local model stores, and Docker volumes are excluded. To inspect the measured scope and caveats, read the [performance report](../reports/week10/performance.md).
