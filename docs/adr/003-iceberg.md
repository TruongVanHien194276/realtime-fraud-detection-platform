# ADR 003: Iceberg for Bronze and Silver lakehouse tables

Status: accepted for local portfolio build.

Context: batch backfill and streaming replay need table-level metadata and insert-only merge keyed by transaction ID.

Decision: store Bronze and Silver as local Iceberg tables, with isolated warehouses for tests and benchmarks.

Alternatives: plain Parquet files would be simpler and smaller but would require custom table-commit and deduplication logic. Delta Lake is another viable format but is not used here.

Consequences: reproducible snapshots and idempotent merge semantics, with more metadata, small-file overhead, and Spark memory cost. Kafka publication remains at-least-once, so Iceberg idempotency is not an end-to-end exactly-once guarantee.
