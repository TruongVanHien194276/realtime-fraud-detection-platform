# ADR 001: PySpark for batch and streaming

Status: accepted for local portfolio build.

Context: PaySim needs the same transaction schema in batch Bronze/Silver and event-time stream processing, with deduplication, watermarking, and reproducible jobs.

Decision: use Spark local[2] for batch and Structured Streaming. Iceberg writes and Kafka ingestion share the same Spark runtime.

Alternatives: DuckDB-only batch plus a separate Python consumer would use less memory but diverge from the distributed processing skills this project demonstrates. Flink would be a valid stream engine but add another runtime.

Consequences: consistent transformations and restartable checkpoints, at the cost of JVM startup, memory pressure, 10-second micro-batch latency, and limited throughput on an 8 GiB Mac. Benchmark results are in `reports/week10/`.
