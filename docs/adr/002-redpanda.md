# ADR 002: Redpanda as local Kafka-compatible broker

Status: accepted for local portfolio build.

Context: replay producers, Spark, and rules consumers need a durable event log and explicit offsets without a multi-node infrastructure stack on a laptop.

Decision: run one Redpanda container and Kafka-compatible topics through Compose.

Alternatives: Apache Kafka would be representative but heavier locally; an in-process queue would not demonstrate broker offsets, consumer lag, or replay semantics.

Consequences: simple local setup and Kafka client compatibility. One broker, one partition, and replica factor one provide no high availability and do not represent production throughput.
