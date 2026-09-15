# ADR-001: Ingestion Bus Selection

**Status:** Accepted
**Date:** 2026-09-15

## Context
ValoStream requires a distributed, highly durable, low-latency streaming event bus to ingest real-time multi-domain data sources, including transactional CDC from Debezium, high-velocity clickstream events, and item metadata updates. The ingestion bus must guarantee partition ordering, high throughput, and reliable integration with stream processing engines.

## Decision
Adopt **Apache Kafka** as the standard real-time event ingestion and streaming bus.

## Alternatives Considered
- **Redpanda**: High-performance C++ Kafka-compatible streaming platform. Rejected due to a smaller ecosystem, fewer enterprise production references at scale, and a less battle-tested Apache Flink connector ecosystem compared to Kafka.

## Consequences
- Requires JVM resource allocation, garbage collection tuning, and cluster coordination management (ZooKeeper/KRaft).
- Guarantees native integration with Debezium CDC connectors and the official Apache Flink Kafka connector (`flink-connector-kafka`).
- Provides industry-standard client support, robust monitoring tooling, and proven horizontal scalability under heavy workloads.
