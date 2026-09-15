# ADR-002: Stream Processing Engine Selection

**Status:** Accepted  
**Date:** 2026-09-15  

## Context
ValoStream transforms continuous raw event streams into Streaming Data Vault 2.0 structures (Hubs, Links, Satellites) in real time. This requires stateful stream processing, exactly-once processing guarantees, sub-second processing latency, deterministic hashing ($MD5$ Hash Keys, HashDiff), stateful deduplication, and direct Iceberg table sink commitments.

## Decision
Adopt **Apache Flink** as the core stream compute and transformation engine.

## Alternatives Considered
- **Spark Structured Streaming**: Micro-batch execution introduces higher latency overhead and checkpoint latency compared to Flink's native pipelined stream execution. Spark is better optimized for large batch and micro-batch operations rather than sub-second continuous event transformation and low-latency feature extraction.
- **Kafka Streams**: Lightweight and embedded, but lacks native distributed resource management, advanced stateful CEP/SQL capabilities at massive multi-tenant scale, and mature two-phase commit sinks into Apache Iceberg.
- *Note on Apache Fluss*: Apache Fluss is often mentioned alongside Flink, but Fluss is a **streaming storage** tier (columnar, updatable stream store with changelog tracking), not a compute engine. Fluss is designed to work *alongside* Flink (e.g. as a real-time mutable storage buffer in front of Iceberg/lakehouse tables), rather than replacing Flink. Its role as streaming storage is evaluated in the architecture's lakehouse storage acceleration strategy.

## Consequences
- Requires operating dedicated Flink JobManagers and TaskManagers, with RocksDB state backend checkpointed to object storage.
- Delivers battle-tested Flink SQL and DataStream APIs with robust exactly-once Apache Iceberg sink integration.
- Enables sub-second end-to-end streaming transformation for real-time Data Vault ingestion and feature extraction.
