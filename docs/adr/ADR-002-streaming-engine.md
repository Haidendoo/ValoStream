# ADR-002: Stream Processing Engine Selection

**Status:** Accepted  
**Date:** 2026-09-15  

## Context
ValoStream transforms continuous raw event streams into Streaming Data Vault 2.0 structures (Hubs, Links, Satellites) in real time. This requires stateful stream processing, exactly-once processing guarantees, sub-second processing latency, deterministic hashing ($MD5$ Hash Keys, HashDiff), stateful deduplication, and direct Iceberg table sink commitments.

## Decision
Adopt **Apache Flink & PyFlink (1.18)** as the core stream compute and transformation engine. PyFlink enables native integration of Python geospatial libraries (Uber H3 discrete global grid indexing, Shapely OGC WKB geometry generation) alongside Table API/SQL with concurrent sinks into both Apache Iceberg (Data Vault 2.0) and Apache Fluss (Streaming Buffer).

## Alternatives Considered
- **Spark Structured Streaming**: Micro-batch execution introduces higher latency overhead and checkpoint latency compared to Flink's native pipelined stream execution. Spark is better optimized for large batch and micro-batch operations rather than sub-second continuous event transformation and low-latency feature extraction.
- **Kafka Streams**: Lightweight and embedded, but lacks native distributed resource management, advanced stateful CEP/SQL capabilities at massive multi-tenant scale, and mature two-phase commit sinks into Apache Iceberg.
- *Synergy with Apache Fluss*: Apache Fluss is a **streaming storage** tier (columnar, updatable stream store with changelog tracking), not a compute engine. Fluss sits beside Flink as a real-time mutable storage buffer in front of Iceberg/lakehouse tables. Adopted in [ADR-013](ADR-013-streaming-storage-tier.md).

## Consequences
- Requires operating dedicated Flink JobManagers and TaskManagers with a Python 3 runtime for PyFlink worker processes.
- Delivers battle-tested Table API, Python UDFs, and robust exactly-once sinks into Apache Iceberg and Apache Fluss simultaneously.
- Enables sub-second end-to-end streaming transformation for real-time Data Vault ingestion, H3 indexing, and feature extraction.
