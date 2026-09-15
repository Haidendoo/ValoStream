# ADR-007: Online Feature Store Selection

**Status:** Accepted
**Date:** 2026-09-15

## Context
Real-time recommendation and scoring models require sub-millisecond retrieval of online features (user rolling statistics, item interaction counts, context vectors) at high queries-per-second (QPS). The online feature store must be directly sinkable from Apache Flink streaming pipelines and integrate with feature store frameworks like Feast.

## Decision
Adopt **Redis** as the online low-latency feature store.

## Alternatives Considered
- **Dragonfly**: Modern multi-threaded Redis-compatible in-memory store. Rejected due to being newer, having fewer enterprise production references at scale, and potential protocol edge-case incompatibilities with streaming connectors.
- **Apache Cassandra / ScyllaDB**: Strong write throughput, but higher P99 read latencies (5–15ms) compared to Redis in-memory lookups (<1ms), violating the tight latency budget.

## Consequences
- Requires managing in-memory RAM capacity, implementing strict TTLs, and setting memory eviction policies.
- Leverages a battle-tested ecosystem, universal client library support, and the mature Apache Flink Redis connector.
- Delivers reliable sub-millisecond feature lookup latency under heavy concurrent inference workloads.
