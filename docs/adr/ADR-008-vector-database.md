# ADR-008: Vector Database Selection

**Status:** Accepted
**Date:** 2026-09-15

## Context
The real-time recommender system requires fast Approximate Nearest Neighbor (ANN) vector search to retrieve candidate items based on deep user and item embeddings. The vector database must support high QPS, metadata payload filtering (e.g., category, availability), and a lightweight operational footprint.

## Decision
Adopt **Qdrant** as the dedicated vector database for embedding retrieval and candidate generation.

## Alternatives Considered
- **Milvus**: Powerful distributed vector database, but rejected due to high architectural complexity requiring multiple external distributed dependencies (etcd, MinIO, Pulsar/Kafka), imposing a heavy operational burden.
- **StarRocks Vector Search**: StarRocks offers basic vector indexing, but ANN search is secondary to its OLAP engine, lacking specialized HNSW tuning, payload filtering flexibility, and dedicated vector lifecycle operations.

## Consequences
- Adds an additional specialized vector database service to maintain alongside relational and OLAP engines.
- Provides a lightweight single-binary deployment with clean gRPC and REST APIs.
- Supports rich metadata payload filtering and high-performance HNSW index search within sub-10ms latency SLAs.
