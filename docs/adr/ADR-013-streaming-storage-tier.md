# ADR-013: Streaming Storage Tier (Apache Fluss) Integration & Synergy with Flink

**Status:** Accepted  
**Date:** 2026-09-16  

## Context
In high-throughput streaming architectures (target 10,000–100,000 events/sec), committing streaming updates directly to Apache Iceberg at short checkpoint intervals (e.g., 2 seconds) produces severe operational side effects:
1. **Small-File Explosion & Manifest Churn:** Generating thousands of Parquet files and commits per day degrades Nessie metadata performance and inflates object storage GET/LIST costs.
2. **High Latency for Point Lookups / Freshness:** Querying Iceberg tables requires scanning files, which cannot support true sub-second streaming point lookups or low-latency streaming joins.

**Apache Fluss** (incubating) is designed specifically as a unified streaming storage system that complements **Apache Flink**. Instead of replacing Flink (which is a compute engine), Fluss provides real-time streaming storage with:
- Sub-second append and update capabilities with primary key changelog tracking.
- Low-latency streaming reads, writes, and point lookups directly for Flink and query engines.
- Native lakehouse tiering: buffering real-time streaming updates in Fluss and automatically sinking them into Apache Iceberg / Paimon in compacted batches (e.g., every 5–15 minutes).

## Decision
Adopt **Apache Fluss** (0.9.1) alongside **Apache Iceberg** in a dual-tier storage topology:
1. **Streaming Storage Tier (Fluss):** Serves real-time, mutable sub-second point lookups (`courier_telemetry_live` updatable PK table) and append logs (`clickstream_realtime_log`), buffering high-frequency pings without creating small files in the lakehouse.
2. **Lakehouse Storage Tier (Iceberg via Nessie):** Flink sinks transactional Data Vault 2.0 tables (`hub_*`, `link_*`, `sat_*`) to Iceberg Parquet files on MinIO/S3 using 10s–60s checkpoints for historical analytics, compliance, and vectorized OLAP via StarRocks.

```
[Kafka Ingestion] ──► [Apache Flink Compute]
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
      [Apache Fluss]              [Redis Feature Store]
  (Streaming Storage Tier:                (<5ms SLA)
   Sub-second updates/lookups)
              │
              ▼ (compaction / batch landing)
   [Apache Iceberg Lakehouse] (Nessie Catalog)
```

## Alternatives Considered
- **Direct Iceberg 2-second Checkpoints:** Causes small-file avalanche (43k commits/day) and manifest churn; rejected as unviable under continuous streaming load.
- **Apache Paimon:** Similar streaming lakehouse storage concept with changelog tables, but Fluss offers tighter co-design with Flink for unified stream-lake tiering.
- **Kafka for all state lookups:** Kafka lacks indexed primary key lookups and column-oriented projection needed for efficient streaming feature extraction.

## Consequences
- **If Fluss is deployed:**
  - Adds a dedicated streaming storage cluster to manage.
  - Eliminates small-file metadata storms on Iceberg and Nessie by decoupling real-time ingestion from lakehouse commits.
  - Enables sub-second streaming lookups for Data Vault deduplication and current satellite views.
- **If postponed:**
  - Keeps initial operational footprint leaner for MVP, relying on relaxed Iceberg checkpoints and scheduled background compaction.
