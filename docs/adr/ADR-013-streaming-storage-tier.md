# ADR-013: Streaming Storage Tier (Apache Fluss) Evaluation & Synergy with Flink

**Status:** Proposed / Under Evaluation  
**Date:** 2026-09-15  

## Context
In high-throughput streaming architectures (target 10,000–100,000 events/sec), committing streaming updates directly to Apache Iceberg at short checkpoint intervals (e.g., 2 seconds) produces severe operational side effects:
1. **Small-File Explosion & Manifest Churn:** Generating thousands of Parquet files and commits per day degrades Nessie metadata performance and inflates object storage GET/LIST costs.
2. **High Latency for Point Lookups / Freshness:** Querying Iceberg tables requires scanning files, which cannot support true sub-second streaming point lookups or low-latency streaming joins.

**Apache Fluss** (incubating) is designed specifically as a unified streaming storage system that complements **Apache Flink**. Instead of replacing Flink (which is a compute engine), Fluss provides real-time streaming storage with:
- Sub-second append and update capabilities with primary key changelog tracking.
- Low-latency streaming reads, writes, and point lookups directly for Flink and query engines.
- Native lakehouse tiering: buffering real-time streaming updates in Fluss and automatically sinking them into Apache Iceberg / Paimon in compacted batches (e.g., every 5–15 minutes).

## Decision
1. **Initial Baseline (Step 2 Walking Skeleton):** Retain direct Flink → Iceberg writes with a relaxed checkpoint interval (30–60 seconds) and scheduled Iceberg compaction jobs (`rewrite_data_files`, `expire_snapshots`) to keep initial infrastructure complexity manageable.
2. **Evaluation Milestone (Step 1.2 Spike & Step 3):** If the Nessie commit ceiling and file compaction benchmarks reveal unacceptable commit latency, high retry rates, or query degradation under 100k events/sec, deploy **Apache Fluss** as the real-time mutable streaming storage tier between Flink and Iceberg.

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
