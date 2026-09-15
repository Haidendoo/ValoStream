# ADR-003: Lakehouse Table Storage Format Selection

**Status:** Accepted
**Date:** 2026-09-15

## Context
The lakehouse layer must store raw and Data Vault 2.0 entities on object storage while providing full ACID transactional guarantees, concurrent read/write isolation, schema evolution, hidden partition evolution, and multi-engine interoperability across Flink, StarRocks, Spark, and dbt.

## Decision
Adopt **Apache Iceberg** as the open lakehouse table format standard across all storage tiers.

## Alternatives Considered
- **Delta Lake**: High maturity but features tighter coupling to the Databricks ecosystem and historically lagged in vendor-neutral open-source governance and multi-engine catalog support.
- **Apache Hudi**: Complex index and file management (timeline server, record-level indexing) introduces unnecessary operational overhead for pure append/upsert Data Vault streaming patterns.

## Consequences
- Requires routine maintenance jobs for table compaction, orphan file cleanup, and snapshot expiration.
- Provides vendor-agnostic open specification with robust schema and partition evolution without data rewriting.
- Delivers seamless multi-engine support with native streaming sinks in Apache Flink and vectorized reads in StarRocks.
