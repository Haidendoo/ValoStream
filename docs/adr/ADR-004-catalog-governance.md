# ADR-004: Lakehouse Catalog and Governance Selection

**Status:** Accepted
**Date:** 2026-09-15

## Context
Managing Iceberg tables across environments requires a centralized, high-concurrency catalog. To support modern DataOps practices, the platform requires Git-like data versioning (branches, tags, zero-copy cloning, atomic commits) to test transformations on isolated branches before promoting changes to production.

## Decision
Adopt **Apache Nessie** as the lakehouse catalog and data versioning service.

## Alternatives Considered
- **Unity Catalog**: Databricks-centric governance platform that introduces ecosystem lock-in and lacks fine-grained Git-like branch-per-PR workflows.
- **Hive Metastore (HMS)**: Legacy relational architecture lacking Git-like branching, commit histories, and multi-table transactional semantics.
- **Apache Polaris**: Snowflake-backed emerging open-source catalog, but less mature OSS ecosystem and lacks Git-like branching capabilities.

## Consequences
- Introduces an independent Nessie catalog service dependency that requires management and persistent backend storage.
- Enables zero-copy branch-per-PR DataOps pipelines, allowing automated dbt quality checks in complete isolation before merging.
- Guarantees atomic merge and instant rollback across related tables in case of upstream data quality failures.
