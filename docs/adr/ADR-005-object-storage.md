# ADR-005: Object Storage Layer Selection

**Status:** Accepted
**Date:** 2026-09-15

## Context
The lakehouse requires a highly scalable, cost-effective, and durable object storage layer to store Iceberg data files (Parquet), metadata files, and checkpoint snapshots across both local development environments and cloud production deployments.

## Decision
Adopt **MinIO** for local development and CI environments, and **AWS S3** for cloud production deployments.

## Alternatives Considered
- **Apache Ozone**: Distributed object store for on-premise big data clusters. Rejected due to a smaller community, less tooling integration, and unnecessary operational complexity for this use case.
- **HDFS**: Legacy distributed file system lacking object store semantics, immutable object write patterns, and cloud-native elastic scaling.

## Consequences
- Requires maintaining configuration profiles to switch between MinIO S3-compatible endpoints (development) and AWS S3 IAM credentials (production).
- Guarantees 100% S3 API compatibility between local container environments and production cloud infrastructure.
- Leverages AWS S3's industry-standard 11 9s durability, global availability, and mature ecosystem integrations.
