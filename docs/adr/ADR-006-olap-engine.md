# ADR-006: Real-Time OLAP Engine Selection

**Status:** Accepted
**Date:** 2026-09-15

## Context
ValoStream serves real-time dashboards, executive BI, and ad-hoc analytical queries over Data Vault 2.0 PIT (Point-in-Time) and Bridge views. The serving engine must query Apache Iceberg tables with sub-second response times using a modern vectorized execution engine and external catalog integration.

## Decision
Adopt **StarRocks** as the real-time MPP OLAP serving engine.

## Alternatives Considered
- **Apache Doris**: Predecessor project. Rejected because Doris has lagged in Iceberg external catalog optimizations, exhibits a slower release cadence for lakehouse features, and codebase divergence has fragmented the surrounding tooling.
- **ClickHouse**: High-performance columnar database, but lacks native external multi-table catalog federation for Iceberg and Apache Nessie.

## Consequences
- Requires dedicated compute resources (Frontend and Backend nodes) with significant memory allocation for vectorized execution.
- Delivers first-class Iceberg external catalog support and direct query execution on Nessie-managed tables without ingesting data into local storage.
- Delivers sub-second query latency on complex multi-table joins across Data Vault PIT and Bridge constructs.
