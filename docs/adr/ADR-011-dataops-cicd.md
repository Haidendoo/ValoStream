# ADR-011: DataOps and CI/CD Framework Selection

**Status:** Accepted
**Date:** 2026-09-15

## Context
To prevent corrupt or invalid data from leaking into production, ValoStream adopts automated DataOps practices. Every schema evolution, transformation model, and Data Vault structure must be automatically validated in isolated Git and Nessie branch environments prior to merging.

## Decision
Adopt **GitHub Actions + dbt Core** for automated DataOps CI/CD workflows and data quality gates.

## Alternatives Considered
- **Custom Bash/Python Scripts**: Brittle, difficult to maintain across teams, and lack standardized declarative testing primitives for data validation.
- **Standalone Heavy Quality Frameworks (e.g. Great Expectations)**: Redundant operational overhead and extra orchestration layers when dbt Core natively provides declarative testing directly within the SQL modeling workflow.

## Consequences
- dbt Core is batch-oriented, requiring coordinated workflow execution against streaming lakehouse tables and PIT views.
- GitHub Actions provides native pull request workflow integration, orchestrating branch-isolated environments via Apache Nessie.
- dbt Core provides declarative test definitions (uniqueness, referential integrity, not-null) that map directly to Data Vault quality gates.
