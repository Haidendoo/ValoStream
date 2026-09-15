# ADR-012: Workflow Orchestration and ML Lifecycle Selection

**Status:** Accepted
**Date:** 2026-09-15

## Context
ValoStream requires reliable scheduling for asynchronous batch operations (Iceberg table compaction, snapshot expiration, Data Vault PIT/Bridge maintenance, automated model retraining) along with dedicated tooling for ML experiment tracking, model registry governance, and deployment tracking.

## Decision
Adopt **Apache Airflow** for DAG-based workflow scheduling and **MLflow** for machine learning lifecycle management as complementary solutions.

## Alternatives Considered
- **Prefect / Dagster**: Modern data workflow orchestrators, but with smaller ecosystems and fewer battle-tested connectors for enterprise data engineering tasks compared to Airflow.
- **Kubeflow**: Heavier Kubernetes infrastructure dependency with excessive operational complexity for a decoupled streaming lakehouse and serving architecture.

## Consequences
- Requires operating two distinct systems: Airflow for workflow DAG scheduling, and MLflow for experiment tracking and model registration.
- Airflow delivers an industry-standard scheduler with mature connector ecosystems and dependable SLA alerting.
- MLflow provides standardized model packaging and registry governance, seamlessly feeding model artifacts into Triton Inference Server.
