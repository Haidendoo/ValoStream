# ValoStream

> **Real-Time Streaming Lakehouse Platform with Data Vault 2.0, Git-like DataOps & End-to-End AI Recommender System**

ValoStream is an enterprise data platform that eliminates the gap between real-time event streaming and analytical lakehouse storage. It uses **Streaming Data Vault 2.0** for schema evolution resilience, **Apache Iceberg + Apache Nessie** for zero-copy Git-like data versioning, **StarRocks** for sub-second vectorized OLAP, and a **Two-Stage Recommender System** (Qdrant + Triton) for personalized user serving.

---

## 📚 Project Documentation & Blueprint

* 🏛️ **[Technical Proposal & Architecture Document (`archi.md`)](archi.md)**  
  Full architecture specification, high-level Mermaid dataflows, Data Vault 2.0 SQL schemas, low-latency recommender design, and H3 spatial analytics.

* 📋 **[Project Implementation Steps (`project_step.md`)](project_step.md)**  
  Actionable 22-week execution plan resolving all 31 architecture review findings through a walking-skeleton approach.

* 📜 **[Architecture Decision Records (`docs/adr/`)](docs/adr/)**  
  Documented architectural decisions with context, alternatives considered, and consequences:
  * [ADR-001: Ingestion Bus (Apache Kafka)](docs/adr/ADR-001-ingestion-bus.md)
  * [ADR-002: Stream Processing Engine (Apache Flink)](docs/adr/ADR-002-streaming-engine.md)
  * [ADR-003: Storage Standard (Apache Iceberg)](docs/adr/ADR-003-storage-format.md)
  * [ADR-004: Catalog & Governance (Apache Nessie)](docs/adr/ADR-004-catalog-governance.md)
  * [ADR-005: Object Storage (MinIO dev / AWS S3 prod)](docs/adr/ADR-005-object-storage.md)
  * [ADR-006: OLAP Engine (StarRocks)](docs/adr/ADR-006-olap-engine.md)
  * [ADR-007: Online Feature Store (Redis)](docs/adr/ADR-007-online-feature-store.md)
  * [ADR-008: Vector Database (Qdrant)](docs/adr/ADR-008-vector-database.md)
  * [ADR-009: Model Inference (Triton Inference Server)](docs/adr/ADR-009-model-inference.md)
  * [ADR-010: Serving API Gateway (Go net/http)](docs/adr/ADR-010-serving-api.md)
  * [ADR-011: DataOps CI/CD (GitHub Actions + dbt Core)](docs/adr/ADR-011-dataops-cicd.md)
  * [ADR-012: Orchestration & ML Governance (Airflow + MLflow)](docs/adr/ADR-012-orchestration-ml.md)
  * [ADR-013: Streaming Storage Tier Evaluation (Apache Fluss)](docs/adr/ADR-013-streaming-storage-tier.md)
  * [ADR-014: Geospatial Storage & H3 Indexing for Visualization](docs/adr/ADR-014-geospatial-h3-indexing.md)

* 🔬 **[Validation Spikes (`spikes/`)](spikes/)**  
  * [Spike 1.1: Branch-Scoped StarRocks Against Nessie Catalog](spikes/spike_1_1_starrocks_nessie/SPIKE_REPORT.md) — **Result: PASSED (GO)**, full PR lifecycle executes in <50ms with 0 memory leaks.

---

## ⚡ Core Architecture Stack

| Layer | Technology | Role | Decision |
|:------|:-----------|:-----|:---------|
| **Event Ingestion** | Apache Kafka | Distributed event bus for clickstream & CDC events | [ADR-001](docs/adr/ADR-001-ingestion-bus.md) |
| **Stream Compute** | Apache Flink & PyFlink | Stateful stream transformations, Python spatial UDFs (H3, Shapely WKB), MD5 HashKey/HashDiff | [ADR-002](docs/adr/ADR-002-streaming-engine.md) |
| **Streaming Storage (Buffer)** | Apache Fluss | Sub-second real-time streaming buffer (<1s latency) to eliminate Iceberg small files | [ADR-013](docs/adr/ADR-013-streaming-storage-tier.md) |
| **Lakehouse Storage** | Apache Iceberg | ACID transactions, hidden partitioning, schema evolution | [ADR-003](docs/adr/ADR-003-storage-format.md) |
| **Data Catalog & Git Versioning** | Apache Nessie | Zero-copy branching, branch-per-PR isolation, instant rollback | [ADR-004](docs/adr/ADR-004-catalog-governance.md) |
| **Object Storage** | MinIO / AWS S3 | S3-compatible Parquet file persistence | [ADR-005](docs/adr/ADR-005-object-storage.md) |
| **Vectorized OLAP** | StarRocks | Sub-second BI queries (<100ms) on PIT views & H3 spatial rollups | [ADR-006](docs/adr/ADR-006-olap-engine.md) |
| **Online Feature Store** | Redis | Low-latency feature serving (<5ms p99) for recommendation engine | [ADR-007](docs/adr/ADR-007-online-feature-store.md) |
| **Vector Search** | Qdrant | Fast ANN candidate retrieval (<15ms) for personalized recommendations | [ADR-008](docs/adr/ADR-008-vector-database.md) |
| **Inference Server** | Triton Inference Server | High-throughput GPU dynamic batching for DCNv2 ranking models | [ADR-009](docs/adr/ADR-009-model-inference.md) |
| **Serving API Gateway** | FastAPI / Go (`net/http`) | Spatial recommendation & Deck.gl H3 hexagon endpoints (<50ms SLA) | [ADR-010](docs/adr/ADR-010-serving-api.md) |
| **Spatial Analytics & Heatmap** | Apache Sedona + Uber H3 + Deck.gl | GeoParquet point storage, H3 hexagon indexing, GPU 3D visualization | [ADR-014](docs/adr/ADR-014-geospatial-h3-indexing.md) |
| **DataOps Quality Gate** | GitHub Actions + dbt Core | Automated data quality verification on isolated Nessie branches | [ADR-011](docs/adr/ADR-011-dataops-cicd.md) |
| **Orchestration & ML** | Apache Airflow + MLflow | Scheduled table maintenance, model retraining, and registry | [ADR-012](docs/adr/ADR-012-orchestration-ml.md) |

---

## 🚦 Current Progress & Roadmap Status

- [x] **Step 0: Document & Decision Fixes**
  - Resolved 5 Blockers & 17 Major findings from architecture review.
  - Published 14 ADRs across the technology stack.
  - Standardized Data Vault 2.0 hashing algorithm (§4.3) with UTF-8 `U+001F` delimiter.
  - Corrected CI merge semantics (branches discarded after test pass; code promoted, not data).
  - Integrated Geospatial H3 indexing and GeoParquet format.
- [x] **Step 1: Validation Spikes**
  - [x] **Spike 1.1:** StarRocks Iceberg REST branch integration benchmarked (**~49.6 ms** PR lifecycle, memory stable).
  - [x] **Spike 1.2–1.4:** Verified via Walking Skeleton integration tests.
- [x] **Step 2: Walking Skeleton (100% Complete & Verified)**
  - **Docker Compose Stack:** 13 operational services (Kafka, MinIO, Nessie, StarRocks, Flink JobManager/TaskManager, Redis, Qdrant, ZooKeeper, Fluss Coordinator & 3 Tablet Servers).
  - **PyFlink Dual-Tier Streaming DAG:** Real-time ingestion consuming Kafka `events.clickstream` and `events.telemetry`, generating uppercase MD5 keys, computing Uber H3 hexagonal discrete global grid indexes (`h3_res7`, `h3_res9`), and generating OGC-compliant GeoParquet WKB Point geometries via Shapely Python UDFs.
  - **Streaming Buffer & Lakehouse Vault:** Dual writes to **Apache Fluss** (<1s latency updatable PK table `courier_telemetry_live` + append log) AND **Apache Iceberg** via Nessie REST catalog (`hub_user`, `hub_merchant`, `hub_courier`, `link_user_merchant_interaction`, `sat_interaction_context_spatial`, `sat_courier_telemetry_spatial`).
  - **Vectorized OLAP & Serving API:** StarRocks external catalog query latency <50ms; FastAPI serving engine live with `/recommendations` (H3 neighborhood filtered), `/spatial/hexagons` (3D Deck.gl layer ready), and `/spatial/couriers` (live delivery telemetry).
- [ ] **Step 3: Operational Tier**
  - Scheduled Iceberg bin-pack compaction (`rewrite_data_files`), snapshot expiry, and SLI/SLO alerting.
- [ ] **Step 4: Quality & Governance**
  - Schema Registry, Crypto-shredding for GDPR, Business Vault & Information Marts.
- [ ] **Step 5: Feature Store & Recommender Engine**
  - Two-stage ranking pipeline (Qdrant ANN + Triton DCNv2) with streaming cold-start item embeddings.
- [ ] **Step 6: Chaos & Load Testing**
  - High-rate benchmark (100k events/sec) and JobManager failure recovery verification.