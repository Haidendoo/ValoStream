# ADR-010: Serving API Runtime Selection

**Status:** Accepted
**Date:** 2026-09-15

## Context
The public-facing recommendation and prediction API orchestrates requests across the online feature store (Redis), vector database (Qdrant), and model inference server (Triton). The total end-to-end response time budget is strict (p99 < 50ms), requiring a high-concurrency, low-overhead gateway runtime.

## Decision
Adopt **Go (`net/http`)** for the high-performance serving API service.

## Alternatives Considered
- **FastAPI (Python)**: Python's Global Interpreter Lock (GIL) limits concurrency, resulting in higher per-request overhead. While `uvloop` improves async throughput, it cannot close the latency gap required for tight 50ms p99 SLA targets.
- **Node.js (Express / Fastify)**: Single-threaded event-loop architecture can experience tail latency spikes under heavy JSON serialization and multi-backend fan-out operations.

## Consequences
- Requires writing more explicit boilerplate code and data struct definitions compared to Python web frameworks.
- Delivers sub-millisecond framework overhead with minimal garbage collection pauses.
- Lightweight Goroutines allow handling high concurrent request volumes while consistently meeting p99 latency targets.
