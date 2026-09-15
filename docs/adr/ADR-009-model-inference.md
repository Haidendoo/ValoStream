# ADR-009: Real-Time Model Inference Server Selection

**Status:** Accepted
**Date:** 2026-09-15

## Context
ValoStream's recommendation and ranking pipeline requires high-throughput, low-latency execution of machine learning models (two-stage ranking, click-through rate prediction). The serving infrastructure must maximize hardware utilization (CPU and GPU), support dynamic batching, and serve models exported from various frameworks (PyTorch, ONNX, TensorRT).

## Decision
Adopt **Triton Inference Server** as the model serving runtime.

## Alternatives Considered
- **Ray Serve**: Excellent for distributed Python pipelines and combined training/serving workflows, but introduces heavyweight cluster orchestration overhead for dedicated inference serving.
- **FastAPI**: Constrained by the Python Global Interpreter Lock (GIL), lacks hardware-level dynamic batching, and is not production-grade for high-throughput ML inference at scale.

## Consequences
- Imposes stricter model export requirements (ONNX, TorchScript, TensorRT) and declarative configuration schemas.
- Provides production-grade dynamic batching, concurrent model execution, model versioning, and zero-downtime hot reloading.
- Achieves sub-millisecond p99 inference latency over standard gRPC and HTTP protocols.
