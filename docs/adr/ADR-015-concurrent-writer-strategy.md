# ADR-015: Concurrent Writer Strategy for Iceberg + Nessie

**Status:** Accepted  
**Date:** 2026-09-17  

## Context
ValoStream runs **continuous streaming writes** (PyFlink → Iceberg via Nessie) alongside periodic **batch maintenance** (compaction, snapshot expiration). Apache Iceberg uses **Optimistic Concurrency Control (OCC)** — if two writers modify the same table metadata concurrently, the slower writer receives a `CommitFailedException` and must retry.

In high-throughput streaming (10k+ events/sec), unmanaged concurrent writes can cause:
1. **Commit conflicts** between streaming writer and compaction jobs (both modify manifest lists).
2. **Retry storms** where conflicting commits cascade into repeated failures.
3. **Data staleness** if compaction blocks the streaming writer's checkpoint commits.

## Decision
Adopt a **Namespace-Isolated + Side-Branch Compaction** strategy:

### 1. Namespace Isolation (Streaming Writes)
The PyFlink streaming job is the **sole writer** to all Data Vault 2.0 tables on the Nessie `main` branch. No other process writes to these tables concurrently. This eliminates intra-table write conflicts entirely.

### 2. Side-Branch Compaction (Maintenance)
Maintenance operations (bin-pack compaction, manifest rewriting) run on a **dedicated Nessie branch**:

```
main ─────────────────────────► (streaming writes continue uninterrupted)
  │
  └── maintenance/compact-YYYYMMDD ──► compact ──► merge back to main
```

- **Create branch:** `nessie branch create maintenance/compact-YYYYMMDD --ref main`
- **Run compaction** on the branch (no conflict with streaming on `main`).
- **Merge back:** `nessie merge maintenance/compact-YYYYMMDD --into main`
  - If merge conflicts: Nessie resolves via content-merge (compacted files replace original files).
  - If streaming wrote new data during compaction: new data is preserved; only pre-compaction files are replaced.
- **Delete branch:** `nessie branch delete maintenance/compact-YYYYMMDD`

### 3. Snapshot Expiration (Metadata-Only)
Snapshot expiration (`expire_snapshots`) is a **metadata-only** operation that does not rewrite data files. It runs directly on `main` after the streaming writer's checkpoint completes. Risk of conflict is minimal because it only removes old snapshot references.

## Alternatives Considered
- **Per-Table Serialization (Lock Queue):** Forces compaction to wait for streaming quiescence window. Rejected: introduces unnecessary downtime and complexity; not needed when Nessie branching provides conflict-free isolation.
- **Flink-Internal Compaction (Iceberg Flink Actions):** Run compaction within the same Flink job. Rejected: consumes TaskManager slots that should serve streaming throughput; mixes operational concerns with ingestion logic.
- **Accept Conflicts + Exponential Backoff:** Let conflicts happen and retry with backoff. Rejected: under sustained 100k events/sec load, retry storms can cascade and cause checkpoint timeouts.

## Consequences
- **Zero unrecoverable merge conflicts** in 24-hour continuous operation (Nessie branch isolation guarantees this).
- Compaction runs independently without pausing or slowing the streaming pipeline.
- Requires Nessie branch management (create → compact → merge → delete) in the maintenance script/DAG.
- Nessie merge semantics handle the "new data arrived during compaction" case automatically.
