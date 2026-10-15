# dbt quality gate and first consumption marts

`analytics/` contains ten dbt models for the six Iceberg tables currently produced
by the streaming job. Data Vault supplies integration/history; materialized native
StarRocks tables supply the initial consumption layer.

| Layer | Models | Grain |
| --- | --- | --- |
| Staging | `stg_users`, `stg_merchants`, `stg_couriers` | One hash/business-key mapping, earliest observed timestamp |
| Staging | `stg_interaction_links` | One interaction/user/merchant mapping |
| Staging | `stg_interactions` | One interaction; exact payload replays removed |
| Staging | `stg_courier_telemetry` | One courier/timestamp; exact payload replays removed |
| Business Vault | `current_courier_positions` | Latest position per courier, selected before any status filtering |
| Mart | `fct_interactions` | One interaction with user and merchant identifiers |
| Mart | `mart_h3_demand_hourly` | Hour, H3 cell, resolution (7 or 9), event type |
| Mart | `mart_user_merchant_affinity` | User/merchant pair, cumulative interactions and purchases |

The current append-only ingestion repeats hub/link records. Grouping preserves
conflicting business-key mappings so uniqueness tests reject them; it does not
silently choose an arbitrary mapping. Conflicting interaction payloads or courier
payloads at the same timestamp also fail. This normalizes consumption but does not
fix raw-vault ingestion uniqueness. `geom_wkb` and `record_source` are not part of
the consumption payload; source record provenance is still checked for nulls.

The H3 mart stores dwell sums and sample counts so downstream aggregation can
compute a weighted average. Each resolution counts eligible interactions
separately; summing both resolutions together double-counts events. Centroids are
means of event coordinates, not geographic H3 cell centers. `event_at` uses the
existing satellite `load_date`, which the current Flink job populates from event
time. These models are not a bitemporal PIT implementation.

## Local checks

Use Python 3.12 from the repository root:

```bash
python3 -m venv .venv-analytics
.venv-analytics/bin/python -m pip install -r analytics/requirements-integration.txt
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.analytics-ci.yml up -d --wait starrocks
.venv-analytics/bin/python -m unittest discover -s tests/analytics -v
.venv-analytics/bin/python -m ops.check_analytics
PYTHONPATH=. .venv-analytics/bin/python tests/analytics/nessie_smoke.py
```

The native fixture check creates uniquely named source/output databases and drops
them on exit. It verifies exact expected mart values, then deliberately injects
four corruptions: conflicting hub key, orphan link, ambiguous courier timestamp,
and wrong source column type. The command succeeds only when the expected dbt
data tests fail; a connection or SQL execution error does not count as rejection.

The Nessie smoke test writes synthetic Iceberg/Parquet data on a temporary branch
in a unique namespace, clones that branch, builds and tests the same models
through a branch-scoped StarRocks catalog, and checks mart values. It then deletes
the temporary catalogs, output schema, both branches, and its own S3 fixture
directory. It never writes tables to `main`. The local `warehouse` bucket is
created if absent and retained.

## Gate on an existing lakehouse snapshot

Once `main.vault` contains the six source tables:

```bash
.venv-analytics/bin/python -m ops.nessie_quality_gate --ref main
```

The runner resolves the source ref to a hash, creates a unique Nessie branch at
that hash, and mounts it as a unique StarRocks catalog. dbt reads those Iceberg
tables and writes only to a unique native StarRocks database. All temporary
resources are discarded even when dbt fails; no branch merge occurs. An existing
resource whose creation failed is never registered for deletion. A frozen commit
does not make independently committed source tables transactionally consistent;
select a completed ingestion snapshot when using this gate on a live pipeline.

Configuration uses `STARROCKS_HOST`, `STARROCKS_PORT`, `STARROCKS_USER`,
`STARROCKS_PASSWORD`, `NESSIE_URL` (host-facing), `STARROCKS_ICEBERG_URL` and
`STARROCKS_S3_ENDPOINT` (reachable from StarRocks), `S3_ACCESS_KEY`,
`S3_SECRET_KEY`, and `ICEBERG_WAREHOUSE`. Defaults match the local Compose stack.
`--source-schema` selects an alternate Iceberg namespace. The local catalog uses
explicit MinIO credentials and disables vended credentials, so StarRocks data-file
reads use the configured MinIO endpoint.

## Build persistent development marts

With a configured StarRocks catalog `nessie_main`:

```bash
DBT_SOURCE_CATALOG=nessie_main DBT_SOURCE_SCHEMA=vault \
  DBT_OUTPUT_SCHEMA=valostream_analytics \
  .venv-analytics/bin/dbt build --project-dir analytics --profiles-dir analytics
```

All models use dbt `table` materialization and rebuild on each successful run.
They are persisted tables, not automatically refreshed materialized views.
There is no installed refresh scheduler or freshness SLA yet. A future five-minute
schedule is the target for current positions and interaction/H3/affinity marts;
until scheduled and measured, freshness is only as recent as the last build.
The StarRocks adapter drops/recreates tables, so builds are not atomic publication
of the whole model graph. Use an isolated output schema for validation; deployment
must add validated publication before exposing these tables to live serving.
The existing serving API has not been switched to these marts.

## CI and validation

`.github/workflows/analytics.yml` runs on model, source schema, streaming schema,
gate, test, and Compose changes. It starts StarRocks 4.1.4, Nessie, and MinIO,
executes the native and branch-backed checks, uploads results, and tears down
services. It requires no production credentials. The event-contract workflow
continues checking Kafka schema compatibility independently.

Validated locally on 2026-10-15 with dbt Core 1.12.6, dbt-StarRocks 1.12.2,
StarRocks 4.1.4, and PyIceberg 0.12.0:

- Ten models and 97 deterministic data tests passed against native fixtures and
  real Nessie/Iceberg/MinIO sources.
- All four corruption scenarios were rejected by their intended tests.
- Seven unit tests cover resource ownership, cleanup after failures, and identifiers.
- Fixture assertions confirm three interactions despite replayed input, correct
  H3 counts/dwell totals, latest courier positions despite out-of-order input, and
  affinity totals.

This is correctness validation on tiny synthetic datasets, not a latency/load
benchmark. Production raw-vault deduplication, bitemporal PIT, async refresh,
statistical monitoring on a seven-day baseline, PII erasure, and atomic serving
publication remain pending. GitHub-hosted execution of the new workflow has not
been observed yet.

References: [dbt-StarRocks adapter](https://github.com/StarRocks/dbt-starrocks),
[StarRocks MinIO configuration](https://docs.starrocks.io/docs/loading/objectstorage/minio/).
