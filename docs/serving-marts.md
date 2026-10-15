# Serving validated marts

The FastAPI service supports two sources, selected once at process startup:

| Configuration | Source |
| --- | --- |
| `STARROCKS_MART_SCHEMA` unset or empty | Existing raw vault: `STARROCKS_CATALOG.STARROCKS_VAULT_SCHEMA` (defaults `nessie_main.vault`) |
| `STARROCKS_MART_SCHEMA=<release_schema>` | Completed dbt tables in `default_catalog.<release_schema>` |

Both modes use `STARROCKS_HOST`, `STARROCKS_PORT`, `STARROCKS_USER`, and
`STARROCKS_PASSWORD`, defaulting to `localhost:9030`, `root`, and an empty password
for local development. Catalog/schema identifiers must start with a letter or
underscore, contain only letters, digits, and underscores, and be at most 64
characters. Invalid identifiers fail startup.

## Local start

From the repository root, use Python 3.12 and an existing StarRocks catalog with
the six source tables documented in the [analytics runbook](analytics-quality-gate.md):

```bash
python3 -m venv .venv-analytics
.venv-analytics/bin/python -m pip install -r analytics/requirements.txt -r serving/requirements.txt

# Choose a fresh schema for every build. The API must not read it during the build.
release_schema="valostream_release_$(date -u +%Y%m%d_%H%M%S)"
DBT_SOURCE_CATALOG=nessie_main DBT_SOURCE_SCHEMA=vault \
  DBT_OUTPUT_SCHEMA="$release_schema" \
  .venv-analytics/bin/dbt build --project-dir analytics --profiles-dir analytics && \
STARROCKS_MART_SCHEMA="$release_schema" \
  .venv-analytics/bin/python -m uvicorn serving.app:app --host 0.0.0.0 --port 8000
```

The `&&` starts the API only if all models and data tests succeed. Alternatively,
omit `STARROCKS_MART_SCHEMA` to run the API against the raw vault.

Models use dbt `table` materialization, which drops/recreates tables. Never rebuild
the schema used by running API workers. Build and validate a new schema, configure
new workers to use it, check `/healthz`, and switch traffic using your deployment
system. Keep the old schema while old workers drain and for rollback. A rolling
deployment can temporarily serve both snapshots. Automatic promotion, refresh,
retention, and atomic traffic switching are not implemented.

For a stable input, build against a catalog pinned to a completed Nessie snapshot.
Reading the mutable `nessie_main` catalog during ingestion does not guarantee that
all models see the same source snapshot. The isolated quality-gate command cleans
up its output; its temporary schema must not be used as a serving release.

## Endpoint behavior

- `/spatial/couriers` returns one latest position per courier. Status filtering
  happens after latest-row selection, so an older matching status cannot reappear.
- `/spatial/hexagons` supports resolution **7 or 9**; other values return 422.
  Interaction counts exclude exact replays. Mart rollups weight coordinate means
  by event count and dwell means by non-null sample count. Courier counts and
  `active_couriers` count the latest known positions, including idle couriers;
  they do not measure recent activity or impose a freshness cutoff.
- `/recommendations` ranks merchants by interaction count divided by
  `1 + distance_meters / 2000`, rounded to two decimals, before applying `limit`.
  Distance is measured to the mean interaction location, not a merchant address.
  H3 filtering uses the resolution-7 neighborhood and falls back globally only
  when that neighborhood has no results. `user_id` is echoed for compatibility;
  scoring is based on popularity and location, without user personalization.
- `/stats` retains the six legacy table keys. Values are raw row counts in vault
  mode and normalized staging counts in mart mode, so these counts can differ.
- `/healthz` checks that all tables needed by the active source can be resolved
  through StarRocks and reports `source: marts` or `source: vault`. Failure returns
  503. It does not scan source data files, run quality tests, or check freshness.
- `/metrics` records request counts by route/status and latency by route. Query
  strings and user identifiers are excluded from metric labels.

Database errors return generic 503 responses; detailed exceptions stay in server
logs. Connections close on success, failure, and before recommendation fallback.
Invalid coordinates, entities, statuses, and limits return 422 before any query.

Mart data is as recent as its last successful build. This change adds no scheduler
or measured freshness/latency SLA. Raw mode removes exact consumption replays,
but does not provide the mart quality gate's rejection of conflicting payloads.

## Verification

```bash
.venv-analytics/bin/python -m pip install -r serving/requirements-test.txt
make -C deploy serving-test PYTHON="$PWD/.venv-analytics/bin/python"
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.analytics-ci.yml up -d --wait starrocks
make -C deploy serving-check PYTHON="$PWD/.venv-analytics/bin/python"
```

Thirteen API/query unit tests cover input validation, error handling, connection
cleanup, fallback, readiness, ranking SQL, and request metrics. The integration
check creates two uniquely named native StarRocks databases, builds all ten dbt
models with 97 data tests, then exercises real HTTP responses in both source modes.
Fixtures include replayed hubs/links/events, out-of-order courier positions,
unequal H3 group sizes, and a more popular but distant merchant. Both temporary
databases are removed on success or failure. dbt results remain under
`analytics/target/api-smoke/`. These checks run in the analytics PR workflow and
validate correctness on synthetic data; they are not a load benchmark.
