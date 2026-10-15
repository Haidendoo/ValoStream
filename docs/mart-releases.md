# Managed mart refresh and publication

`python -m ops.refresh_marts` builds a frozen Nessie snapshot into a new native
StarRocks schema, runs the complete dbt quality gate and serving SQL probes, then
publishes the schema through a local JSON manifest. APIs configured with
`STARROCKS_RELEASE_FILE` read that pointer once per request. A failed build leaves
the previous release serving; an in-flight request keeps its selected schema.

This implementation targets Linux/POSIX processes on **one host**, using the same
local filesystem, manifest path, and StarRocks cluster. It is not a distributed
release coordinator. API workers may finish requests from the old release while
new requests use the new one.

## Start and operate

Use Python 3.12 from the repository root. Nessie, MinIO, and StarRocks must be
running, and the selected source ref/namespace must contain the six source tables.
Connection settings match the [analytics runbook](analytics-quality-gate.md).
The runner creates its own temporary StarRocks catalog; it does not require a
preconfigured `nessie_main` catalog.

```bash
.venv-analytics/bin/python -m pip install -r analytics/requirements.txt -r serving/requirements.txt
release_manifest="$PWD/var/releases/active.json"

# Build once, validate, and publish. Nonzero exit means the operation failed.
.venv-analytics/bin/python -m ops.refresh_marts publish \
  --manifest "$release_manifest" --ref main --source-schema vault

# Run the API in another terminal with the same absolute manifest path.
STARROCKS_RELEASE_FILE="$release_manifest" \
  .venv-analytics/bin/python -m uvicorn serving.app:app --host 0.0.0.0 --port 8000
```

Keep `STARROCKS_MART_SCHEMA` unset when using the manifest. The manifest is created
with owner-only permissions; run the API and publisher as the same service user.
If using containers on that host, mount the containing directory into the API
with write access for lease files, not an individual manifest file: publication
replaces the file's inode. Every API worker and collector must share the same
directory and the same lease-file inodes on the host's local filesystem.

To refresh periodically, run this command under your process supervisor:

```bash
.venv-analytics/bin/python -m ops.refresh_marts watch \
  --manifest "$release_manifest" --ref main --source-schema vault \
  --interval-seconds 300
```

The loop attempts a build immediately, then waits 300 seconds **after completion**
before the next attempt. It logs failures and retries on the next interval. SIGINT
or SIGTERM requests exit after the current build finishes; it does not interrupt
an in-progress publication. dbt has a ten-minute subprocess timeout. No daemon or
system service is installed automatically by these commands.

Inspect the active/previous release, source hashes, and timestamps:

```bash
.venv-analytics/bin/python -m ops.refresh_marts status --manifest "$release_manifest"
curl --fail http://localhost:8000/healthz
```

Stop the refresh loop before a rollback so the next scheduled build cannot replace
your rollback. The rollback command checks the previous release's tables and
serving queries, then swaps active and previous:

```bash
.venv-analytics/bin/python -m ops.refresh_marts rollback --manifest "$release_manifest"
```

Rollback requires a prior successful publication. It does not rebuild data or
rerun tests against a moving source. A second rollback swaps the two releases
again. Successfully published tables must remain immutable.

## Publication guarantees and limits

1. A nonblocking `flock` on `active.json.lock` serializes builds, rollbacks, and pruning.
   Another writer using the same manifest fails before building. Keep the lock
   file in place and use a single canonical manifest path for every writer.
2. The runner resolves the source ref to a hash and creates a temporary Nessie
   branch/catalog at that hash. It never merges the branch. A frozen hash does not
   make independent upstream table commits transactional; choose a completed
   ingestion snapshot when cross-table consistency is required.
3. Every attempt gets a new `vs_release_<uuid>` schema. `dbt build` runs all ten
   models and their data tests, followed by queries covering the serving paths.
   Ordinary build/validation failures drop only that attempt's output. Temporary
   source resources are cleaned before publication.
4. The manifest records active, previous, and retired releases owned by this pointer.
   Writing uses a temporary file in the same directory, file `fsync`, `os.replace`,
   then directory `fsync`. Readers see a complete old or new document. If an I/O
   error occurs after replacement, the new pointer may already be visible; inspect
   `status` before retrying. Validated output is never dropped after publication
   begins, even if the final durability step fails.
5. Missing, malformed, or unsupported manifests cause data endpoints to return
   503. The API does not silently switch to raw data. `/metrics` remains available.
   `/healthz` reports the selected schema and optional freshness checks described below.

## Retention and request protection

A new publication retires the former `previous` release and records its retirement
time. Active and previous schemas are always protected. The collector only considers
entries in this manifest's retired list; it never scans database-name prefixes to
infer ownership. Grace is measured from retirement, not build completion, so a
long-lived active release is not collected immediately after being replaced.

```bash
# Preview eligible releases; no database is dropped by default.
.venv-analytics/bin/python -m ops.refresh_marts prune \
  --manifest "$release_manifest" --retention-seconds 86400

# Apply the same policy explicitly.
.venv-analytics/bin/python -m ops.refresh_marts prune \
  --manifest "$release_manifest" --retention-seconds 86400 --apply

# Opt into cleanup after each successful periodic publication.
.venv-analytics/bin/python -m ops.refresh_marts watch \
  --manifest "$release_manifest" --interval-seconds 300 \
  --prune-retired --retention-seconds 86400
```

The default grace is one day; the minimum is five minutes. Each managed API request
holds a shared OS file lock for its selected schema until its dependency exits,
including query errors. The collector requires a nonblocking exclusive lock and
reports `in_use` instead of dropping a busy schema. Readers recheck the pointer
after acquiring a lease to handle selection races. Competing publishers/collectors
use the writer lock; request leases do not block publication.

`prune` reports `grace_period`, `in_use`, `eligible` (preview), or `deleted`. A failed
drop keeps the ownership record for retry. A failed manifest write after a successful
drop also remains retryable through `DROP DATABASE IF EXISTS`. Drops use no `FORCE`;
physical disk reclamation follows the StarRocks trash-retention policy, so it is not
immediate. See [StarRocks data recovery](https://docs.starrocks.io/docs/administration/Data_recovery/).

Enable pruning only after every consumer uses this manifest and the updated lease
protocol. Static-schema clients, copied manifests, independently configured readers,
and external SQL sessions are not protected. Do not repurpose or manually rebuild
managed schema names. Keep lease files under `active.json.leases/` in place: deleting
a locked file could split readers across different inodes. Empty lease files and dbt
artifacts are retained; this command manages database schemas only. Unknown orphan
schemas from process crashes/publication failures require separate inspection.

Manifest format is now **v2**. New code reads v1 manifests, treating absent watermarks
as unknown and absent history as empty. Deploy new readers first, then the publisher,
and drain old workers before enabling pruning. Old API versions reject v2. Versions
older than the previous release in a legacy manifest cannot be reconstructed as
owned and are not automatically deleted.

## Freshness and alerts

Each newly built release captures `MAX(load_date)` for interactions and courier
telemetry from its validated staging tables. These are latest observed event times,
not Flink processing watermarks or proof that every partition is caught up. Empty
streams have null timestamps. A newly rebuilt snapshot can still contain old data.

The producer supplies epoch milliseconds; the streaming job now explicitly sets
`table.local-time-zone=UTC` when converting `TIMESTAMP_LTZ` to vault `TIMESTAMP`.
The runner interprets StarRocks `DATETIME` values as UTC. Historical data written
under a different timezone must be normalized before using event-age thresholds.
See [Flink timezone conversion](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/table/timezone/).

Set thresholds on the API process, for example:

```bash
STARROCKS_RELEASE_FILE="$release_manifest" \
STARROCKS_MAX_BUILD_AGE_SECONDS=1800 \
STARROCKS_MAX_EVENT_AGE_SECONDS=900 \
  .venv-analytics/bin/python -m uvicorn serving.app:app --host 0.0.0.0 --port 8000
```

Both limits default to `0` (disabled) and require manifest mode when enabled.
`/healthz` reports build age and per-stream event ages, returning 503 for an enabled
limit that is exceeded, an unknown required timestamp, or a timestamp over five
minutes in the future. Otherwise freshness is `ok`, or `unchecked` if both limits
are disabled. The data endpoints continue serving the selected release; route
traffic according to readiness in your deployment. Quiet streams can exceed event
age thresholds without an ingestion fault, so choose limits for expected traffic.
This is a configurable check, not a measured freshness SLA.

`/metrics` exports manifest validity, build and event timestamps, watermark-known
flags, configured limits, and `valostream_release_freshness_healthy`. Only the fixed
`interactions`/`couriers` stream labels are used. Scrapes recompute these metrics
without database queries; missing manifests omit old timestamp samples and report
invalid status. Prometheus loads `deploy/prometheus/alerts.yml`, with two-minute
pending periods for API unavailability, invalid manifests, and failed freshness
checks. Alerts are visible in Prometheus; external notification routing requires
an Alertmanager setup and is not configured by this change.

Artifacts are stored under `analytics/target/releases/<schema>/` (override with
`--artifacts`). `built_at` records build completion, not the latest event time.
The refresh delay is a scheduling setting, not a measured data-freshness SLA.
Multi-host coordination, notification routing, and incremental materialization
remain future work.

## Validation

```bash
.venv-analytics/bin/python -m pip install -r analytics/requirements-integration.txt -r serving/requirements-test.txt
.venv-analytics/bin/python -m unittest discover -s tests/analytics -v
.venv-analytics/bin/python -m unittest discover -s tests/serving -v
make -C deploy release-check PYTHON="$PWD/.venv-analytics/bin/python"
```

Unit tests cover serialization, ownership, rejected builds, publication failures,
rollback, periodic retry, retention ownership/grace/leases, freshness, and request
consistency during a pointer change. The
integration check creates real Iceberg fixtures on an isolated Nessie branch,
publishes three releases, verifies event timestamps and collection after a request
lease is released, rejects a conflicting business-key mapping, and verifies API
rollback without restarting the application. Its
schemas, branches, and fixture files are removed on exit. CI uploads dbt results
from `analytics/target/release-smoke/`.

Alert rules have executable cases in `tests/monitoring/releases.test.yml`:

```bash
docker run --rm --entrypoint promtool -v "$PWD:/workspace:ro" -w /workspace \
  prom/prometheus:latest test rules tests/monitoring/releases.test.yml
```

Filesystem primitives: [Python `os.replace`](https://docs.python.org/3/library/os.html#os.replace)
and [Python `fcntl.flock`](https://docs.python.org/3/library/fcntl.html#fcntl.flock).
