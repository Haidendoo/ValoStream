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
read-only, not an individual file: publication replaces the file's inode.

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

1. A nonblocking `flock` on `active.json.lock` serializes builds and rollbacks.
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
4. The manifest records the new active release and the prior active release.
   Writing uses a temporary file in the same directory, file `fsync`, `os.replace`,
   then directory `fsync`. Readers see a complete old or new document. If an I/O
   error occurs after replacement, the new pointer may already be visible; inspect
   `status` before retrying. Validated output is never dropped after publication
   begins, even if the final durability step fails.
5. Missing, malformed, or unsupported manifests cause data endpoints to return
   503. The API does not silently switch to raw data. `/metrics` remains available.
   `/healthz` reports the selected schema, but does not enforce a freshness limit.

Successful output schemas are retained, including releases older than `previous`,
so long-running requests and rollback targets are not deleted by a refresh. Storage
therefore grows with each successful build. Automatic retention is not implemented;
inventory and remove obsolete schemas only after checking all consumers and draining
old requests. Keep active and previous releases. Process crashes or cleanup errors
can leave temporary resources; inspect logs before deleting any resource.

Artifacts are stored under `analytics/target/releases/<schema>/` (override with
`--artifacts`). `built_at` records build completion, not the latest event time.
The refresh delay is a scheduling setting, not a measured data-freshness SLA.
Multi-host coordination, automatic retention, freshness alerts, and incremental
materialization remain future work.

## Validation

```bash
.venv-analytics/bin/python -m pip install -r analytics/requirements-integration.txt -r serving/requirements-test.txt
.venv-analytics/bin/python -m unittest discover -s tests/analytics -v
.venv-analytics/bin/python -m unittest discover -s tests/serving -v
make -C deploy release-check PYTHON="$PWD/.venv-analytics/bin/python"
```

Unit tests cover serialization, ownership, rejected builds, publication failures,
rollback, periodic retry, and request consistency during a pointer change. The
integration check creates real Iceberg fixtures on an isolated Nessie branch,
publishes two releases, adds a conflicting business-key mapping to trigger dbt
rejection, and verifies API rollback without restarting the application. Its
schemas, branches, and fixture files are removed on exit. CI uploads dbt results
from `analytics/target/release-smoke/`.

Filesystem primitives: [Python `os.replace`](https://docs.python.org/3/library/os.html#os.replace)
and [Python `fcntl.flock`](https://docs.python.org/3/library/fcntl.html#fcntl.flock).
