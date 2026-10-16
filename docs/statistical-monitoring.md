# Daily statistical monitoring

Step 4.3 now includes an opt-in daily volume monitor, separate from the deterministic
dbt PR gate. It reads a frozen snapshot of Nessie `main`, compares a completed UTC
day with the preceding seven calendar days, and publishes a JSON report. It never
merges data or writes to source tables; its temporary Nessie branch and StarRocks
catalog are removed after each run.

## What is checked

| Stream | Source | Logical event key used for counting |
| --- | --- | --- |
| interactions | `vault.sat_interaction_context_spatial` | `hk_interaction_id, load_date` |
| couriers | `vault.sat_courier_telemetry_spatial` | `hk_courier_id, load_date` |

`load_date` is treated as UTC, matching the current Flink ingestion convention.
Exact replays collapse before counting. Conflicting payloads, null keys, and other
contract violations remain the deterministic gate's responsibility.
Each query has a 90-second server timeout, below the connection's 120-second read
timeout, so a slow query can fail while preserving the connection for cleanup.

The monitor queries exactly eight completed days: seven baseline days and one
target day. An absolute deviation **strictly greater than 50%** from the baseline
mean is an anomaly; exactly 50% passes. A target day with no records counts as zero.
Every baseline day must have an observed bucket. Missing baseline days produce
`insufficient_baseline`, never a healthy result. This deliberately treats a genuinely
idle baseline day as unknown: the source currently has no independent daily
completeness ledger that could distinguish zero traffic from missing ingestion.
Observed buckets alone do not prove ingestion completeness; freshness checks and
pipeline health remain necessary. Production acceptance still requires real history.

## Run manually

Install `analytics/requirements.txt` in the analytics environment, then from the
repository root:

```bash
python -m ops.statistical_monitor --report var/quality/latest.json
# Optional explicit historical day: use a separate report to preserve live status.
python -m ops.statistical_monitor --date 2026-10-08 --report var/quality/2026-10-08.json
```

The default is yesterday in UTC. Current/future days are rejected. The command uses
the same `STARROCKS_*`, `NESSIE_URL`, `STARROCKS_ICEBERG_URL`, `ICEBERG_WAREHOUSE`,
and S3 environment settings as the [Nessie quality gate](analytics-quality-gate.md).
`--source-schema` defaults to `vault`; the source reference is always `main`.
Exit codes: `0` healthy, `1` anomaly or runtime failure, `2` insufficient baseline.
Runtime failures replace any previous healthy report with an error report, then
propagate the exception. A report-write failure fails the command; the overdue
alert detects a report that stops advancing. Reports record the source hash and
aggregated counts, without subject identifiers or database credentials.

## Schedule with Airflow

`airflow/dags/dag_statistical_monitors.py` targets **Airflow 3.3.2 / Python 3.11**.
The DAG uses `airflow.sdk.DAG` and the standard provider's `PythonOperator`.
Install the runtime in a separate Python 3.11 environment from the repository root:

```bash
python -m pip install -r airflow/requirements.txt
```

This pins Airflow and resolves the standard provider, PyMySQL, and Requests with
the upstream 3.3.2/Python 3.11 constraints. The dedicated CI job uses the same file.
Keep Airflow separate from the dbt Python environment.

1. Make the repository available on the Airflow worker's `PYTHONPATH`; deploy the
   DAG file to its configured DAG folder. Workers need `pymysql` and `requests` plus
   network access to StarRocks and Nessie.
2. Set `VALOSTREAM_QUALITY_REPORT_FILE` to an absolute writable path on the worker.
   Optionally set `VALOSTREAM_QUALITY_SOURCE_SCHEMA` (default `vault`). Provide the
   same connection settings used by the CLI.
3. Share that report path with the serving API and configure the same environment
   variable there. Use the same service account or explicitly arrange file read
   access; reports are atomically replaced with owner-only permissions by default.
4. Unpause `valostream_statistical_monitors` after configuring the worker. It runs
   at **02:00 UTC daily (09:00 Asia/Bangkok)**, with two ten-minute retries, one active
   run, and a fifteen-minute task timeout. The target comes from the scheduled
   interval end, so a delayed retry checks the original date. An explicit UTC
   `CronDataIntervalTimetable` preserves this behavior independently of the
   installation's cron defaults. Catchup is disabled.

The DAG starts paused and is not deployed by the default Compose stack. Running
the checks does not itself install or start Airflow. Follow Airflow's
[data interval semantics](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/dag-run.html)
when triggering historical runs. Airflow 3 manual runs without a data interval
fail with an explicit instruction to use the CLI with `--date`; the task never
guesses a window from the worker's clock. Keep one writer per live report; run
manual backfills with separate output paths.

Deployment uses the Airflow 3 API server, scheduler, and DAG processor. This
repository has no deployed Airflow 2 metadata database to migrate. The existing
monitor queries StarRocks directly and does not access Airflow's metadata database.
See the [upstream Airflow 3 migration guide](https://airflow.apache.org/docs/apache-airflow/3.3.2/installation/upgrading_to_airflow3.html)
for changes to an existing external Airflow installation.

## Metrics and alerts

When `VALOSTREAM_QUALITY_REPORT_FILE` is configured on the API, `/metrics` exports:

| Metric | Meaning |
| --- | --- |
| `valostream_quality_report_configured` | Monitoring is enabled for this API instance. |
| `valostream_quality_report_valid` | The report exists and passes format/window validation. |
| `valostream_quality_healthy` | Both streams have seven baseline buckets and pass the threshold. Errors and insufficient baselines are unhealthy. |
| `valostream_quality_window_end_timestamp_seconds` | End of the target UTC day, independent of when a historical report was rerun. |

Prometheus rules in `deploy/prometheus/alerts.yml` fire after two minutes for a
missing/invalid report, an unhealthy result, or a target window more than 30 hours
old. Missing/corrupt reports remove prior healthy samples on the next scrape.
Disabled monitoring emits no report metrics and triggers no statistical alerts.
These rules do not affect API readiness or block mart publication. API scrape
failure is covered by the existing `ServingAPIUnavailable` alert.

Alert rules are supplied; external Slack/PagerDuty delivery is not configured or
sent. Production rollout still needs an Alertmanager receiver, shared report
storage (or a distributed replacement), and seven days of actual ingestion data.
Distribution-shape drift and seasonality-aware thresholds remain future work.

## Validation

```bash
python -m unittest discover -s tests/analytics -v
python -m unittest discover -s tests/serving -v
PYTHONPATH=. python tests/analytics/statistical_smoke.py
```

The smoke test creates eight days of synthetic Iceberg data on an isolated branch,
adds exact replays and rows outside the time window, then checks counts and anomaly
classification through StarRocks. It cleans up its temporary resources and never
writes fixtures to `main`. Synthetic fixtures validate behavior, not the existence
of a production baseline. CI also checks Prometheus alerts and parses the DAG under
the declared Airflow runtime.

Local validation on 2026-10-16: 50 analytics tests and 20 serving tests passed;
the Iceberg/StarRocks smoke test and both Prometheus rule suites passed. After the
Airflow migration, the DAG checks passed under 3.3.2 and cover parsing, SDK-to-scheduler
serialization, calendar/year
boundaries, retry window selection, missing manual-run intervals, and failure
propagation. This is correctness evidence on small
fixtures; no production baseline or throughput benchmark is claimed.
