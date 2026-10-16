"""Compare a completed UTC day on frozen Nessie main with its previous seven days."""

import argparse
from contextlib import closing
from datetime import date, datetime, time, timedelta, timezone
import json
import os
from pathlib import Path
import tempfile

import requests

from serving.quality import STREAMS, summarize_report
from .analytics_common import connection, identifier
from .nessie_quality_gate import frozen_catalog


def collect_counts(conn, catalog, schema, target_day):
    catalog, schema = identifier(catalog), identifier(schema)
    start = datetime.combine(target_day - timedelta(days=7), time())
    end = datetime.combine(target_day + timedelta(days=1), time())
    sources = {
        "interactions": ("sat_interaction_context_spatial", "hk_interaction_id, load_date"),
        "couriers": ("sat_courier_telemetry_spatial", "hk_courier_id, load_date"),
    }
    result = {}
    with conn.cursor() as cursor:
        cursor.execute("SET time_zone = '+00:00'")
        # Cancel on the server before the shared 120s socket read timeout, keeping
        # the connection usable for temporary catalog cleanup after a slow scan.
        cursor.execute("SET query_timeout = 90")
        for stream in STREAMS:
            table, keys = sources[stream]
            # Count identity/timestamp pairs; exact replays do not
            # inflate volume. Conflicting payload detection remains the dbt gate's job.
            cursor.execute(
                f"SELECT CAST(load_date AS DATE) AS event_day, COUNT(*) FROM "
                f"(SELECT DISTINCT {keys} FROM {catalog}.{schema}.{table} "
                "WHERE load_date >= %s AND load_date < %s) AS events "
                "GROUP BY CAST(load_date AS DATE)", (start, end),
            )
            result[stream] = {day.isoformat(): int(count) for day, count in cursor.fetchall()}
    return result


def write_report(path, report):
    """Publish a complete document atomically for concurrent metrics readers."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    name = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as handle:
            name = handle.name
            json.dump(report, handle, allow_nan=False, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        if name and os.path.exists(name):
            os.unlink(name)


def run_monitor(target_day, report_path, source_schema="vault"):
    # Reject incomplete/future windows before opening any connections.
    now = datetime.now(timezone.utc)
    if target_day >= now.date():
        raise ValueError("Monitor requires a completed UTC calendar day")
    identifier(source_schema)
    report = {"version": 1, "source_ref": "main", "target_day": target_day.isoformat(),
              "checked_at": now.isoformat(), "state": "error"}
    try:
        with closing(connection()) as conn, requests.Session() as session:
            with frozen_catalog(
                conn, session, os.environ.get("NESSIE_URL", "http://localhost:19120"),
                os.environ.get("STARROCKS_ICEBERG_URL", "http://valostream-nessie:19120/iceberg"),
                "main", source_schema,
            ) as snapshot:
                report["source_hash"] = snapshot["hash"]
                report["counts"] = collect_counts(conn, snapshot["catalog"], snapshot["schema"], target_day)
        # Cleanup failures also invalidate the run; never leave a previous green result.
        report["state"] = "complete"
        report["checked_at"] = datetime.now(timezone.utc).isoformat()
        summary = summarize_report(report)
    except Exception:
        report["state"] = "error"
        report["checked_at"] = datetime.now(timezone.utc).isoformat()
        write_report(report_path, report)
        raise
    write_report(report_path, report)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat,
                        default=datetime.now(timezone.utc).date() - timedelta(days=1))
    parser.add_argument("--source-schema", default="vault")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    summary = run_monitor(args.date, args.report, args.source_schema)
    print(json.dumps(summary, sort_keys=True))
    return {"ok": 0, "anomaly": 1, "insufficient_baseline": 2}[summary["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
