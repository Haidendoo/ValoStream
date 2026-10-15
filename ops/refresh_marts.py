"""Build frozen Nessie snapshots, validate, and publish immutable serving releases."""

import argparse
from contextlib import ExitStack, closing, contextmanager
from dataclasses import asdict, replace
from datetime import datetime, timezone
import fcntl
import json
import logging
import os
from pathlib import Path
import signal
from threading import Event
import uuid

import requests

from serving.queries import ServingSource
from serving.releases import (Release, ReleaseState, RetiredRelease, read_state, release_lease,
                              timestamp, utc_now, write_state)
from .analytics_common import ROOT, connection, run_dbt
from .nessie_quality_gate import frozen_catalog

log = logging.getLogger(__name__)


@contextmanager
def writer_lock(manifest):
    """One publisher/rollback per local manifest; readers do not take the lock."""
    manifest = Path(manifest)
    manifest.parent.mkdir(parents=True, exist_ok=True)
    with manifest.with_name(manifest.name + ".lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("Another mart release operation is in progress") from error
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def verify_release(conn, schema):
    """Resolve every model and execute the serving query shapes before promotion."""
    source = ServingSource(mart_schema=schema)
    with conn.cursor() as cursor:
        for table in [*source.readiness_tables(), source.mart("mart_user_merchant_affinity")]:
            cursor.execute(f"SELECT * FROM {table} LIMIT 1")
            cursor.fetchall()
        queries = [source.recommendations(10.775, 106.7, 1), source.live_couriers(None, 1)]
        queries += [source.hexagons(entity, resolution, 1)
                    for entity in ("interactions", "couriers") for resolution in (7, 9)]
        for sql, params in queries:
            cursor.execute(sql, params)
            cursor.fetchall()


def capture_watermarks(conn, schema):
    source = ServingSource(mart_schema=schema)
    watermarks = {}
    with conn.cursor() as cursor:
        for stream, table in (("interactions", "stg_interactions"), ("couriers", "stg_courier_telemetry")):
            cursor.execute(f"SELECT MAX(load_date) FROM {source.mart(table)}")
            value = cursor.fetchone()[0]
            # Flink writes UTC event timestamps to the timezone-less StarRocks DATETIME.
            watermarks[stream] = value.replace(tzinfo=timezone.utc).isoformat() if value is not None else None
    return watermarks


def publish(conn, session, manifest, source_ref="main", source_schema="vault", artifacts=None):
    manifest = Path(manifest)
    with writer_lock(manifest):
        try:
            old = read_state(manifest)
        except FileNotFoundError:
            old = None
        schema = f"vs_release_{uuid.uuid4().hex}"
        artifacts = Path(artifacts or ROOT / "analytics/target/releases") / schema
        with frozen_catalog(
            conn, session, os.environ.get("NESSIE_URL", "http://localhost:19120"),
            os.environ.get("STARROCKS_ICEBERG_URL", "http://valostream-nessie:19120/iceberg"),
            source_ref, source_schema,
        ) as snapshot:
            with conn.cursor() as cursor:
                # No IF NOT EXISTS: cleanup must only own databases created by this run.
                cursor.execute(f"CREATE DATABASE {schema}")
            try:
                code = run_dbt(["build"], {
                    "DBT_SOURCE_CATALOG": snapshot["catalog"], "DBT_SOURCE_SCHEMA": snapshot["schema"],
                    "DBT_OUTPUT_SCHEMA": schema,
                }, artifacts)
                if code:
                    raise RuntimeError(f"Mart quality gate failed (dbt exit {code}); artifacts: {artifacts}")
                verify_release(conn, schema)
                release = Release(schema, snapshot["ref"], snapshot["hash"], snapshot["schema"], utc_now(),
                                  capture_watermarks(conn, schema))
            except BaseException:
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP DATABASE {schema}")
                raise
        # Source cleanup finishes before publishing. Validated output is retained if
        # publication fails, including failures after replace/fsync: it may be live.
        retired = old.retired if old else ()
        if old and old.previous:
            retired += (RetiredRelease(old.previous, utc_now()),)
        state = ReleaseState(release, old.active if old else None, utc_now(), retired=retired)
        write_state(manifest, state)
        log.info("Published %s from %s@%s", schema, release.source_ref, release.source_hash)
        return state


def rollback(conn, manifest):
    with writer_lock(manifest):
        old = read_state(manifest)
        if old.previous is None:
            raise ValueError("No previous release is available")
        verify_release(conn, old.previous.schema)
        state = ReleaseState(old.previous, old.active, utc_now(), retired=old.retired)
        write_state(manifest, state)
        log.info("Rolled back to %s", state.active.schema)
        return state


def prune(conn, manifest, retention_seconds=86400, apply=False, now=None):
    """Only collect recorded retired releases, after a grace period and lease check."""
    if retention_seconds < 300:
        raise ValueError("Retention grace must be at least 300 seconds")
    now = now or datetime.now(timezone.utc)
    report = []
    with writer_lock(manifest):
        state = read_state(manifest)
        # Validation in ReleaseState excludes active/previous and duplicate entries.
        for entry in state.retired:
            schema = entry.release.schema
            if (now - timestamp(entry.retired_at)).total_seconds() < retention_seconds:
                report.append({"schema": schema, "status": "grace_period"})
                continue
            with ExitStack() as stack:
                try:
                    stack.enter_context(release_lease(manifest, schema, exclusive=True))
                except BlockingIOError:
                    report.append({"schema": schema, "status": "in_use"})
                    continue
                if not apply:
                    report.append({"schema": schema, "status": "eligible"})
                    continue
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP DATABASE IF EXISTS {schema}")
                state = replace(state, version=2,
                                retired=tuple(item for item in state.retired if item != entry))
                # A failed write leaves a retryable entry; DROP IF EXISTS is idempotent.
                write_state(manifest, state)
                report.append({"schema": schema, "status": "deleted"})
    return report


def refresh_loop(refresh, interval, stop):
    """Fixed delay after each attempt; failed builds leave the last release live."""
    if interval < 30:
        raise ValueError("Refresh interval must be at least 30 seconds")
    while not stop.is_set():
        try:
            refresh()
        except Exception:
            log.exception("Mart refresh failed; retaining the published release")
        stop.wait(interval)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("publish", "watch", "rollback", "status", "prune"))
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--ref", default="main")
    parser.add_argument("--source-schema", default="vault")
    parser.add_argument("--artifacts", type=Path, default=ROOT / "analytics/target/releases")
    parser.add_argument("--interval-seconds", type=int, default=300)
    parser.add_argument("--retention-seconds", type=int, default=86400)
    parser.add_argument("--apply", action="store_true", help="Delete eligible schemas with prune; default is dry-run")
    parser.add_argument("--prune-retired", action="store_true", help="Prune after successful watch refreshes")
    args = parser.parse_args()
    if args.retention_seconds < 300:
        parser.error("--retention-seconds must be at least 300")
    if args.apply and args.action != "prune":
        parser.error("--apply is only supported with prune")
    if args.prune_retired and args.action != "watch":
        parser.error("--prune-retired is only supported with watch")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    def refresh():
        with closing(connection()) as conn, requests.Session() as session:
            state = publish(conn, session, args.manifest, args.ref, args.source_schema, args.artifacts)
            if args.prune_retired:
                log.info("Retention: %s", prune(conn, args.manifest, args.retention_seconds, apply=True))
            return state

    if args.action == "watch":
        stop = Event()
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, lambda *_: stop.set())
        refresh_loop(refresh, args.interval_seconds, stop)
        return
    if args.action == "prune":
        with closing(connection()) as conn:
            report = prune(conn, args.manifest, args.retention_seconds, args.apply)
        print(json.dumps(report, indent=2))
        return
    if args.action == "status":
        state = read_state(args.manifest)
    elif args.action == "rollback":
        with closing(connection()) as conn:
            state = rollback(conn, args.manifest)
    else:
        state = refresh()
    print(json.dumps(asdict(state), indent=2))


if __name__ == "__main__":
    main()
