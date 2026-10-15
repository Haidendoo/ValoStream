"""Build frozen Nessie snapshots, validate, and publish immutable serving releases."""

import argparse
from contextlib import closing, contextmanager
from dataclasses import asdict
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
from serving.releases import Release, ReleaseState, read_state, utc_now, write_state
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
                release = Release(schema, snapshot["ref"], snapshot["hash"], snapshot["schema"], utc_now())
            except BaseException:
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP DATABASE {schema}")
                raise
        # Source cleanup finishes before publishing. Validated output is retained if
        # publication fails, including failures after replace/fsync: it may be live.
        state = ReleaseState(release, old.active if old else None, utc_now())
        write_state(manifest, state)
        log.info("Published %s from %s@%s", schema, release.source_ref, release.source_hash)
        return state


def rollback(conn, manifest):
    with writer_lock(manifest):
        old = read_state(manifest)
        if old.previous is None:
            raise ValueError("No previous release is available")
        verify_release(conn, old.previous.schema)
        state = ReleaseState(old.previous, old.active, utc_now())
        write_state(manifest, state)
        log.info("Rolled back to %s", state.active.schema)
        return state


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
    parser.add_argument("action", choices=("publish", "watch", "rollback", "status"))
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--ref", default="main")
    parser.add_argument("--source-schema", default="vault")
    parser.add_argument("--artifacts", type=Path, default=ROOT / "analytics/target/releases")
    parser.add_argument("--interval-seconds", type=int, default=300)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    def refresh():
        with closing(connection()) as conn, requests.Session() as session:
            return publish(conn, session, args.manifest, args.ref, args.source_schema, args.artifacts)

    if args.action == "watch":
        stop = Event()
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, lambda *_: stop.set())
        refresh_loop(refresh, args.interval_seconds, stop)
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
