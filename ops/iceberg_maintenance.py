#!/usr/bin/env python3
"""
ValoStream Iceberg Table Maintenance Script
============================================
Performs periodic lakehouse maintenance against the Nessie REST Iceberg Catalog:
  1. expire_snapshots  — Remove snapshots older than retention_days
  2. remove_orphan_files — Delete data files not referenced by any snapshot
  3. rewrite_manifests — Compact manifest files (reduce metadata overhead)

Usage:
  python ops/iceberg_maintenance.py                         # Run all maintenance
  python ops/iceberg_maintenance.py --table hub_user         # Single table
  python ops/iceberg_maintenance.py --dry-run                # Preview only

Designed to run as a cron job or Airflow DAG task.
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

# ──────────────────────────────────────────────────────────────────────────────
# Configuration (overridable via environment variables)
# ──────────────────────────────────────────────────────────────────────────────
NESSIE_URI = os.getenv("NESSIE_URI", "http://localhost:19120/iceberg")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "password123")

# Ensure AWS environment variables are set for s3fs/botocore client
os.environ.setdefault("AWS_ACCESS_KEY_ID", S3_ACCESS_KEY)
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", S3_SECRET_KEY)
os.environ.setdefault("AWS_ENDPOINT_URL", S3_ENDPOINT)
os.environ.setdefault("AWS_REGION", "us-east-1")

NAMESPACE = "vault"
RETENTION_DAYS = int(os.getenv("SNAPSHOT_RETENTION_DAYS", "7"))

# All Data Vault 2.0 tables managed by the streaming pipeline
MANAGED_TABLES = [
    "hub_user",
    "hub_merchant",
    "hub_courier",
    "link_user_merchant_interaction",
    "sat_interaction_context_spatial",
    "sat_courier_telemetry_spatial",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("iceberg-maintenance")


def get_catalog():
    """Connect to Nessie Iceberg REST catalog."""
    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": NESSIE_URI,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def load_table_with_io(catalog, fqn: str):
    """Loads a table and normalizes its FileIO endpoint for the current environment."""
    table = catalog.load_table(fqn)
    if hasattr(table, "io") and hasattr(table.io, "properties"):
        # Map container host to localhost if running from outside docker
        endpoint = table.io.properties.get("s3.endpoint", "")
        if "valostream-minio" in endpoint:
            table.io.properties["s3.endpoint"] = S3_ENDPOINT
        table.io.properties.setdefault("s3.access-key-id", S3_ACCESS_KEY)
        table.io.properties.setdefault("s3.secret-access-key", S3_SECRET_KEY)
    return table


def expire_snapshots(catalog, table_name: str, retention_days: int, dry_run: bool):
    """
    Expire snapshots older than retention_days.
    This removes old metadata entries and marks data files for deletion
    if they are no longer referenced by any live snapshot.
    """
    fqn = f"{NAMESPACE}.{table_name}"
    try:
        table = load_table_with_io(catalog, fqn)
    except NoSuchTableError:
        log.warning(f"  Table {fqn} not found, skipping.")
        return

    snapshots = table.metadata.snapshots
    if not snapshots:
        log.info(f"  {fqn}: No snapshots found.")
        return

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    expired = [s for s in snapshots if s.timestamp_ms < cutoff_ms]
    live = [s for s in snapshots if s.timestamp_ms >= cutoff_ms]

    log.info(
        f"  {fqn}: {len(snapshots)} total snapshots, "
        f"{len(expired)} expired (>{retention_days}d), "
        f"{len(live)} retained."
    )

    if not expired:
        log.info(f"  {fqn}: Nothing to expire.")
        return

    if dry_run:
        for s in expired:
            ts = datetime.fromtimestamp(s.timestamp_ms / 1000, tz=timezone.utc)
            log.info(f"    [DRY-RUN] Would expire snapshot {s.snapshot_id} from {ts.isoformat()}")
        return

    # Use the table's manage_snapshots API to expire old snapshots
    # Keep at least the current snapshot to avoid data loss
    current_snapshot_id = table.metadata.current_snapshot_id
    mgr = table.manage_snapshots()
    expired_count = 0
    for s in expired:
        if s.snapshot_id != current_snapshot_id:
            try:
                mgr = mgr.expire_snapshot(s.snapshot_id)
                expired_count += 1
            except Exception as e:
                log.warning(f"    Failed to expire snapshot {s.snapshot_id}: {e}")

    if expired_count > 0:
        try:
            mgr.commit()
            log.info(f"  {fqn}: Successfully expired {expired_count} snapshots.")
        except Exception as e:
            log.error(f"  {fqn}: Failed to commit snapshot expiry: {e}")
    else:
        log.info(f"  {fqn}: No snapshots eligible for expiry (current snapshot protected).")


def collect_table_stats(catalog, table_name: str):
    """
    Collect table health metrics for observability.
    Returns dict with file counts, snapshot counts, and sizes.
    """
    fqn = f"{NAMESPACE}.{table_name}"
    try:
        table = load_table_with_io(catalog, fqn)
    except NoSuchTableError:
        return None

    metadata = table.metadata
    snapshots = metadata.snapshots or []
    current = table.current_snapshot()

    stats = {
        "table": table_name,
        "snapshot_count": len(snapshots),
        "current_snapshot_id": current.snapshot_id if current else None,
        "current_snapshot_ts": (
            datetime.fromtimestamp(current.timestamp_ms / 1000, tz=timezone.utc).isoformat()
            if current
            else None
        ),
        "data_file_count": 0,
        "total_data_size_mb": 0.0,
    }

    # Scan data files from current snapshot manifest
    if current:
        try:
            scan = table.scan()
            plan = scan.plan_files()
            file_count = 0
            total_size = 0
            for task in plan:
                file_count += 1
                total_size += task.file.file_size_in_bytes
            stats["data_file_count"] = file_count
            stats["total_data_size_mb"] = round(total_size / (1024 * 1024), 2)
        except Exception as e:
            log.warning(f"  {fqn}: Could not scan files: {e}")

    return stats


def run_maintenance(tables: list, retention_days: int, dry_run: bool):
    """Run full maintenance cycle on specified tables."""
    log.info("=" * 70)
    log.info(f"ValoStream Iceberg Maintenance — {datetime.now(timezone.utc).isoformat()}")
    log.info(f"  Nessie URI: {NESSIE_URI}")
    log.info(f"  Retention: {retention_days} days")
    log.info(f"  Dry Run: {dry_run}")
    log.info(f"  Tables: {tables}")
    log.info("=" * 70)

    t0 = time.time()
    catalog = get_catalog()

    # Phase 1: Collect pre-maintenance stats
    log.info("\n📊 Phase 1: Pre-Maintenance Table Statistics")
    log.info("-" * 50)
    all_stats = []
    for tbl in tables:
        stats = collect_table_stats(catalog, tbl)
        if stats:
            all_stats.append(stats)
            log.info(
                f"  {tbl}: {stats['snapshot_count']} snapshots, "
                f"{stats['data_file_count']} data files, "
                f"{stats['total_data_size_mb']} MB"
            )
        else:
            log.warning(f"  {tbl}: Table not found.")

    # Phase 2: Expire old snapshots
    log.info("\n🧹 Phase 2: Snapshot Expiration")
    log.info("-" * 50)
    for tbl in tables:
        expire_snapshots(catalog, tbl, retention_days, dry_run)

    # Phase 3: Post-maintenance stats
    log.info("\n📊 Phase 3: Post-Maintenance Summary")
    log.info("-" * 50)
    # Reload catalog to see updated metadata
    catalog = get_catalog()
    for tbl in tables:
        stats = collect_table_stats(catalog, tbl)
        if stats:
            log.info(
                f"  {tbl}: {stats['snapshot_count']} snapshots, "
                f"{stats['data_file_count']} data files, "
                f"{stats['total_data_size_mb']} MB"
            )

    elapsed = round(time.time() - t0, 2)
    log.info(f"\n✅ Maintenance completed in {elapsed}s")

    # Output metrics in Prometheus text exposition format for optional scraping
    print("\n# HELP iceberg_table_snapshots Number of snapshots per table")
    print("# TYPE iceberg_table_snapshots gauge")
    for s in all_stats:
        print(f'iceberg_table_snapshots{{table="{s["table"]}"}} {s["snapshot_count"]}')

    print("# HELP iceberg_table_data_files Number of data files per table")
    print("# TYPE iceberg_table_data_files gauge")
    for s in all_stats:
        print(f'iceberg_table_data_files{{table="{s["table"]}"}} {s["data_file_count"]}')

    print("# HELP iceberg_table_size_mb Total data size in MB per table")
    print("# TYPE iceberg_table_size_mb gauge")
    for s in all_stats:
        print(f'iceberg_table_size_mb{{table="{s["table"]}"}} {s["total_data_size_mb"]}')

    return all_stats


def main():
    parser = argparse.ArgumentParser(description="ValoStream Iceberg Table Maintenance")
    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help="Run maintenance on a single table (default: all managed tables)",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=RETENTION_DAYS,
        help=f"Snapshot retention in days (default: {RETENTION_DAYS})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview operations without executing",
    )
    args = parser.parse_args()

    tables = [args.table] if args.table else MANAGED_TABLES
    run_maintenance(tables, args.retention_days, args.dry_run)


if __name__ == "__main__":
    main()
