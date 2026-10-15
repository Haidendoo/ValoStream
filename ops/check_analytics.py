"""Build dbt against isolated StarRocks fixtures, then prove quality gates reject corruption."""

import argparse
import json
from pathlib import Path
import uuid

from .analytics_common import ROOT, connection, identifier, run_dbt, wait_for_backend


def load_fixtures(cursor, schema):
    schema = identifier(schema)
    fixtures = json.loads((ROOT / "analytics/fixtures/vault.json").read_text())
    for name, table in fixtures.items():
        name = identifier(name)
        columns = ", ".join(f"{identifier(col)} {kind}" for col, kind in table["columns"].items())
        key = next(iter(table["columns"]))
        cursor.execute(f"CREATE TABLE {schema}.{name} ({columns}) "
                       f"DUPLICATE KEY({key}) DISTRIBUTED BY HASH({key}) BUCKETS 1 "
                       "PROPERTIES ('replication_num'='1')")
        placeholders = ",".join(["%s"] * len(table["columns"]))
        cursor.executemany(f"INSERT INTO {schema}.{name} VALUES ({placeholders})", table["rows"])


def assert_marts(cursor, output):
    output = identifier(output)
    cursor.execute(f"SELECT count(*) FROM {output}.fct_interactions")
    assert cursor.fetchone()[0] == 3, "Duplicate source rows inflated the fact table"
    cursor.execute(f"SELECT resolution, sum(event_count), sum(total_dwell_time_ms) "
                   f"FROM {output}.mart_h3_demand_hourly GROUP BY resolution ORDER BY resolution")
    assert cursor.fetchall() == ((7, 3, 600), (9, 3, 600)), "H3 rollup lost or duplicated events"
    cursor.execute(f"SELECT courier_id, status FROM {output}.current_courier_positions ORDER BY courier_id")
    assert cursor.fetchall() == (("c1", "delivering"), ("c2", "idle")), "Current position selected old telemetry"
    cursor.execute(f"SELECT sum(interaction_count), sum(purchase_count) FROM {output}.mart_user_merchant_affinity")
    assert cursor.fetchone() == (3, 1), "Affinity counts are incorrect"


def require_test_failure(artifact_dir, expected_prefix):
    results = json.loads((Path(artifact_dir) / "run_results.json").read_text())["results"]
    # A SQL/runtime error is not evidence that the quality rule caught bad data.
    failures = [r for r in results if r["status"] == "fail" and
                r["unique_id"].startswith("test.valostream." + expected_prefix)]
    if not failures:
        raise AssertionError(f"Expected a data-test failure for {expected_prefix}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, default=ROOT / "analytics/target/fixture-check")
    args = parser.parse_args()
    suffix = uuid.uuid4().hex[:12]
    source, output = f"vs_fixture_{suffix}", f"vs_check_{suffix}"
    environment = {"DBT_SOURCE_CATALOG": "default_catalog", "DBT_SOURCE_SCHEMA": source,
                   "DBT_OUTPUT_SCHEMA": output}
    conn = connection()
    created = []
    try:
        wait_for_backend(conn)
        with conn.cursor() as cursor:
            for schema in (source, output):
                cursor.execute(f"CREATE DATABASE {schema}")
                created.append(schema)
            load_fixtures(cursor, source)
            if run_dbt(["build"], environment, args.artifacts / "valid"):
                raise RuntimeError("Valid fixture dbt build failed")
            assert_marts(cursor, output)

            # A duplicate hub key with a different business key must not be hidden by deduplication.
            cursor.execute(f"INSERT INTO {source}.hub_user "
                           f"SELECT hk_user_id, 'conflicting_user', load_date, record_source "
                           f"FROM {source}.hub_user WHERE user_id='u1' LIMIT 1")
            if run_dbt(["build", "--select", "stg_users"], environment, args.artifacts / "conflicting-key") == 0:
                raise AssertionError("Conflicting hub key passed")
            require_test_failure(args.artifacts / "conflicting-key", "unique_stg_users_hk_user_id")
            cursor.execute(f"DELETE FROM {source}.hub_user WHERE user_id='conflicting_user'")

            cursor.execute(f"INSERT INTO {source}.link_user_merchant_interaction "
                           f"SELECT 'orphan_event', 'missing_user', hk_merchant_id, load_date, record_source "
                           f"FROM {source}.link_user_merchant_interaction LIMIT 1")
            if run_dbt(["build", "--select", "stg_users", "stg_interaction_links"], environment,
                       args.artifacts / "orphan-link") == 0:
                raise AssertionError("Orphan link passed")
            require_test_failure(args.artifacts / "orphan-link", "relationships_stg_interaction_links_hk_user_id")
            cursor.execute(f"DELETE FROM {source}.link_user_merchant_interaction WHERE hk_interaction_id='orphan_event'")

            cursor.execute(f"INSERT INTO {source}.sat_courier_telemetry_spatial "
                           f"SELECT hk_courier_id, load_date, latitude, longitude, speed_kmh, bearing_deg, "
                           f"'to_merchant', h3_res7, h3_res9, geom_wkb, record_source "
                           f"FROM {source}.sat_courier_telemetry_spatial WHERE status='delivering' LIMIT 1")
            if run_dbt(["build", "--select", "stg_courier_telemetry"], environment,
                       args.artifacts / "ambiguous-position") == 0:
                raise AssertionError("Conflicting courier timestamp passed")
            require_test_failure(args.artifacts / "ambiguous-position", "unique_combination_stg_courier_telemetry")

            cursor.execute(f"ALTER TABLE {source}.hub_user RENAME hub_user_valid")
            cursor.execute(f"CREATE TABLE {source}.hub_user DISTRIBUTED BY HASH(hk_user_id) BUCKETS 1 "
                           "PROPERTIES ('replication_num'='1') AS "
                           f"SELECT hk_user_id, CAST(123 AS BIGINT) AS user_id, load_date, record_source "
                           f"FROM {source}.hub_user_valid")
            if run_dbt(["test", "--select", "source:vault.hub_user"], environment,
                       args.artifacts / "wrong-type") == 0:
                raise AssertionError("Wrong source column type passed")
            require_test_failure(args.artifacts / "wrong-type", "source_schema_conforms_vault_hub_user")
            print("Analytics checks passed: correct aggregates/latest positions and all corruption cases rejected.")
    finally:
        try:
            with conn.cursor() as cursor:
                for schema in reversed(created):
                    cursor.execute(f"DROP DATABASE {schema}")
        finally:
            conn.close()


if __name__ == "__main__":
    main()
