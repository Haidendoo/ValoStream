"""Exercise real API responses over dbt marts and equivalent raw-vault fixtures."""

from contextlib import ExitStack
import hashlib
import json
from unittest.mock import patch
import uuid

from fastapi.testclient import TestClient
import h3

from ops.analytics_common import ROOT, connection, run_dbt, wait_for_backend
from ops.check_analytics import load_fixtures
from serving import app as api
from serving.queries import ServingSource


def edge_case_fixtures():
    fixtures = json.loads((ROOT / "analytics/fixtures/vault.json").read_text())
    satellites = fixtures["sat_interaction_context_spatial"]
    for values in satellites["rows"]:
        if values[2] == "purchase":
            values[6] = 10.776
        elif values[1] == "2026-10-15 11:00:00":
            values[6] = 10.777

    def first(table):
        return dict(zip(fixtures[table]["columns"], fixtures[table]["rows"][0]))

    def append(table, row):
        fixtures[table]["rows"].append([row[column] for column in fixtures[table]["columns"]])

    # Unequal group sizes distinguish weighted means from means of hourly averages.
    extra_key = hashlib.md5(b"i4").hexdigest().upper()
    append("link_user_merchant_interaction", {**first("link_user_merchant_interaction"),
                                             "hk_interaction_id": extra_key})
    append("sat_interaction_context_spatial", {**first("sat_interaction_context_spatial"),
                                              "hk_interaction_id": extra_key, "dwell_time_ms": 500})
    # A more popular distant merchant must rank below the nearby merchant by score.
    merchant = hashlib.md5(b"m2").hexdigest().upper()
    append("hub_merchant", {**first("hub_merchant"), "hk_merchant_id": merchant, "merchant_id": "m2"})
    far7, far9 = [int(h3.latlng_to_cell(10.81, 106.74, resolution), 16) for resolution in (7, 9)]
    for index in range(5):
        key = hashlib.md5(f"distant_interaction_{index}".encode()).hexdigest().upper()
        append("link_user_merchant_interaction", {**first("link_user_merchant_interaction"),
                                                 "hk_interaction_id": key, "hk_merchant_id": merchant})
        append("sat_interaction_context_spatial", {**first("sat_interaction_context_spatial"),
                                                  "hk_interaction_id": key, "latitude": 10.81,
                                                  "longitude": 106.74, "h3_res7": far7, "h3_res9": far9})
    return fixtures


def verify_api(source, mode):
    with patch.object(api, "SOURCE", source), TestClient(api.app) as client:
        response = client.get("/healthz")
        assert response.status_code == 200, response.text
        assert response.json()["source"] == mode
        couriers = client.get("/spatial/couriers").json()
        assert couriers["total"] == 2, couriers
        assert len({row["courier_id"] for row in couriers["couriers"]}) == 2
        idle = client.get("/spatial/couriers?status=idle").json()
        assert [row["courier_id"] for row in idle["couriers"]] == ["c2"], idle
        delivering = client.get("/spatial/couriers?status=delivering").json()
        assert [row["courier_id"] for row in delivering["couriers"]] == ["c1"], delivering
        for resolution in (7, 9):
            response = client.get(f"/spatial/hexagons?resolution={resolution}")
            assert response.status_code == 200, response.text
            rows = response.json()["hexagons"]
            assert sum(row["count"] for row in rows) == 9, rows
            near = next(row for row in rows if row["count"] == 4)
            assert abs(near["properties"]["avg_dwell_ms"] - 275) < 0.001, near
            assert abs(near["centroid"][1] - 10.77575) < 0.000001, near
            response = client.get(f"/spatial/hexagons?entity=couriers&resolution={resolution}")
            assert response.status_code == 200, response.text
            rows = response.json()["hexagons"]
            assert sum(row["count"] for row in rows) == 2, rows
            assert sum(row["properties"]["active_couriers"] for row in rows) == 2, rows
        response = client.get("/recommendations?user_id=u1&lat=10.77575&lon=106.7&limit=1")
        assert response.status_code == 200, response.text
        top = response.json()["recommendations"][0]
        assert top["merchant_id"] == "m1", top
        assert top["interaction_count"] == 4 and top["distance_meters"] == 0 and top["score"] == 4, top
        fallback = client.get("/recommendations?user_id=u1&lat=0&lon=0&use_h3_filter=true")
        assert fallback.status_code == 200 and fallback.json()["total_returned"] == 2, fallback.text
        stats = client.get("/stats")
        assert stats.status_code == 200, stats.text
        assert stats.json()["hub_user"] == (2 if mode == "marts" else 3)
        assert client.get("/spatial/hexagons?resolution=8").status_code == 422
        print(f"API {mode}: current positions, weighted H3 aggregates, ranking, fallback, and readiness passed.")


def main():
    suffix = uuid.uuid4().hex[:12]
    source, output = f"vs_api_fixture_{suffix}", f"vs_api_marts_{suffix}"
    with ExitStack() as stack:
        conn = connection()
        stack.callback(conn.close)
        wait_for_backend(conn)

        def drop(schema):
            with conn.cursor() as cursor:
                cursor.execute(f"DROP DATABASE {schema}")

        with conn.cursor() as cursor:
            for schema in (source, output):
                cursor.execute(f"CREATE DATABASE {schema}")
                stack.callback(drop, schema)
            load_fixtures(cursor, source, edge_case_fixtures())
        code = run_dbt(["build"], {"DBT_SOURCE_CATALOG": "default_catalog", "DBT_SOURCE_SCHEMA": source,
                                  "DBT_OUTPUT_SCHEMA": output}, ROOT / "analytics/target/api-smoke")
        if code:
            raise RuntimeError("API fixture dbt build failed")
        verify_api(ServingSource(mart_schema=output), "marts")
        verify_api(ServingSource(catalog="default_catalog", vault_schema=source), "vault")
    print("Serving integration passed; temporary schemas removed.")


if __name__ == "__main__":
    main()
