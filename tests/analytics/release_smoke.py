"""Publish real Nessie/dbt releases; prove hot switching, rejection, and rollback."""

from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import os

from fastapi.testclient import TestClient
import pyarrow as pa
from pyiceberg.io.pyarrow import PyArrowFileIO

from nessie_smoke import iceberg_fixtures
from ops.analytics_common import ROOT
from ops.refresh_marts import publish, rollback
from serving import app as api
from serving.releases import read_state


def main():
    with iceberg_fixtures() as (conn, session, branch, namespace, catalog), ExitStack() as stack:
        directory = stack.enter_context(TemporaryDirectory())
        manifest = Path(directory) / "active.json"
        artifacts = ROOT / "analytics/target/release-smoke"
        stack.enter_context(patch.object(api, "RELEASE_FILE", manifest))
        client = stack.enter_context(TestClient(api.app))

        def schemas():
            with conn.cursor() as cursor:
                cursor.execute("SHOW DATABASES")
                return {row[0] for row in cursor.fetchall() if row[0].startswith("vs_release_")}

        def drop(schema):
            with conn.cursor() as cursor:
                cursor.execute(f"DROP DATABASE {schema}")

        def build():
            state = publish(conn, session, manifest, branch, namespace, artifacts)
            stack.callback(drop, state.active.schema)
            return state

        def user_count(expected, schema):
            health = client.get("/healthz")
            assert health.status_code == 200, health.text
            assert health.json()["mart_schema"] == schema, health.text
            response = client.get("/stats")
            assert response.status_code == 200, response.text
            assert response.json()["hub_user"] == expected, response.text

        assert client.get("/healthz").status_code == 503
        before = schemas()
        first = build()
        assert first.previous is None
        user_count(2, first.active.schema)

        # Add a valid user to the source. Existing native releases must stay unchanged.
        table = catalog.load_table((namespace, "hub_user"))
        table.io = PyArrowFileIO({**table.io.properties,
                                 "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://localhost:9000")})
        batch = table.scan().to_arrow()
        row = batch.to_pylist()[0]
        added = {**row, "hk_user_id": "C" * 32, "user_id": "u3"}
        table.append(pa.Table.from_pylist([added], schema=batch.schema))
        user_count(2, first.active.schema)
        second = build()
        assert second.previous == first.active
        assert second.active.source_hash != first.active.source_hash
        user_count(3, second.active.schema)

        # A conflicting hash/business-key mapping must fail the real dbt gate.
        table.append(pa.Table.from_pylist([{**added, "user_id": "conflicting_user"}], schema=batch.schema))
        try:
            build()
        except RuntimeError as error:
            assert "quality gate failed" in str(error), error
        else:
            raise AssertionError("Corrupt release was published")
        assert read_state(manifest) == second
        assert schemas() == before | {first.active.schema, second.active.schema}
        user_count(3, second.active.schema)

        reverted = rollback(conn, manifest)
        assert reverted.active == first.active and reverted.previous == second.active
        user_count(2, first.active.schema)
        response = client.get("/spatial/couriers")
        assert response.status_code == 200 and response.json()["total"] == 2, response.text
        print("Release smoke passed: two frozen builds, hot switch, rejected corruption, rollback.")
    print("Owned release databases, fixture branches, and files removed.")


if __name__ == "__main__":
    main()
