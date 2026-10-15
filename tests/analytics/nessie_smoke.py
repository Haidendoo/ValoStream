"""Create synthetic Iceberg fixtures on a disposable branch; run the real Nessie/dbt gate."""

from contextlib import ExitStack
from datetime import datetime
import json
import os
import uuid

import pyarrow as pa
from pyarrow.fs import S3FileSystem
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import PyArrowFileIO
import requests

from ops.analytics_common import ROOT, connection, run_dbt
from ops.check_analytics import assert_marts
from ops.nessie_quality_gate import isolated_catalog


def main():
    base = os.environ.get("NESSIE_URL", "http://localhost:19120").rstrip("/")
    suffix = uuid.uuid4().hex
    branch, namespace = f"fixture_{suffix}", f"fixture_{suffix}"
    endpoint = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
    access, secret = os.environ.get("S3_ACCESS_KEY", "admin"), os.environ.get("S3_SECRET_KEY", "password123")
    fs = S3FileSystem(endpoint_override=endpoint, access_key=access, secret_key=secret,
                      region="us-east-1", allow_bucket_creation=True)
    fs.create_dir("warehouse")
    arrow_types = {"VARCHAR": pa.string(), "DATETIME": pa.timestamp("us"), "BIGINT": pa.int64(),
                   "INT": pa.int32(), "DOUBLE": pa.float64(), "VARBINARY": pa.binary()}
    fixtures = json.loads((ROOT / "analytics/fixtures/vault.json").read_text())
    with requests.Session() as session, ExitStack() as stack:
        def request(method, path, **kwargs):
            r = session.request(method, base + path, timeout=20, **kwargs)
            r.raise_for_status()
            return r.json() if r.content else None

        def remove_branch():
            ref = request("GET", f"/api/v2/trees/{branch}")["reference"]
            request("DELETE", f"/api/v2/trees/{branch}@{ref['hash']}")

        parent = request("GET", "/api/v2/trees/main")["reference"]
        fs.create_dir(f"warehouse/{namespace}")
        stack.callback(fs.delete_dir, f"warehouse/{namespace}")
        request("POST", "/api/v2/trees", params={"name": branch, "type": "BRANCH"}, json=parent)
        stack.callback(remove_branch)
        catalog = load_catalog("fixture", **{
            "type": "rest", "uri": base + "/iceberg/" + branch,
            "warehouse": "s3://warehouse/", "s3.endpoint": endpoint,
            "s3.access-key-id": access, "s3.secret-access-key": secret, "s3.region": "us-east-1",
        })
        # Nessie advertises a container endpoint; this probe runs on the host.
        catalog.properties["s3.endpoint"] = endpoint
        catalog.properties["py-io-impl"] = "pyiceberg.io.pyarrow.PyArrowFileIO"
        catalog.create_namespace(namespace)
        for name, fixture in fixtures.items():
            fields = [pa.field(column, arrow_types[kind.split('(')[0]])
                      for column, kind in fixture["columns"].items()]
            rows = []
            for values in fixture["rows"]:
                row = dict(zip(fixture["columns"], values))
                row["load_date"] = datetime.fromisoformat(row["load_date"])
                rows.append(row)
            batch = pa.Table.from_pylist(rows, schema=pa.schema(fields))
            table = catalog.create_table((namespace, name), schema=batch.schema,
                                         location=f"s3://warehouse/{namespace}/{name}")
            table.io = PyArrowFileIO({**table.io.properties, "s3.endpoint": endpoint})
            table.append(batch)

        conn = connection()
        stack.callback(conn.close)
        with isolated_catalog(conn, session, base,
                              os.environ.get("STARROCKS_ICEBERG_URL", "http://valostream-nessie:19120/iceberg"),
                              branch, namespace) as env:
            code = run_dbt(["build"], env, ROOT / "analytics/target/nessie-smoke")
            if code:
                raise RuntimeError("Nessie-backed dbt build failed")
            with conn.cursor() as cursor:
                assert_marts(cursor, env["DBT_OUTPUT_SCHEMA"])
    print("Nessie-backed dbt gate and mart assertions passed; disposable branches and files cleaned up.")


if __name__ == "__main__":
    main()
