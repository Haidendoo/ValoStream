"""Run dbt against a frozen Nessie branch and disposable native StarRocks output.

The branch is discarded on success or failure. This command never merges data.
"""

import argparse
from contextlib import ExitStack, contextmanager
import os
from pathlib import Path
from urllib.parse import quote
import uuid

import requests

from .analytics_common import ROOT, connection, identifier, run_dbt


@contextmanager
def frozen_catalog(conn, session, nessie_url, iceberg_url, source_ref="main", source_schema="vault"):
    """Own a temporary branch/catalog, leaving output ownership to the caller."""
    source_schema = identifier(source_schema)
    suffix = uuid.uuid4().hex
    branch = f"ci_{suffix}"
    catalog = identifier(f"vs_ci_{suffix}")

    def request(method, path, **kwargs):
        response = session.request(method, nessie_url.rstrip("/") + path, timeout=20, **kwargs)
        response.raise_for_status()
        return response.json() if response.content else None

    def sql(statement, parameters=None):
        with conn.cursor() as cursor:
            cursor.execute(statement, parameters)

    def delete_branch():
        ref = request("GET", f"/api/v2/trees/{branch}")["reference"]
        request("DELETE", f"/api/v2/trees/{branch}@{ref['hash']}")

    # ExitStack runs every registered cleanup even if another cleanup fails.
    with ExitStack() as stack:
        parent = request("GET", f"/api/v2/trees/{quote(source_ref, safe='')}")["reference"]
        request("POST", "/api/v2/trees", params={"name": branch, "type": "BRANCH"},
                json={"type": parent["type"], "name": source_ref, "hash": parent["hash"]})
        stack.callback(delete_branch)
        sql(f"""CREATE EXTERNAL CATALOG {catalog} PROPERTIES (
            "type"="iceberg", "iceberg.catalog.type"="rest",
            "iceberg.catalog.vended-credentials-enabled"="false",
            "iceberg.catalog.uri"=%s,
            "iceberg.catalog.warehouse"=%s,
            "aws.s3.endpoint"=%s, "aws.s3.access_key"=%s, "aws.s3.secret_key"=%s,
            "aws.s3.enable_path_style_access"="true",
            "aws.s3.region"="us-east-1", "aws.s3.use_instance_profile"="false",
            "aws.s3.use_aws_sdk_default_behavior"="false"
        )""", (
            iceberg_url.rstrip("/") + "/" + branch,
            os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse/"),
            os.environ.get("STARROCKS_S3_ENDPOINT", "http://valostream-minio:9000"),
            os.environ.get("S3_ACCESS_KEY", "admin"), os.environ.get("S3_SECRET_KEY", "password123"),
        ))
        stack.callback(sql, f"DROP CATALOG {catalog}")
        print(f"Frozen Nessie {source_ref}@{parent['hash']} via {branch}", flush=True)
        yield {"catalog": catalog, "schema": source_schema, "ref": source_ref, "hash": parent["hash"]}


@contextmanager
def isolated_catalog(conn, session, nessie_url, iceberg_url, source_ref="main", source_schema="vault"):
    with frozen_catalog(conn, session, nessie_url, iceberg_url, source_ref, source_schema) as snapshot:
        output = identifier(f"vs_out_{uuid.uuid4().hex}")
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {output}")
        try:
            yield {"DBT_SOURCE_CATALOG": snapshot["catalog"], "DBT_SOURCE_SCHEMA": snapshot["schema"],
                   "DBT_OUTPUT_SCHEMA": output}
        finally:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP DATABASE {output}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ref", default="main", help="Nessie source ref; its current hash is frozen for this run")
    parser.add_argument("--source-schema", default="vault")
    parser.add_argument("--artifacts", type=Path, default=ROOT / "analytics/target/nessie-gate")
    args = parser.parse_args()
    conn = connection()
    try:
        with requests.Session() as session, isolated_catalog(
            conn, session, os.environ.get("NESSIE_URL", "http://localhost:19120"),
            os.environ.get("STARROCKS_ICEBERG_URL", "http://valostream-nessie:19120/iceberg"), args.ref, args.source_schema,
        ) as environment:
            code = run_dbt(["build"], environment, args.artifacts)
    finally:
        conn.close()
    raise SystemExit(code)


if __name__ == "__main__":
    main()
