"""Shared connection and subprocess settings for isolated analytics checks."""

import os
from pathlib import Path
import re
import subprocess
import sys
import time

import pymysql

ROOT = Path(__file__).resolve().parents[1]


def identifier(value):
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return value


def connection():
    return pymysql.connect(
        host=os.environ.get("STARROCKS_HOST", "localhost"),
        port=int(os.environ.get("STARROCKS_PORT", "9030")),
        user=os.environ.get("STARROCKS_USER", "root"),
        password=os.environ.get("STARROCKS_PASSWORD", ""),
        autocommit=True, connect_timeout=10, read_timeout=120,
    )


def wait_for_backend(conn, timeout=90):
    """FE accepts SELECT 1 before a fresh all-in-one container's BE is ready."""
    deadline = time.monotonic() + timeout
    while True:
        with conn.cursor() as cursor:
            cursor.execute("SHOW BACKENDS")
            columns = [column[0].lower() for column in cursor.description]
            alive_index = columns.index("alive")
            if any(str(row[alive_index]).lower() in ("true", "1") for row in cursor.fetchall()):
                return
        if time.monotonic() >= deadline:
            raise TimeoutError("No live StarRocks backend became available")
        time.sleep(2)


def run_dbt(args, environment, artifact_dir):
    # Use the dbt executable from the same virtualenv as this script.
    dbt = Path(sys.executable).parent / "dbt"
    env = {**os.environ, **environment, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    env["DBT_TARGET_PATH"] = str(Path(artifact_dir).resolve())
    env["DBT_LOG_PATH"] = str(Path(artifact_dir).resolve() / "logs")
    return subprocess.run([
        str(dbt), *args, "--project-dir", str(ROOT / "analytics"),
        "--profiles-dir", str(ROOT / "analytics"),
    ], env=env, check=False, timeout=600).returncode
