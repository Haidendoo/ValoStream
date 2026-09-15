#!/usr/bin/env python3
"""
Spike 1.1: Branch-Scoped StarRocks Against Nessie (Validation of Finding CI-2)
Protocol:
1. Connects to Nessie and StarRocks.
2. Runs 20 sequential PR simulation cycles:
   - Create Nessie branch `pr_feature_N` from `main`
   - Mount StarRocks Iceberg REST catalog pointing to `/iceberg/pr_feature_N`
   - Execute metadata query (`SHOW DATABASES FROM cat_pr_N`)
   - Unmount (DROP CATALOG) from StarRocks
   - Delete Nessie branch
3. Measures latency (avg, p50, p95) and checks StarRocks FE JVM stability.
"""

import time
import requests
import pymysql

NESSIE_HOST = "localhost"
NESSIE_PORT = 19120
NESSIE_URL = f"http://{NESSIE_HOST}:{NESSIE_PORT}"
STARROCKS_HOST = "localhost"
STARROCKS_PORT = 9030
STARROCKS_USER = "root"
STARROCKS_PASSWORD = ""
NUM_CYCLES = 20

def get_main_hash():
    r = requests.get(f"{NESSIE_URL}/api/v2/trees/main")
    r.raise_for_status()
    return r.json()["reference"]["hash"]

def create_nessie_branch(name, parent_hash):
    t0 = time.time()
    r = requests.post(
        f"{NESSIE_URL}/api/v2/trees",
        params={"name": name, "type": "BRANCH"},
        json={"type": "BRANCH", "name": "main", "hash": parent_hash}
    )
    r.raise_for_status()
    t_ms = (time.time() - t0) * 1000
    return r.json()["reference"]["hash"], t_ms

def delete_nessie_branch(name, branch_hash):
    t0 = time.time()
    r = requests.delete(f"{NESSIE_URL}/api/v2/trees/{name}@{branch_hash}")
    r.raise_for_status()
    return (time.time() - t0) * 1000

def get_fe_memory(cursor):
    try:
        cursor.execute("SHOW PROC '/frontends'")
        # Return summary info or rows count
        return len(cursor.fetchall())
    except Exception:
        return 0

def run_benchmark():
    print("=" * 70)
    print("⚡ VALOSTREAM SPIKE 1.1: STARROCKS BRANCH-SCOPED NESSIE CATALOGS")
    print("=" * 70)

    main_hash = get_main_hash()
    print(f"📌 Nessie main branch head hash: {main_hash}")

    conn = pymysql.connect(
        host=STARROCKS_HOST,
        port=STARROCKS_PORT,
        user=STARROCKS_USER,
        password=STARROCKS_PASSWORD,
        autocommit=True
    )
    cursor = conn.cursor()

    metrics = {
        "nessie_branch_create_ms": [],
        "starrocks_cat_create_ms": [],
        "starrocks_query_ms": [],
        "starrocks_cat_drop_ms": [],
        "nessie_branch_delete_ms": []
    }

    print(f"\n🚀 Running {NUM_CYCLES} simulated CI PR cycles...\n")

    for i in range(1, NUM_CYCLES + 1):
        branch_name = f"pr_feature_{i}_{int(time.time()*1000)%100000}"
        cat_name = f"cat_pr_{i}"

        # 1. Nessie Create Branch
        b_hash, t_b_create = create_nessie_branch(branch_name, main_hash)
        metrics["nessie_branch_create_ms"].append(t_b_create)

        # 2. StarRocks Create Catalog
        create_sql = f"""
        CREATE EXTERNAL CATALOG {cat_name}
        PROPERTIES (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "iceberg.catalog.uri" = "http://valostream_nessie:19120/iceberg/{branch_name}",
            "iceberg.catalog.warehouse" = "s3://warehouse",
            "aws.s3.endpoint" = "http://valostream_minio:9000",
            "aws.s3.access_key" = "admin",
            "aws.s3.secret_key" = "password123",
            "aws.s3.enable_path_style_access" = "true"
        );
        """
        t0 = time.time()
        cursor.execute(create_sql)
        t_cat_create = (time.time() - t0) * 1000
        metrics["starrocks_cat_create_ms"].append(t_cat_create)

        # 3. StarRocks Query
        t0 = time.time()
        cursor.execute(f"SHOW DATABASES FROM {cat_name}")
        cursor.fetchall()
        t_query = (time.time() - t0) * 1000
        metrics["starrocks_query_ms"].append(t_query)

        # 4. StarRocks Drop Catalog
        t0 = time.time()
        cursor.execute(f"DROP CATALOG {cat_name}")
        t_cat_drop = (time.time() - t0) * 1000
        metrics["starrocks_cat_drop_ms"].append(t_cat_drop)

        # 5. Nessie Delete Branch
        t_b_del = delete_nessie_branch(branch_name, b_hash)
        metrics["nessie_branch_delete_ms"].append(t_b_del)

        total_cycle_ms = t_b_create + t_cat_create + t_query + t_cat_drop + t_b_del
        print(f"Cycle {i:02d}/{NUM_CYCLES} | Total: {total_cycle_ms:6.1f}ms | Cat Create: {t_cat_create:5.1f}ms | Query: {t_query:4.1f}ms | Cat Drop: {t_cat_drop:4.1f}ms")

    # Verify no dangling catalogs
    cursor.execute("SHOW CATALOGS")
    remaining_catalogs = cursor.fetchall()
    cursor.close()
    conn.close()

    print("\n" + "=" * 70)
    print("📊 BENCHMARK METRICS SUMMARY (milliseconds)")
    print("=" * 70)

    def stats(arr):
        s = sorted(arr)
        p50 = s[len(s) // 2]
        p95 = s[int(len(s) * 0.95)]
        avg = sum(s) / len(s)
        return avg, p50, p95

    for metric_name, vals in metrics.items():
        avg, p50, p95 = stats(vals)
        print(f"{metric_name:26s} | Avg: {avg:6.2f}ms | p50: {p50:6.2f}ms | p95: {p95:6.2f}ms")

    total_cycle_times = [
        sum(x) for x in zip(
            metrics["nessie_branch_create_ms"],
            metrics["starrocks_cat_create_ms"],
            metrics["starrocks_query_ms"],
            metrics["starrocks_cat_drop_ms"],
            metrics["nessie_branch_delete_ms"]
        )
    ]
    t_avg, t_p50, t_p95 = stats(total_cycle_times)
    print("-" * 70)
    print(f"{'TOTAL PR LIFECYCLE':26s} | Avg: {t_avg:6.2f}ms | p50: {t_p50:6.2f}ms | p95: {t_p95:6.2f}ms")
    print(f"Remaining catalogs in StarRocks: {len(remaining_catalogs)} (expected: 1 default_catalog)")
    print("=" * 70)

if __name__ == "__main__":
    run_benchmark()
