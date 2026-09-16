#!/usr/bin/env python3
"""
ValoStream PyFlink Real-Time Lakehouse Ingestion Pipeline
- Consumes Kafka topics (events.clickstream, events.telemetry)
- Python UDFs for H3 Hexagonal Indexing (res 7 & 9) and Shapely GeoParquet (WKB Point)
- Computes Data Vault 2.0 MD5 Hash Keys (UPPER)
- Sinks into Apache Iceberg Data Vault 2.0 tables via Apache Nessie REST Catalog
"""

import hashlib
from shapely.geometry import Point
import h3

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.udf import udf

# ==============================================================================
# Python UDFs for Geospatial & Hash Key Processing
# ==============================================================================

@udf(result_type=DataTypes.BIGINT())
def py_h3_res7(lat: float, lon: float):
    """Computes H3 Resolution 7 hexagonal cell index (~1.2 km2)."""
    if lat is None or lon is None:
        return None
    try:
        # Returns integer uint64 representation of H3 index
        return int(h3.latlng_to_cell(float(lat), float(lon), 7), 16)
    except Exception:
        return None

@udf(result_type=DataTypes.BIGINT())
def py_h3_res9(lat: float, lon: float):
    """Computes H3 Resolution 9 hexagonal cell index (~0.1 km2)."""
    if lat is None or lon is None:
        return None
    try:
        return int(h3.latlng_to_cell(float(lat), float(lon), 9), 16)
    except Exception:
        return None

@udf(result_type=DataTypes.BYTES())
def py_point_to_wkb(lat: float, lon: float):
    """Generates standard Well-Known Binary (WKB) point for GeoParquet format."""
    if lat is None or lon is None:
        return None
    try:
        # Standard GeoParquet: Point(x=lon, y=lat)
        return Point(float(lon), float(lat)).wkb
    except Exception:
        return None

@udf(result_type=DataTypes.STRING())
def py_md5(val: str):
    """Computes standard UPPER MD5 hash key."""
    if val is None:
        return None
    return hashlib.md5(str(val).strip().encode("utf-8")).hexdigest().upper()

@udf(result_type=DataTypes.STRING())
def py_composite_hk(part1: str, part2: str, part3: str):
    """Computes composite interaction hash key using standard delimiter."""
    if not part1 or not part2 or not part3:
        return None
    token = f"{str(part1).strip()};{str(part2).strip()};{str(part3).strip()}"
    return hashlib.md5(token.encode("utf-8")).hexdigest().upper()


def main():
    print("🚀 Initializing ValoStream PyFlink Geospatial Lakehouse Job...")

    # 1. Execution Environment & Checkpointing (10s interval, Exactly-Once)
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    env.set_parallelism(2)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 2. Register Python Geospatial UDFs
    t_env.create_temporary_function("py_h3_res7", py_h3_res7)
    t_env.create_temporary_function("py_h3_res9", py_h3_res9)
    t_env.create_temporary_function("py_point_to_wkb", py_point_to_wkb)
    t_env.create_temporary_function("py_md5", py_md5)
    t_env.create_temporary_function("py_composite_hk", py_composite_hk)
    print("✅ Registered Python UDFs (H3 Res7/9, WKB GeoParquet, MD5 Keys)")

    # 3. Register Nessie Iceberg REST Catalog
    t_env.execute_sql("""
    CREATE CATALOG nessie_catalog WITH (
      'type'='iceberg',
      'catalog-type'='rest',
      'uri'='http://valostream-nessie:19120/iceberg',
      'ref'='main',
      'warehouse'='s3://warehouse/',
      'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
      's3.endpoint'='http://valostream-minio:9000',
      's3.path-style-access'='true',
      's3.access-key-id'='admin',
      's3.secret-access-key'='password123'
    );
    """)
    print("✅ Registered Nessie Iceberg Catalog")

    # 4. Register Fluss Streaming Storage Catalog
    t_env.execute_sql("""
    CREATE CATALOG fluss_catalog WITH (
      'type' = 'fluss',
      'bootstrap.servers' = 'coordinator-server:9123'
    );
    """)
    print("✅ Registered Fluss Streaming Storage Catalog")

    # 5. Create Real-Time Streaming Tables in Fluss
    t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS fluss_catalog.fluss.courier_telemetry_live (
        hk_courier_id STRING,
        courier_id STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        speed_kmh DOUBLE,
        bearing_deg INT,
        status STRING,
        h3_res7 BIGINT,
        h3_res9 BIGINT,
        event_ts_ms BIGINT,
        PRIMARY KEY (hk_courier_id) NOT ENFORCED
    );
    """)

    t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS fluss_catalog.fluss.clickstream_realtime_log (
        hk_interaction_id STRING,
        user_id STRING,
        merchant_id STRING,
        event_type STRING,
        device_type STRING,
        dwell_time_ms BIGINT,
        latitude DOUBLE,
        longitude DOUBLE,
        h3_res7 BIGINT,
        h3_res9 BIGINT,
        event_ts_ms BIGINT
    );
    """)
    print("✅ Created Fluss Real-Time Streaming Tables")

    # 6. Ensure Lakehouse Database and Geospatial Tables Exist in Iceberg
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS nessie_catalog.vault;")

    t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS nessie_catalog.vault.hub_user (
        hk_user_id STRING,
        user_id STRING,
        load_date TIMESTAMP(3),
        record_source STRING
    ) WITH ('format-version' = '2');
    """)

    t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS nessie_catalog.vault.hub_merchant (
        hk_merchant_id STRING,
        merchant_id STRING,
        load_date TIMESTAMP(3),
        record_source STRING
    ) WITH ('format-version' = '2');
    """)

    t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS nessie_catalog.vault.hub_courier (
        hk_courier_id STRING,
        courier_id STRING,
        load_date TIMESTAMP(3),
        record_source STRING
    ) WITH ('format-version' = '2');
    """)

    t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS nessie_catalog.vault.link_user_merchant_interaction (
        hk_interaction_id STRING,
        hk_user_id STRING,
        hk_merchant_id STRING,
        load_date TIMESTAMP(3),
        record_source STRING
    ) WITH ('format-version' = '2');
    """)

    # Sat Interaction Context with H3 indices and GeoParquet WKB point
    t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS nessie_catalog.vault.sat_interaction_context_spatial (
        hk_interaction_id STRING,
        load_date TIMESTAMP(3),
        event_type STRING,
        device_type STRING,
        dwell_time_ms BIGINT,
        referrer_page STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        h3_res7 BIGINT,
        h3_res9 BIGINT,
        geom_wkb BYTES,
        record_source STRING
    ) WITH ('format-version' = '2');
    """)

    # Sat Courier Telemetry with H3 indices and GeoParquet WKB point
    t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS nessie_catalog.vault.sat_courier_telemetry_spatial (
        hk_courier_id STRING,
        load_date TIMESTAMP(3),
        latitude DOUBLE,
        longitude DOUBLE,
        speed_kmh DOUBLE,
        bearing_deg INT,
        status STRING,
        h3_res7 BIGINT,
        h3_res9 BIGINT,
        geom_wkb BYTES,
        record_source STRING
    ) WITH ('format-version' = '2');
    """)
    print("✅ Created Lakehouse Geospatial Tables in Iceberg")

    # 5. Kafka Sources
    t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS default_catalog.default_database.kafka_clickstream (
        event_id STRING,
        user_id STRING,
        item_id STRING,
        merchant_id STRING,
        event_type STRING,
        device_type STRING,
        dwell_time_ms BIGINT,
        referrer_page STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        event_ts_ms BIGINT,
        proc_time AS PROCTIME(),
        event_time AS TO_TIMESTAMP_LTZ(event_ts_ms, 3),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'events.clickstream',
        'properties.bootstrap.servers' = 'valostream-kafka:29092',
        'properties.group.id' = 'pyflink-lakehouse-clickstream',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    );
    """)

    t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS default_catalog.default_database.kafka_telemetry (
        telemetry_id STRING,
        courier_id STRING,
        order_id STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        speed_kmh DOUBLE,
        bearing_deg INT,
        status STRING,
        event_ts_ms BIGINT,
        proc_time AS PROCTIME(),
        event_time AS TO_TIMESTAMP_LTZ(event_ts_ms, 3),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'events.telemetry',
        'properties.bootstrap.servers' = 'valostream-kafka:29092',
        'properties.group.id' = 'pyflink-lakehouse-telemetry',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    );
    """)
    print("✅ Created Kafka Source Tables")

    # 6. Build Statement Set with Geospatial Transformation
    stmt_set = t_env.create_statement_set()

    # Hub User
    stmt_set.add_insert_sql("""
    INSERT INTO nessie_catalog.vault.hub_user
    SELECT
        py_md5(user_id) AS hk_user_id,
        user_id,
        event_time AS load_date,
        'events.clickstream' AS record_source
    FROM default_catalog.default_database.kafka_clickstream
    """)

    # Hub Merchant
    stmt_set.add_insert_sql("""
    INSERT INTO nessie_catalog.vault.hub_merchant
    SELECT
        py_md5(merchant_id) AS hk_merchant_id,
        merchant_id,
        event_time AS load_date,
        'events.clickstream' AS record_source
    FROM default_catalog.default_database.kafka_clickstream
    """)

    # Hub Courier
    stmt_set.add_insert_sql("""
    INSERT INTO nessie_catalog.vault.hub_courier
    SELECT
        py_md5(courier_id) AS hk_courier_id,
        courier_id,
        event_time AS load_date,
        'events.telemetry' AS record_source
    FROM default_catalog.default_database.kafka_telemetry
    """)

    # Link User-Merchant Interaction
    stmt_set.add_insert_sql("""
    INSERT INTO nessie_catalog.vault.link_user_merchant_interaction
    SELECT
        py_composite_hk(user_id, merchant_id, CAST(event_ts_ms AS STRING)) AS hk_interaction_id,
        py_md5(user_id) AS hk_user_id,
        py_md5(merchant_id) AS hk_merchant_id,
        event_time AS load_date,
        'events.clickstream' AS record_source
    FROM default_catalog.default_database.kafka_clickstream
    """)

    # Sat Interaction Context with H3 Indexing and GeoParquet WKB Point
    stmt_set.add_insert_sql("""
    INSERT INTO nessie_catalog.vault.sat_interaction_context_spatial
    SELECT
        py_composite_hk(user_id, merchant_id, CAST(event_ts_ms AS STRING)) AS hk_interaction_id,
        event_time AS load_date,
        event_type,
        device_type,
        dwell_time_ms,
        referrer_page,
        latitude,
        longitude,
        py_h3_res7(latitude, longitude) AS h3_res7,
        py_h3_res9(latitude, longitude) AS h3_res9,
        py_point_to_wkb(latitude, longitude) AS geom_wkb,
        'events.clickstream' AS record_source
    FROM default_catalog.default_database.kafka_clickstream
    """)

    # Sat Courier Telemetry with H3 Indexing and GeoParquet WKB Point
    stmt_set.add_insert_sql("""
    INSERT INTO nessie_catalog.vault.sat_courier_telemetry_spatial
    SELECT
        py_md5(courier_id) AS hk_courier_id,
        event_time AS load_date,
        latitude,
        longitude,
        speed_kmh,
        bearing_deg,
        status,
        py_h3_res7(latitude, longitude) AS h3_res7,
        py_h3_res9(latitude, longitude) AS h3_res9,
        py_point_to_wkb(latitude, longitude) AS geom_wkb,
        'events.telemetry' AS record_source
    FROM default_catalog.default_database.kafka_telemetry
    """)

    # Fluss: Real-Time Updatable Primary-Key Courier Telemetry Stream (<1s latency)
    stmt_set.add_insert_sql("""
    INSERT INTO fluss_catalog.fluss.courier_telemetry_live
    SELECT
        py_md5(courier_id) AS hk_courier_id,
        courier_id,
        latitude,
        longitude,
        speed_kmh,
        bearing_deg,
        status,
        py_h3_res7(latitude, longitude) AS h3_res7,
        py_h3_res9(latitude, longitude) AS h3_res9,
        event_ts_ms
    FROM default_catalog.default_database.kafka_telemetry
    """)

    # Fluss: Real-Time Append-Only Log Stream for Clickstream
    stmt_set.add_insert_sql("""
    INSERT INTO fluss_catalog.fluss.clickstream_realtime_log
    SELECT
        py_composite_hk(user_id, merchant_id, CAST(event_ts_ms AS STRING)) AS hk_interaction_id,
        user_id,
        merchant_id,
        event_type,
        device_type,
        dwell_time_ms,
        latitude,
        longitude,
        py_h3_res7(latitude, longitude) AS h3_res7,
        py_h3_res9(latitude, longitude) AS h3_res9,
        event_ts_ms
    FROM default_catalog.default_database.kafka_clickstream
    """)

    print("🚀 Submitting PyFlink Streaming Statement Set to cluster...")
    job_result = stmt_set.execute()
    print(f"🎉 PyFlink Job submitted successfully! Job ID: {job_result.get_job_client().get_job_id()}")


if __name__ == "__main__":
    main()
