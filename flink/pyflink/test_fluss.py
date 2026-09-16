#!/usr/bin/env python3
"""
Test Apache Fluss Flink Catalog and Table Operations
"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    print("🚀 Testing Apache Fluss Catalog in PyFlink...")
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 1. Register Fluss Catalog
    print("Connecting to Fluss Coordinator at coordinator-server:9123...")
    t_env.execute_sql("""
    CREATE CATALOG fluss_catalog WITH (
      'type' = 'fluss',
      'bootstrap.servers' = 'coordinator-server:9123'
    );
    """)
    print("✅ Registered fluss_catalog successfully!")

    # 2. Show databases in Fluss
    t_env.execute_sql("USE CATALOG fluss_catalog;")
    res = t_env.execute_sql("SHOW DATABASES;").collect()
    databases = [row[0] for row in res]
    print(f"✅ Fluss Databases: {databases}")

    # 3. Create a primary-key streaming table in Fluss
    print("Creating courier_telemetry_live table in Fluss...")
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
    print("✅ Created table courier_telemetry_live in Fluss!")

    # 4. Show tables
    res_tables = t_env.execute_sql("SHOW TABLES FROM fluss_catalog.fluss;").collect()
    tables = [row[0] for row in res_tables]
    print(f"✅ Fluss Tables in fluss database: {tables}")

    # 5. Insert a test record into Fluss Primary Key table
    print("Writing a real-time record into Fluss Primary Key Table...")
    t_env.execute_sql("""
    INSERT INTO fluss_catalog.fluss.courier_telemetry_live
    VALUES (
        'HK_DRV_TEST_01', 'DRV_TEST_01', 10.7750, 106.7000, 25.5, 90, 'delivering',
        609775225674399743, 618782424921276415, 1789549000000
    );
    """).wait()
    print("✅ Successfully inserted record into Fluss!")

    # 6. Read back from Fluss
    print("Querying back from Fluss...")
    query_result = t_env.execute_sql("""
    SELECT hk_courier_id, courier_id, latitude, longitude, speed_kmh, status
    FROM fluss_catalog.fluss.courier_telemetry_live /*+ OPTIONS('scan.startup.mode' = 'earliest') */
    LIMIT 1;
    """).collect()
    rows = [list(r) for r in query_result]
    print(f"✅ Retrieved from Fluss: {rows}")
    print("🎉 Fluss Integration Test 100% Passed!")

if __name__ == "__main__":
    main()
