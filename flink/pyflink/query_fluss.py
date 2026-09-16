#!/usr/bin/env python3
"""
Query real-time records from Apache Fluss
"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    t_env.execute_sql("""
    CREATE CATALOG fluss_catalog WITH (
      'type' = 'fluss',
      'bootstrap.servers' = 'coordinator-server:9123'
    );
    """)

    print("📊 Querying real-time courier telemetry from Apache Fluss (<1s latency)...")
    result = t_env.execute_sql("""
    SELECT hk_courier_id, courier_id, latitude, longitude, speed_kmh, status, h3_res7
    FROM fluss_catalog.fluss.courier_telemetry_live /*+ OPTIONS('scan.startup.mode' = 'earliest') */
    LIMIT 5;
    """)
    rows = list(result.collect())
    for r in rows:
        print("  ->", r)

if __name__ == "__main__":
    main()
