"""Run inside the Flink image with kafka_sources on PYTHONPATH and both topics populated.

This bounded probe reads Kafka through the same source DDL as the streaming job.
It exercises the Java Avro format/registry integration, without writing lakehouse tables.
"""

import os

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

from kafka_sources import kafka_source_ddl


def main():
    if os.environ.get("EVENT_FORMAT") != "avro":
        raise ValueError("Run this probe with EVENT_FORMAT=avro")
    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    table_env.get_config().set("parallelism.default", "1")
    for topic in ("events.clickstream", "events.telemetry"):
        ddl = kafka_source_ddl(topic).replace(
            "'scan.startup.mode' = 'earliest-offset'",
            "'scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset'",
        )
        table_env.execute_sql(ddl)
        table = "kafka_" + topic.split(".")[-1]
        count = 0
        # Collect physical Avro fields; computed TIMESTAMP_LTZ values are not
        # supported by this Flink 1.18 Python collect() bridge.
        physical = table_env.from_path(table).drop_columns(col("proc_time"), col("event_time"))
        with physical.execute().collect() as rows:
            for row in rows:
                if not row[0]:
                    raise AssertionError(f"Missing event ID in {topic}")
                count += 1
        if count == 0:
            raise AssertionError(f"No events found in {topic}; run the integration tests first")
        print(f"{topic}: Flink decoded {count} Avro records", flush=True)


if __name__ == "__main__":
    main()
