"""Kafka source DDL configuration, importable without a Flink installation."""

import os


def kafka_options(topic, environment=None):
    config = os.environ if environment is None else environment
    event_format = config.get("EVENT_FORMAT", "json")
    if event_format not in ("json", "avro"):
        raise ValueError("EVENT_FORMAT must be json or avro")
    if topic not in ("events.clickstream", "events.telemetry"):
        raise ValueError(f"Unsupported Kafka source: {topic}")
    options = {
        "connector": "kafka",
        "topic": f"{topic}.avro.v1" if event_format == "avro" else topic,
        "properties.bootstrap.servers": config.get("KAFKA_BOOTSTRAP_SERVERS", "valostream-kafka:29092"),
        "properties.group.id": f"pyflink-lakehouse-{topic.split('.')[-1]}" + ("-avro-v1" if event_format == "avro" else ""),
        "scan.startup.mode": "earliest-offset",
        "format": "avro-confluent" if event_format == "avro" else "json",
    }
    if event_format == "avro":
        options["avro-confluent.url"] = config.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    else:
        options.update({"json.fail-on-missing-field": "true", "json.ignore-parse-errors": "false"})
    return ",\n        ".join(
        "'{}' = '{}'".format(key, value.replace("'", "''")) for key, value in options.items()
    )


def kafka_source_ddl(topic):
    """Source schemas used by the streaming job and the bounded integration probe."""
    if topic == "events.clickstream":
        return f"""
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
        {kafka_options("events.clickstream")}
    );
    """
    if topic == "events.telemetry":
        return f"""
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
        {kafka_options("events.telemetry")}
    );
    """
    raise ValueError(f"Unsupported Kafka source: {topic}")
