"""Versioned event contracts and Confluent-wire-format Avro serialization."""

import io
import json
from pathlib import Path
import struct

from fastavro import parse_schema, schemaless_writer
from fastavro.validation import validate


SCHEMA_DIR = Path(__file__).resolve().parents[1] / "schemas"
TOPICS = ("events.clickstream", "events.telemetry", "cdc.users", "cdc.products")


def topic_name(topic, event_format):
    if topic not in TOPICS:
        raise ValueError(f"Unknown event contract: {topic}")
    if event_format not in ("json", "avro"):
        raise ValueError(f"Unsupported event format: {event_format}")
    # Existing topics contain JSON. Never mix binary Avro into their history.
    return f"{topic}.avro.v1" if event_format == "avro" else topic


class EventContract:
    def __init__(self, topic, schema_dir=SCHEMA_DIR):
        if topic not in TOPICS:
            raise ValueError(f"Unknown event contract: {topic}")
        self.topic = topic
        self.schema = json.loads((Path(schema_dir) / f"{topic}.avsc").read_text())
        self.parsed = parse_schema(self.schema)

    def encode(self, event, event_format="json", schema_id=None):
        topic_name(self.topic, event_format)  # Validate the format even for dry runs.
        payload = io.BytesIO()
        # Reject unknown fields, absent required fields and incorrect types before send().
        validate(event, self.parsed)
        schemaless_writer(payload, self.parsed, event, strict_allow_default=True)
        if event_format == "json":
            defaults = {field["name"]: field["default"] for field in self.schema["fields"] if "default" in field}
            return json.dumps({**defaults, **event}, allow_nan=False).encode("utf-8")
        if type(schema_id) is not int or not 0 < schema_id < 2**32:
            raise ValueError("Avro serialization requires a positive uint32 schema ID")
        return struct.pack(">BI", 0, schema_id) + payload.getvalue()
