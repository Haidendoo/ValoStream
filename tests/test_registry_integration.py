"""Opt-in integration checks: VALOSTREAM_INTEGRATION=1 python -m unittest discover -s tests -v."""

import copy
import io
import json
import os
import struct
import unittest
import uuid

from fastavro import schemaless_reader
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from producer.delivery_stream_producer import generate_clickstream_event, generate_courier_telemetry_event, publish_event
from producer.event_contracts import EventContract, TOPICS
from producer.schema_registry import SchemaRegistry


@unittest.skipUnless(os.environ.get("VALOSTREAM_INTEGRATION") == "1", "Requires local Kafka and Schema Registry")
class RegistryIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.registry = SchemaRegistry(os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8082"))

    def test_registration_is_repeatable_and_exact_lookup_works(self):
        for topic in TOPICS:
            contract = EventContract(topic)
            schema_id = self.registry.register(contract)
            self.assertEqual(self.registry.register(contract), schema_id)
            self.assertEqual(self.registry.lookup(contract), schema_id)

    def test_real_registry_evolution(self):
        subject = f"valostream-contract-test-{uuid.uuid4().hex}-value"
        schema = EventContract("events.clickstream").schema
        body = {"schemaType": "AVRO", "schema": json.dumps(schema)}
        self.registry.request("POST", f"/subjects/{subject}/versions", body)
        try:
            self.registry.request("PUT", f"/config/{subject}", {"compatibility": "BACKWARD"})
            additive = copy.deepcopy(schema)
            additive["fields"].append({"name": "campaign", "type": ["null", "string"], "default": None})
            compatible = self.registry.request("POST", f"/compatibility/subjects/{subject}/versions/latest",
                                               {"schemaType": "AVRO", "schema": json.dumps(additive)})
            self.assertTrue(compatible["is_compatible"])
            breaking = copy.deepcopy(schema)
            breaking["fields"][0]["type"] = "long"
            incompatible = self.registry.request("POST", f"/compatibility/subjects/{subject}/versions/latest",
                                                 {"schemaType": "AVRO", "schema": json.dumps(breaking)})
            self.assertFalse(incompatible["is_compatible"])
        finally:
            self.registry.request("DELETE", f"/subjects/{subject}")
            self.registry.request("DELETE", f"/subjects/{subject}?permanent=true")

    def test_kafka_round_trip_resolves_writer_schema(self):
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all")
        consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False,
                                 consumer_timeout_ms=15000)
        try:
            for topic, generate in (("events.clickstream", generate_clickstream_event),
                                    ("events.telemetry", generate_courier_telemetry_event)):
                contract = EventContract(topic)
                schema_id = self.registry.register(contract)
                event = generate()
                metadata = publish_event(producer, contract, event, "avro", schema_id).get(timeout=30)
                partition = TopicPartition(metadata.topic, metadata.partition)
                consumer.assign([partition])
                consumer.seek(partition, metadata.offset)
                message = next(consumer)
                self.assertEqual(message.offset, metadata.offset)
                magic, writer_id = struct.unpack(">BI", message.value[:5])
                self.assertEqual((magic, writer_id), (0, schema_id))
                writer = self.registry.request("GET", f"/schemas/ids/{writer_id}")
                decoded = schemaless_reader(io.BytesIO(message.value[5:]), json.loads(writer["schema"]))
                self.assertEqual(decoded, event)
        finally:
            consumer.close()
            producer.close(timeout=5)


if __name__ == "__main__":
    unittest.main()
