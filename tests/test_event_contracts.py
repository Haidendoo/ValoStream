import copy
from contextlib import redirect_stderr, redirect_stdout
import importlib.util
import io
import json
import re
from pathlib import Path
import struct
import unittest
from unittest.mock import Mock, patch

from fastavro import schemaless_reader
from fastavro.validation import ValidationError

from producer.delivery_stream_producer import (
    generate_clickstream_event, generate_courier_telemetry_event, main, publish_event,
)
from producer.event_contracts import EventContract, TOPICS, topic_name
from producer.schema_registry import SchemaRegistry, check_evolution


class EventContractsTest(unittest.TestCase):
    def test_every_schema_parses(self):
        for topic in TOPICS:
            with self.subTest(topic=topic):
                EventContract(topic)

    def test_generated_events_round_trip_with_confluent_header(self):
        for topic, generate in (("events.clickstream", generate_clickstream_event),
                                ("events.telemetry", generate_courier_telemetry_event)):
            contract = EventContract(topic)
            for _ in range(20):
                event = generate()
                encoded = contract.encode(event, "avro", 42)
                self.assertEqual(encoded[:5], struct.pack(">BI", 0, 42))
                self.assertEqual(schemaless_reader(io.BytesIO(encoded[5:]), contract.parsed), event)
                self.assertEqual(json.loads(contract.encode(event)), event)

    def test_invalid_payload_never_reaches_kafka(self):
        contract = EventContract("events.clickstream")
        missing = generate_clickstream_event()
        del missing["user_id"]
        wrong = {**generate_clickstream_event(), "event_ts_ms": "yesterday"}
        fractional = {**generate_clickstream_event(), "event_ts_ms": 123.5}
        extra = {**generate_clickstream_event(), "unapproved": "field"}
        null = {**generate_clickstream_event(), "user_id": None}
        for event in (missing, wrong, fractional, extra, null):
            for event_format in ("json", "avro"):
                with self.subTest(event=event, format=event_format):
                    producer = Mock()
                    with self.assertRaises((ValidationError, ValueError, TypeError)):
                        publish_event(producer, contract, event, event_format, 42)
                    producer.send.assert_not_called()

    def test_nullable_order_id_defaults_to_null(self):
        event = generate_courier_telemetry_event()
        del event["order_id"]
        contract = EventContract("events.telemetry")
        wire = contract.encode(event, "avro", 1)
        decoded = schemaless_reader(io.BytesIO(wire[5:]), contract.parsed)
        self.assertIsNone(decoded["order_id"])
        self.assertIsNone(json.loads(contract.encode(event))["order_id"])

    def test_cdc_contracts_are_flat_change_events(self):
        for topic, event in (
            ("cdc.users", {"user_id": "u1", "op": "d", "event_ts_ms": 1000}),
            ("cdc.products", {"product_id": "p1", "merchant_id": "m1", "op": "c", "event_ts_ms": 1000}),
        ):
            contract = EventContract(topic)
            wire = contract.encode(event, "avro", 12)
            decoded = schemaless_reader(io.BytesIO(wire[5:]), contract.parsed)
            for key, value in event.items():
                self.assertEqual(decoded[key], value)

    def test_avro_requires_registered_id(self):
        for schema_id in (None, 0, -1, 2**32, True):
            with self.assertRaises(ValueError):
                EventContract("events.clickstream").encode(generate_clickstream_event(), "avro", schema_id)

    def test_publish_uses_isolated_topic_and_bytes(self):
        producer = Mock()
        contract = EventContract("events.clickstream")
        publish_event(producer, contract, generate_clickstream_event(), "avro", 3)
        args, kwargs = producer.send.call_args
        self.assertEqual(args, ("events.clickstream.avro.v1",))
        self.assertIsInstance(kwargs["value"], bytes)
        self.assertEqual(topic_name("events.clickstream", "json"), "events.clickstream")

    def test_invalid_cli_rate_and_count(self):
        for args in (("--rate", "0"), ("--max-events", "-1")):
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                main(list(args))
            self.assertEqual(error.exception.code, 2)

    def test_async_delivery_failure_is_reported_and_producer_closes(self):
        with patch("producer.delivery_stream_producer.KafkaProducer") as factory:
            producer = factory.return_value
            producer.send.return_value.get.side_effect = RuntimeError("Broker rejected record")
            with redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, "Broker rejected"):
                main(["--max-events", "1"])
            producer.close.assert_called_once()

    def test_avro_dry_run_never_contacts_services(self):
        with patch("producer.delivery_stream_producer.KafkaProducer") as producer, \
             patch("producer.delivery_stream_producer.SchemaRegistry") as registry, \
             redirect_stdout(io.StringIO()) as output:
            main(["--format", "avro", "--dry-run", "--max-events", "1"])
            producer.assert_not_called()
            registry.assert_not_called()
            self.assertIn(".avro.v1]", output.getvalue())


class EvolutionTest(unittest.TestCase):
    def setUp(self):
        self.previous = EventContract("events.clickstream").schema
        self.candidate = copy.deepcopy(self.previous)

    def test_adding_optional_field_passes_and_reads_old_data(self):
        self.candidate["fields"].append({"name": "campaign", "type": ["null", "string"], "default": None})
        check_evolution(self.previous, self.candidate)
        event = generate_clickstream_event()
        wire = EventContract("events.clickstream").encode(event, "avro", 1)
        read = schemaless_reader(io.BytesIO(wire[5:]), self.previous, self.candidate)
        self.assertIsNone(read["campaign"])

    def test_adding_field_without_default_fails(self):
        self.candidate["fields"].append({"name": "campaign", "type": "string"})
        with self.assertRaisesRegex(ValueError, "BACKWARD"):
            check_evolution(self.previous, self.candidate)

    def test_removing_required_field_fails_application_policy(self):
        self.candidate["fields"] = [f for f in self.candidate["fields"] if f["name"] != "user_id"]
        with self.assertRaisesRegex(ValueError, "Required field"):
            check_evolution(self.previous, self.candidate)

    def test_changing_required_type_fails(self):
        self.candidate["fields"][0]["type"] = "long"
        with self.assertRaises(ValueError):
            check_evolution(self.previous, self.candidate)


class RegistryTest(unittest.TestCase):
    def setUp(self):
        self.session = Mock()
        self.registry = SchemaRegistry("http://registry:8081/", self.session)
        self.contract = EventContract("events.clickstream")

    def response(self, body, status=200):
        response = Mock(status_code=status)
        response.json.return_value = body
        return response

    def test_registration_sets_subject_policy(self):
        self.session.request.side_effect = [
            self.response({"error_code": 40401}, 404),
            self.response({"compatibility": "BACKWARD"}), self.response({"id": 42}),
        ]
        self.assertEqual(self.registry.register(self.contract), 42)
        calls = self.session.request.call_args_list
        self.assertEqual(calls[1].args[:2], ("PUT", "http://registry:8081/config/events.clickstream.avro.v1-value"))
        self.assertEqual(calls[1].kwargs["json"], {"compatibility": "BACKWARD"})

    def test_registration_rejects_required_removal_before_mutation(self):
        previous = copy.deepcopy(self.contract.schema)
        self.contract.schema["fields"].pop(0)
        self.session.request.return_value = self.response({"schema": json.dumps(previous)})
        with self.assertRaises(ValueError):
            self.registry.register(self.contract)
        self.assertEqual(self.session.request.call_count, 1)

    def test_lookup_posts_exact_schema_without_registration(self):
        self.session.request.return_value = self.response({"id": 7})
        self.assertEqual(self.registry.lookup(self.contract), 7)
        args = self.session.request.call_args.args
        self.assertEqual(args, ("POST", "http://registry:8081/subjects/events.clickstream.avro.v1-value"))

    def test_registry_errors_propagate(self):
        import requests
        response = self.response({}, 503)
        response.raise_for_status.side_effect = requests.HTTPError("Unavailable")
        self.session.request.return_value = response
        with self.assertRaises(requests.HTTPError):
            self.registry.lookup(self.contract)


class FlinkSourceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).resolve().parents[1] / "flink/pyflink/kafka_sources.py"
        spec = importlib.util.spec_from_file_location("kafka_sources", path)
        cls.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.module)

    def test_avro_matches_producer_topics(self):
        for topic in ("events.clickstream", "events.telemetry"):
            sql = self.module.kafka_options(topic, {"EVENT_FORMAT": "avro"})
            self.assertIn(f"'topic' = '{topic_name(topic, 'avro')}'", sql)
            self.assertIn("'format' = 'avro-confluent'", sql)
            self.assertIn("'avro-confluent.url' = 'http://schema-registry:8081'", sql)
            self.assertNotIn("json.", sql)

    def test_legacy_json_fails_on_malformed_data(self):
        sql = self.module.kafka_options("events.clickstream", {})
        self.assertIn("'topic' = 'events.clickstream'", sql)
        self.assertIn("'json.ignore-parse-errors' = 'false'", sql)
        self.assertIn("'properties.group.id' = 'pyflink-lakehouse-clickstream'", sql)

    def test_unknown_format_fails(self):
        with self.assertRaises(ValueError):
            self.module.kafka_options("events.clickstream", {"EVENT_FORMAT": "typo"})

    def test_source_columns_match_writer_contracts(self):
        sql_types = {"string": "STRING", "long": "BIGINT", "int": "INT", "double": "DOUBLE"}
        for topic in ("events.clickstream", "events.telemetry"):
            ddl = self.module.kafka_source_ddl(topic)
            columns = re.findall(r"^\s+(\w+) (STRING|BIGINT|INT|DOUBLE),$", ddl, re.MULTILINE)
            expected = []
            for field in EventContract(topic).schema["fields"]:
                avro_type = field["type"]
                if isinstance(avro_type, list):
                    avro_type = next(kind for kind in avro_type if kind != "null")
                expected.append((field["name"], sql_types[avro_type]))
            self.assertEqual(columns, expected)


if __name__ == "__main__":
    unittest.main()
