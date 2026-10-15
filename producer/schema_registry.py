"""Validate and explicitly register contracts; producers only look up approved schemas."""

import argparse
import json
from pathlib import Path
from urllib.parse import quote

import requests
from avro.compatibility import ReaderWriterCompatibilityChecker, SchemaCompatibilityType
from avro.schema import parse

from .event_contracts import EventContract, SCHEMA_DIR, TOPICS, topic_name


def check_evolution(previous, candidate):
    """BACKWARD Avro compatibility plus preservation of required application fields."""
    result = ReaderWriterCompatibilityChecker().get_compatibility(
        parse(json.dumps(candidate)), parse(json.dumps(previous))
    )
    if result.compatibility != SchemaCompatibilityType.compatible:
        raise ValueError(f"Not BACKWARD compatible: {result.messages}")
    fields = {field["name"]: field for field in candidate["fields"]}
    for field in previous["fields"]:
        if "default" not in field:
            replacement = fields.get(field["name"])
            if replacement is None or replacement["type"] != field["type"]:
                raise ValueError(f"Required field removed or changed: {field['name']}")


class SchemaRegistry:
    def __init__(self, url, session=None):
        self.url = url.rstrip("/")
        self.session = session or requests.Session()

    def request(self, method, path, body=None, missing_ok=False):
        response = self.session.request(
            method, f"{self.url}{path}", json=body, timeout=10,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        if missing_ok and response.status_code == 404:
            if response.json().get("error_code") in (40401, 40402):
                return None
        response.raise_for_status()
        return response.json()

    @staticmethod
    def subject(contract):
        return quote(f"{topic_name(contract.topic, 'avro')}-value", safe="")

    @staticmethod
    def body(contract):
        return {"schemaType": "AVRO", "schema": json.dumps(contract.schema)}

    def lookup(self, contract):
        # Fail closed if the exact local schema has not been approved/registered.
        result = self.request("POST", f"/subjects/{self.subject(contract)}", self.body(contract))
        return result["id"]

    def register(self, contract):
        subject = self.subject(contract)
        previous = self.request("GET", f"/subjects/{subject}/versions/latest", missing_ok=True)
        if previous is not None:
            check_evolution(json.loads(previous["schema"]), contract.schema)
        self.request("PUT", f"/config/{subject}", {"compatibility": "BACKWARD"})
        result = self.request("POST", f"/subjects/{subject}/versions", self.body(contract))
        return result["id"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("check", "register"))
    parser.add_argument("--url", default="http://localhost:8082")
    parser.add_argument("--schema-dir", type=Path, default=SCHEMA_DIR)
    parser.add_argument("--baseline-dir", type=Path)
    args = parser.parse_args()
    registry = SchemaRegistry(args.url)
    for topic in TOPICS:
        contract = EventContract(topic, args.schema_dir)
        parse(json.dumps(contract.schema))
        if args.baseline_dir:
            previous = args.baseline_dir / f"{topic}.avsc"
            if previous.exists():
                check_evolution(json.loads(previous.read_text()), contract.schema)
        if args.action == "register":
            schema_id = registry.register(contract)
            print(f"{topic_name(topic, 'avro')}-value: schema ID {schema_id}")
        else:
            print(f"{topic}: valid")


if __name__ == "__main__":
    main()
