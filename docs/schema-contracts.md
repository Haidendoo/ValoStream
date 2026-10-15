# Event contracts (Step 4.1)

The delivery producer validates records against checked-in Avro contracts before
calling Kafka. JSON remains the default. Avro uses Confluent framing (magic byte,
four-byte schema ID, binary record) and separate topics so existing JSON history
remains readable.

| Contract | JSON topic | Avro topic / subject |
| --- | --- | --- |
| Clickstream | `events.clickstream` | `events.clickstream.avro.v1` / `events.clickstream.avro.v1-value` |
| Courier telemetry | `events.telemetry` | `events.telemetry.avro.v1` / `events.telemetry.avro.v1-value` |
| User changes | Reserved: `cdc.users` | `cdc.users.avro.v1` / `cdc.users.avro.v1-value` |
| Product changes | Reserved: `cdc.products` | `cdc.products.avro.v1` / `cdc.products.avro.v1-value` |

The two CDC schemas are flat change-event contracts reserved for future adapters,
not Debezium envelopes. They require an entity ID, `op`, and millisecond event
timestamp; product changes also require a merchant ID. Their producers and Flink
consumers are not implemented yet. They must not be used to ingest real profile
data until the Step 4.2 PII/erasure work is complete.

## Run locally

Run from the repository root with Python 3.12:

```bash
python3 -m venv .venv-contracts
.venv-contracts/bin/python -m pip install -r producer/requirements.txt
docker compose -f deploy/docker-compose.yml up -d --wait kafka schema-registry
.venv-contracts/bin/python -m producer.schema_registry check
.venv-contracts/bin/python -m producer.schema_registry register
.venv-contracts/bin/python -m producer.delivery_stream_producer --format avro --max-events 100
```

Schema Registry is exposed at `http://localhost:8082`; containers use
`http://schema-registry:8081`. Registration is explicit and repeatable. The
producer looks up the exact checked-in schema once at startup and fails if the
registry is unavailable or that schema is unregistered. It never registers a new
schema on the hot path. Broker delivery errors propagate and exit nonzero; the
final count distinguishes generated records from acknowledged records.

An offline dry run validates payloads without using Kafka or Schema Registry:

```bash
.venv-contracts/bin/python producer/delivery_stream_producer.py --format avro --dry-run --max-events 5
.venv-contracts/bin/python -m unittest discover -s tests -v
```

To include the real registry/Kafka tests after starting those services:

```bash
VALOSTREAM_INTEGRATION=1 .venv-contracts/bin/python -m unittest discover -s tests -v
```

These tests register the four development subjects and publish synthetic records
to the two Avro event topics. Compatibility experiments use disposable subjects
that the tests delete afterwards.

## Flink migration

Stop the existing streaming job with a savepoint before switching inputs. Rebuild
the Flink image to install the Avro Confluent SQL format bundle, then recreate both
Flink services with the same format setting:

```bash
docker compose -f deploy/docker-compose.yml build flink-jobmanager
EVENT_FORMAT=avro docker compose -f deploy/docker-compose.yml up -d flink-jobmanager flink-taskmanager
docker compose -f deploy/docker-compose.yml exec flink-jobmanager \
  flink run -d -py /opt/flink/pyflink/streaming_vault_job.py \
  -pyfs /opt/flink/pyflink/kafka_sources.py
```

Start the Avro producer after schema registration. The new sources use distinct
consumer groups and begin at the earliest offsets of the Avro topics. This is a
new input stream, not an automatic offset/savepoint migration. Do not replay old
JSON events into Avro topics or run both ingestion jobs into the same vault unless
you have arranged deduplication: existing append-only sinks can duplicate rows.
`record_source` continues to identify the logical event contract, independent of
wire format. To use the legacy stream, set `EVENT_FORMAT=json` and use the default
producer mode. JSON consumers now fail on malformed or missing fields instead of
silently ignoring parse errors.

After rebuilding the image and populating both topics, run the bounded Flink
decoder check without starting the lakehouse ingestion job:

```bash
docker compose -f deploy/docker-compose.yml run --rm --no-deps -T \
  -e EVENT_FORMAT=avro -e PYTHONPATH=/opt/flink/pyflink \
  --entrypoint python3 flink-jobmanager - < tests/flink_avro_smoke.py
```

The probe reads from earliest to the current latest offsets and checks physical
Avro fields using the streaming job's source DDL. It does not write vault tables.

## Evolution and enforcement

Each subject uses `BACKWARD`: deploy consumers that understand the new schema
before updating writers. New fields should have a default, usually a nullable
union with `null` first and `default: null`.

Avro backward compatibility permits discarding writer fields. ValoStream adds a
stricter application rule: fields without defaults may not be removed or change
type. Both the registration command and the pull-request CI gate enforce this.
The registry alone does not enforce the extra rule; direct API writes bypass it.
Restrict registry write access to the registration process in production.

The event-contract CI gate compares checked-in contracts with the PR base, checks
syntax, executes unit tests, and validates Compose. A separate
[analytics workflow](analytics-quality-gate.md) also runs on schema changes and
builds/tests dbt models against disposable Nessie/Iceberg fixtures.
`BACKWARD` compares the latest version only; use a transitive policy if a
future retention/replay strategy needs compatibility with every historical version.

Kafka itself does not validate payloads in this local deployment. Validation is
enforced by this producer, and decoding failures stop the Flink job. Other Kafka
writers must adopt the same contracts or pass through a validating adapter.

## Validation recorded on 2026-10-15

- 25 unit/integration tests passed with Python 3.12, Kafka, and Schema Registry 7.7.1.
- The CLI producer published 20 Avro events and received 20 broker acknowledgements.
- Flink 1.18 with the 1.18.1 Avro Confluent bundle decoded 18 clickstream and 6
  telemetry records in the bounded probe (including records from integration tests).
- Compose configuration, Python compilation, and diff whitespace checks passed.
- The existing Flink image plus the new connector was used for the probe. A full
  image rebuild and the downstream Iceberg/Fluss sinks were not exercised in this run.

The schema CI workflow runs deterministic local checks; Kafka/Registry integration
tests are opt-in. Nessie/dbt checks now run in the separate analytics workflow.
CDC adapters and the remaining governance work are still open.

References: [Flink 1.18 Confluent Avro format](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/table/formats/avro-confluent/),
[Avro schema resolution](https://avro.apache.org/docs/1.12.0/specification/#schema-resolution),
[Schema Registry API](https://docs.confluent.io/platform/current/schema-registry/develop/api.html).
