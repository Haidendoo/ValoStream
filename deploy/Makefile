.PHONY: up down restart status logs clean minio-init schema-check schema-register test-contracts test-contracts-integration
.PHONY: analytics-check analytics-nessie-check quality-gate

PYTHON ?= python3
SCHEMA_REGISTRY_URL ?= http://localhost:8082

schema-check:
	cd .. && $(PYTHON) -m producer.schema_registry check

schema-register:
	cd .. && $(PYTHON) -m producer.schema_registry register --url $(SCHEMA_REGISTRY_URL)

test-contracts:
	cd .. && $(PYTHON) -m unittest discover -s tests -v

test-contracts-integration:
	cd .. && VALOSTREAM_INTEGRATION=1 $(PYTHON) -m unittest discover -s tests -v

analytics-check:
	cd .. && $(PYTHON) -m ops.check_analytics

analytics-nessie-check:
	cd .. && PYTHONPATH=. $(PYTHON) tests/analytics/nessie_smoke.py

quality-gate:
	cd .. && $(PYTHON) -m ops.nessie_quality_gate

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

status:
	docker compose ps

logs:
	docker compose logs -f

minio-init:
	python3 -c "\
	import boto3; \
	s3 = boto3.client('s3', endpoint_url='http://localhost:9000', aws_access_key_id='admin', aws_secret_access_key='password123', region_name='us-east-1'); \
	s3.create_bucket(Bucket='warehouse') if 'warehouse' not in [b['Name'] for b in s3.list_buckets().get('Buckets', [])] else print('Bucket warehouse exists'); \
	print('MinIO warehouse bucket verified.')"
