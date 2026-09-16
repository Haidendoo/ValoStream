.PHONY: up down restart status logs clean minio-init

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
