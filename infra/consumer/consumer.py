#Import requirements
import json
import boto3
import time
import os
from dotenv import load_dotenv 
import socket
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from botocore.exceptions import ClientError

load_dotenv(".env")

#Minio Connection
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD")
)

bucket_name = os.getenv("BUCKET", "bronze-transactions")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

def install_local_kafka_hostname_aliases():
    aliases = {
        "host.docker.internal": "127.0.0.1",
        "kafka": "127.0.0.1",
    }

    original_getaddrinfo = socket.getaddrinfo

    def patched_getaddrinfo(host, *args, **kwargs):
        return original_getaddrinfo(aliases.get(host, host), *args, **kwargs)

    socket.getaddrinfo = patched_getaddrinfo

def parse_bootstrap_servers():
    return [server.strip() for server in KAFKA_BOOTSTRAP_SERVERS.split(",") if server.strip()]

def ensure_bucket_exists():
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Using existing bucket {bucket_name}.")
        return
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code not in ("404", "NoSuchBucket", "NotFound"):
            raise

    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Created bucket {bucket_name}.")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"Using existing bucket {bucket_name}.")
        else:
            raise

def create_consumer(max_retries=20, retry_delay_seconds=3):
    bootstrap_servers = parse_bootstrap_servers()
    for attempt in range(1, max_retries + 1):
        try:
            return KafkaConsumer(
                "stock-quotes",
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="bronze-consumer",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=10000
            )
        except NoBrokersAvailable:
            print(
                f"Kafka broker not available at {bootstrap_servers} "
                f"(attempt {attempt}/{max_retries}). Retrying in {retry_delay_seconds}s..."
            )
            time.sleep(retry_delay_seconds)
    raise RuntimeError(
        f"Kafka broker is not reachable at {bootstrap_servers}. "
        "Start it with: docker compose -f infra/docker-compose.yml up -d zookeeper kafka"
    )

# Ensure bucket exists (idempotent)
ensure_bucket_exists()
install_local_kafka_hostname_aliases()

#Define Consumer
consumer = create_consumer()

print("Consumerstreaming and saving to MinIO...")

#Main Function
for message in consumer:
    record = message.value
    symbol = record.get("symbol", "unknown")
    ts = record.get("fetched_at",int(time.time()))
    key = f"{symbol}/{ts}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json"
    )
    print(f"Saved record for {symbol} = s3://{bucket_name}/{key}")
    
    
'''
•  Made MinIO bucket setup idempotent and explicit:
◦  Uses head_bucket + targeted ClientError handling.
◦  Treats BucketAlreadyOwnedByYou / BucketAlreadyExists as non-fatal.
•  Hardened Kafka consumer connectivity:
◦  Added KAFKA_BOOTSTRAP_SERVERS env support (default localhost:29092).
◦  Added hostname aliases for Kafka-advertised hosts (host.docker.internal, kafka → 127.0.0.1).
◦  Added retry loop on NoBrokersAvailable.
◦  Tuned timeout config correctly (request_timeout_ms=30000, api_version_auto_timeout_ms=10000).

Validation
•  Ran consumer with your project venv interpreter.
•  Confirmed:
◦  no bucket creation exception,
◦  no NoBrokersAvailable,
◦  consumer started and processed/saved records to MinIO successfully.
'''
                    