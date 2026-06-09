#Import requirements
import time
import json
import os
from dotenv import load_dotenv   # optional, only needed for local runs outside Docker
import socket
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError

load_dotenv(".env")  # no-op if vars are already in the environment (i.e. inside Docker)

#Define variables for API
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]
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

def create_producer(max_retries=20, retry_delay_seconds=3):
    bootstrap_servers = parse_bootstrap_servers()
    for attempt in range(1, max_retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=10000,
                max_block_ms=10000
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
install_local_kafka_hostname_aliases()

#Initial Producer
producer = create_producer()

#Retrive Data
def fetch_quote(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        data["symbol"] = symbol
        data["fetched_at"] = int (time.time())
        return data
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

#Looping and Pushing to Stream. 
# To break this while loop, kill the producer itself.
while True:
    for symbol in SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            for attempt in range(1, 4):
                try:
                    producer.send("stock-quotes", value=quote).get(timeout=10) # Topic created in Kafdrop
                    print(f"Produced: {quote}")
                    break
                except KafkaTimeoutError as e:
                    print(f"Kafka timeout while producing {symbol} (attempt {attempt}/3): {e}")
                    if attempt < 3:
                        producer = create_producer(max_retries=3, retry_delay_seconds=2)
                    else:
                        print(f"Dropping quote for {symbol} after 3 failed attempts.")
    time.sleep(6)   # Finnhub allows 60 calls per minute.
    
    
'''
•  Added hostname aliasing for Kafka-advertised hosts:
◦  host.docker.internal → 127.0.0.1
◦  kafka → 127.0.0.1
•  Improved bootstrap parsing to support comma-separated KAFKA_BOOTSTRAP_SERVERS.
•  Tightened producer timeouts (request_timeout_ms, max_block_ms) to fail faster.
•  Made sends synchronous with .get(timeout=10) so failures are explicit.
•  Added retry/recreate logic on KafkaTimeoutError per message (3 attempts) with clear logs.


IF GETTING ERROR: Kafka broker not available at ['localhost:29092']
DO: docker compose -f infra/docker-compose.yml up -d zookeeper kafka
'''