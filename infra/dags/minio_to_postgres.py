import os
from dotenv import load_dotenv
import json
import boto3
import psycopg2       #Psycopg 2.
from airflow import DAG
#from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

load_dotenv()

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET = os.getenv("BUCKET", "bronze-transactions")
LOCAL_DIR = "/tmp/minio_downloads"  # use absolute path for Airflow

PG_CONFIGS = {
    "docker": {
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT"),
        "dbname": os.getenv("PG_DB"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "schema": os.getenv("PG_SCHEMA"),
        "table": os.getenv("PG_TABLE"),
    },
    "local": {
        "host": "host.docker.internal",  # or 172.17.0.1 on Linux
        "port": "5432",
        "dbname": "etl_airflow_dbt",
        "user": "postgres",
        "password": "nicopostgres",
        "schema": "staging",
        "table": "stock_quotes_raw",
    },
}

# ── Task 1: Download files from MinIO ────────────────────────
def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD
    )
    objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    local_files = []
    for obj in objects:
        key = obj["Key"]
        local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
        s3.download_file(BUCKET, key, local_file)
        print(f"Downloaded {key} -> {local_file}")
        local_files.append(local_file)
    return local_files

# ── Task 2-3: Load JSON files into Docker and local PostgreSQL instances ──────────────────
def load_to_postgres(target: str, **kwargs):
    cfg = PG_CONFIGS[target]
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")

    if not local_files:
        print("No files to load.")
        return

    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
    )
    cur = conn.cursor()
    schema, table = cfg["schema"], cfg["table"]

    cur.execute(f"""
                
    CREATE SCHEMA IF NOT EXISTS staging;
                
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        id          BIGSERIAL PRIMARY KEY,
        source_file TEXT        NOT NULL,
        raw_data    JSONB       NOT NULL,
        loaded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """)

    inserted = 0
    skipped  = 0
    for filepath in local_files:
        with open(filepath, "r", encoding="utf-8") as fh:
            content = fh.read().strip()
            try:
                payload = json.loads(content)
                records = payload if isinstance(payload, list) else [payload]
            except json.JSONDecodeError:
                records = [json.loads(line) for line in content.splitlines() if line.strip()]

        for record in records:
            cur.execute(
                f"""
                INSERT INTO {schema}.{table} (source_file, raw_data)
                VALUES (%s, %s)
                --ON CONFLICT ON CONSTRAINT uq_{table}_source_payload DO NOTHING
                """,
                (os.path.basename(filepath), json.dumps(record)),
            )
            if cur.rowcount:
                inserted += 1
            else:
                skipped += 1

        print(f"Loaded {len(records)} records from {filepath}")

    conn.commit()
    print(f"Rows inserted: {inserted} | Skipped (duplicates): {skipped}")
    cur.close()
    conn.close()

# ── DAG definition ────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "minio_to_postgres",
    default_args=default_args,
    #schedule="*/1 * * * *",  # every 1 minutes
    schedule="0 0 * * *",  # every day at 00:00 hrs
    #schedule="@once",
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_postgres",
        python_callable=load_to_postgres,
        op_kwargs={"target": "docker"},  # <-- selects the config
        #provide_context=True,
    )
    
    task3 = PythonOperator(
        task_id="load_postgres_local",
        python_callable=load_to_postgres,
        op_kwargs={"target": "local"},   # <-- selects the config
        #provide_context=True,
    )

    task1 >> [task2 , task3]