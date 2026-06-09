# End-to-End Data Engineering Pipeline: PostgreSQL + dbt + Airflow

A fully containerized data pipeline that ingests stock market data from a message broker, stores raw files in object storage, and loads them into PostgreSQL — with transformation and orchestration layers built on dbt and Airflow.

---

## Architecture

```
[ Kafka ] ──► [ MinIO (Bronze Layer) ] ──► [ Airflow DAG ] ──► [ PostgreSQL ]
                                                                      │
                                                               (staging schema)
                                                                      │
                                                               [ dbt (pending) ]
                                                                      │
                                                          ┌───────────┴───────────┐
                                                     [ marts ]              [ reports ]
```

### Components

| Layer | Tool | Role |
|---|---|---|
| **Ingestion** | Kafka + Zookeeper | Streams real-time stock quote events |
| **Object Storage** | MinIO (S3-compatible) | Stores raw JSON files in the `bronze-transactions` bucket |
| **Orchestration** | Apache Airflow | Schedules and monitors the pipeline DAGs |
| **Raw Storage** | PostgreSQL (`staging.stock_quotes_raw`) | Lands raw JSONB payloads from MinIO |
| **Transformation** | dbt *(pending)* | Models and tests data from staging into marts |
| **Observability** | Kafdrop | UI for inspecting Kafka topics and messages |

---

## Pipeline Flow

1. **Kafka producer** publishes stock quote events to a Kafka topic.
2. **MinIO** receives and stores the raw JSON files in the `bronze-transactions` bucket.
3. **Airflow DAG** (`minio_to_postgres`) runs on a schedule and:
   - Downloads files from MinIO to a temp directory.
   - Loads each JSON record into `staging.stock_quotes_raw` as JSONB.
   - Writes to **both** the Docker Postgres and local Postgres in parallel.
4. **dbt** *(coming soon)* will transform the raw staging data into clean, tested, documented models.

---

## Stack

- **Apache Airflow 2.9.3** — orchestration
- **Apache Kafka** (Confluent) — event streaming
- **MinIO** — S3-compatible object storage
- **PostgreSQL 15** — data platform (Docker + local)
- **dbt** — data transformation *(pending)*
- **Kafdrop** — Kafka UI
- **Docker Compose** — local infrastructure

---

## Project Structure

```
.
├── infra/
│   ├── docker-compose.yml      # All services: Kafka, MinIO, Airflow, Postgres
│   ├── dags/
│   │   └── minio_to_postgres.py  # Main ETL DAG
│   ├── logs/                   # Airflow task logs
│   └── plugins/                # Custom Airflow plugins
├── requirements.txt
└── README.md
```

---

## Getting Started

### 1. Start the infrastructure

```bash
cd infra
docker compose up -d
```

### 2. Access the UIs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| MinIO Console | http://localhost:9001 | `admin` / `password123` |
| Kafdrop | http://localhost:9000 | — |

### 3. Create the MinIO bucket

In the MinIO UI, create a bucket named `bronze-transactions`.

### 4. Trigger the DAG

In the Airflow UI, enable and trigger the `minio_to_postgres` DAG.

---

## Roadmap

- [x] Kafka + Zookeeper setup
- [x] MinIO object storage
- [x] Airflow orchestration
- [x] Raw data ingestion into PostgreSQL (Docker + local)
- [ ] dbt staging models
- [ ] dbt mart models with tests and documentation
- [ ] Data quality checks
