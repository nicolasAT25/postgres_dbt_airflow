# End-to-End Data Engineering Pipeline: PostgreSQL + dbt + Airflow

A fully containerized data pipeline that ingests stock market data from a message broker, stores raw files in object storage, loads them into PostgreSQL, and transforms them with dbt — orchestrated end-to-end by Apache Airflow.

---

## Architecture

```
[ Kafka ] ──► [ MinIO (Bronze Layer) ] ──► [ Airflow DAG ]
                                                  │
                          ┌───────────────────────┴───────────────────────┐
                          ▼                                               ▼
               [ Docker PostgreSQL ]                          [ Local PostgreSQL ]
                 (airflow / raw)                               (etl_airflow_dbt / raw)
                          │                                               │
                          ▼                                               ▼
                  [ dbt — docker target ]                   [ dbt — dev target ]
                          │                                               │
               ┌──────────┴──────────┐                       ┌───────────┴──────────┐
          [staging]            [intermediate]            [staging]           [intermediate]
               └──────────┬──────────┘                       └───────────┬──────────┘
                           ▼                                              ▼
                        [marts]                                        [marts]
              (candlestick, kpis, treechart)               (candlestick, kpis, treechart)
```

---

## How the Two Postgres Instances Work

This pipeline writes and transforms data in **two separate PostgreSQL instances in parallel**:

### 🐳 Docker PostgreSQL

| Setting | Value |
|---|---|
| Host (inside Docker network) | `postgres:5432` |
| Host (from your machine) | `localhost:5433` |
| Database | `airflow` |
| User / Password | `airflow` / `airflow` |
| Schema (raw data) | `raw` |
| dbt target | `docker` |

Used for: Airflow's own metadata tables + mirrored pipeline data. Runs inside the Docker network — Airflow containers connect using the service name `postgres`.

### 💻 Local PostgreSQL

| Setting | Value |
|---|---|
| Host (inside Docker network) | `host.docker.internal:5432` |
| Host (from your machine) | `localhost:5432` |
| Database | `etl_airflow_dbt` |
| User / Password | `postgres` / `<your password>` |
| Schema (raw data) | `raw` |
| dbt target | `dev` |

Used for: persistent local analytics platform. Airflow containers reach it via Docker's special `host.docker.internal` DNS name that resolves to the host machine.

Both instances receive the **same raw data** and run the **same dbt models**, so they stay in sync. The local instance is the primary analytics target; the Docker instance mirrors it for containerized development.

> **Linux users:** `host.docker.internal` may not resolve automatically. Add `extra_hosts: ["host.docker.internal:host-gateway"]` to the Airflow services in `docker-compose.yml`.

---

## DAG Flow

```
task1: download_minio
        │
   ┌────┴────┐
task2      task3
(load →    (load →
 Docker PG) Local PG)
   └────┬────┘
        │
   ┌────┴──────────────────┐
task4                    task6
dbt_run_local            dbt_run_docker
(--target dev)           (--target docker)
   │                         │
task5                    task7
dbt_test_local           dbt_test_docker
```

---

## dbt Models

| Layer | Model | Materialization | Description |
|---|---|---|---|
| **Staging** | `stg_stock_quotes` | View | Cleans and types the raw JSONB payload |
| **Intermediate** | `int_stock_quotes` | Table | Enriched quotes with derived fields |
| **Marts** | `mart_candlestick` | Table | OHLC candlestick data (last 12 days per symbol) |
| **Marts** | `mart_general_kpis` | Table | Aggregated KPIs per company |
| **Marts** | `mart_treechart` | Table | Market share / weight per company |

### dbt Profiles (`infra/dbt_profiles/profiles.yml`)

Two targets are defined — both read credentials from environment variables at runtime:

| Target | Connects to | Host | Tasks |
|---|---|---|---|
| `dev` | Local PostgreSQL | `host.docker.internal:5432` | `dbt_run_local`, `dbt_test_local` |
| `docker` | Docker PostgreSQL | `postgres:5432` | `dbt_run_docker`, `dbt_test_docker` |

No credentials are hardcoded — all values come from `infra/.env` via `{{ env_var('VAR_NAME') }}`.

---

## Stack

| Tool | Version | Role |
|---|---|---|
| Apache Airflow | 2.9.3 | Orchestration |
| Apache Kafka | Confluent 7.4.1 | Event streaming |
| Zookeeper | Confluent 7.4.1 | Kafka coordination |
| MinIO | latest | S3-compatible object storage (Bronze layer) |
| PostgreSQL | 15 | Data platform (Docker + local) |
| dbt-core + dbt-postgres | 1.11.x | Data transformation |
| Kafdrop | latest | Kafka UI |
| Docker Compose | v2 | Local infrastructure |

---

## Project Structure

```
.
├── infra/
│   ├── Dockerfile                  # Extends apache/airflow:2.9.3 with dbt installed
│   ├── docker-compose.yml          # All services: Kafka, MinIO, Airflow, Postgres
│   ├── .env                        # Environment variables (credentials, config) — not committed
│   ├── .env.example                # Template for .env
│   ├── dbt_profiles/
│   │   └── profiles.yml            # Docker-aware dbt profiles (dev + docker targets)
│   ├── dags/
│   │   └── minio_to_postgres.py    # Main ETL + dbt DAG (7 tasks)
│   ├── logs/                       # Airflow task logs (auto-generated)
│   ├── plugins/                    # Custom Airflow plugins
│   ├── producer/                   # Kafka producer scripts
│   └── consumer/                   # Kafka consumer scripts
├── dbt_stocks/
│   └── dbt_stocks/
│       ├── dbt_project.yml         # Project config: schemas, materializations
│       ├── macros/
│       │   └── generate_schema.sql # Custom schema naming (uses DBT_ENV_NAME)
│       └── models/
│           ├── sources/            # __sources_stocks.yml — raw.stock_quotes_raw
│           ├── staging/            # stg_stock_quotes (view)
│           ├── intermediate/       # int_stock_quotes (table)
│           └── marts/              # mart_candlestick, mart_general_kpis, mart_treechart
├── requirements.txt
└── README.md
```

---

## Getting Started

### Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- PostgreSQL 15+ installed locally
- Python 3.10+

### 1. Configure environment variables

```bash
cp infra/.env.example infra/.env
```

Edit `infra/.env` filling in your values (see `infra/.env.example` as reference):

```env
# Finnhub API
API_KEY=your_api_key

# MinIO
MINIO_ENDPOINT=http://minio:9000
MINIO_ROOT_USER=
MINIO_ROOT_PASSWORD=
BUCKET=bronze-transactions

# PostgreSQL — Docker (used by Airflow internals + pipeline)
PG_HOST=postgres
PG_PORT=5432
PG_USER=
PG_PASSWORD=
PG_DB=
PG_SCHEMA=
PG_TABLE=stock_quotes_raw

# PostgreSQL — Local (your host machine)
PG_HOST_LOCAL=host.docker.internal
PG_PORT_LOCAL=5432
PG_USER_LOCAL=
PG_PASSWORD_LOCAL=
PG_DB_LOCAL=
PG_SCHEMA_LOCAL=
PG_TABLE_LOCAL=stock_quotes_raw

# dbt
DBT_ENV_NAME=dev
```

### 2. Prepare local PostgreSQL

Your local Postgres must accept connections from Docker containers:

**`postgresql.conf`** — allow connections on all interfaces:
```
listen_addresses = '*'
```

**`pg_hba.conf`** — allow Docker subnet:
```
host    all    all    172.16.0.0/12    md5
```

Create the target database if it doesn't exist:
```bash
psql -U postgres -c "CREATE DATABASE etl_airflow_dbt;"
```

Restart PostgreSQL after config changes:
```bash
brew services restart postgresql@17   # macOS
sudo systemctl restart postgresql     # Linux
```

### 3. Build and start the infrastructure

The Airflow image is extended via `Dockerfile` to include `dbt-core` and `dbt-postgres`:

```dockerfile
FROM apache/airflow:2.9.3
RUN pip install --no-cache-dir dbt-core dbt-postgres python-dotenv
```

```bash
cd infra
docker compose build        # builds the custom airflow-dbt:2.9.3 image with dbt installed
docker compose up -d        # starts all services in detached (background) mode
```

> `docker compose up -d` starts all containers in the background. You can check their status anytime with `docker compose ps` and follow logs with `docker compose logs -f`.

To verify dbt is available inside the Airflow container:
```bash
docker exec airflow-scheduler dbt --version
```

To stop all services:
```bash
docker compose down          # stops and removes containers (data volumes are preserved)
docker compose down -v       # also removes volumes (wipes all PostgreSQL data)
```

### 4. Initialize Airflow (first run only)

```bash
docker exec airflow-webserver airflow db migrate
docker exec airflow-webserver airflow users create \
  --username airflow --password airflow \
  --firstname Air --lastname Flow \
  --role Admin --email admin@example.com
```

### 5. Access the UIs

| Service | URL | Default Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| MinIO Console | http://localhost:9001 | `admin` / `password123` |
| MinIO S3 API | http://localhost:9002 | — |
| Kafdrop | http://localhost:9000 | — |
| Docker PostgreSQL | `localhost:5433` | `airflow` / `airflow` |
| Local PostgreSQL | `localhost:5432` | `postgres` / `<your password>` |

### 6. Create the MinIO bucket

In the MinIO UI (http://localhost:9001), create a bucket named `bronze-transactions`.

### 7. Trigger the DAG

In the Airflow UI, enable and trigger the `minio_to_postgres` DAG. It runs 7 tasks:

| Task | What it does |
|---|---|
| `download_minio` | Downloads JSON files from MinIO to `/tmp/minio_downloads` |
| `load_postgres` | Inserts raw JSONB records into Docker PostgreSQL (`airflow.raw.stock_quotes_raw`) |
| `load_postgres_local` | Inserts raw JSONB records into Local PostgreSQL (`etl_airflow_dbt.raw.stock_quotes_raw`) |
| `dbt_run_local` | Runs all dbt models against local PostgreSQL (`--target dev`) |
| `dbt_test_local` | Runs all dbt tests against local PostgreSQL |
| `dbt_run_docker` | Runs all dbt models against Docker PostgreSQL (`--target docker`) |
| `dbt_test_docker` | Runs all dbt tests against Docker PostgreSQL |

### 8. Run dbt locally (optional, outside Airflow)

```bash
cd dbt_stocks/dbt_stocks
dbt run --target dev     # materializes models in local PostgreSQL
dbt test --target dev    # runs data quality tests
dbt docs generate        # generates documentation
dbt docs serve           # serves docs at http://localhost:8081
```

---

## Key Configuration Details

### Why `host.docker.internal`?

Docker containers run in an isolated network. They cannot reach `localhost` — that resolves to the container itself, not your machine. `host.docker.internal` is Docker's special DNS name that always resolves to the host machine's IP, allowing containers to reach services running locally (like your PostgreSQL).

### How dbt credentials are injected

The `infra/dbt_profiles/profiles.yml` uses dbt's `env_var()` function to read all values from environment variables:

```yaml
dev:
  type: postgres
  host: "{{ env_var('PG_HOST_LOCAL') }}"
  port: "{{ env_var('PG_PORT_LOCAL') | int }}"
  ...
```

The Airflow `BashOperator` tasks pass these env vars explicitly via `env=DBT_BASE_ENV`, which is populated from the `.env` file loaded at DAG parse time.

### Port mapping

| Service | Internal (Docker network) | External (your machine) |
|---|---|---|
| Kafka | `kafka:9092` | `localhost:29092` |
| MinIO S3 API | `minio:9000` | `localhost:9002` |
| MinIO Console | `minio:9001` | `localhost:9001` |
| Airflow | `airflow-webserver:8080` | `localhost:8080` |
| Docker PostgreSQL | `postgres:5432` | `localhost:5433` |
| Kafdrop | `kafdrop:9000` | `localhost:9000` |

---

## Roadmap

- [x] Kafka + Zookeeper setup
- [x] MinIO object storage (Bronze layer)
- [x] Airflow orchestration with custom Docker image (dbt included)
- [x] Raw data ingestion into Docker + local PostgreSQL in parallel
- [x] dbt staging, intermediate, and mart models
- [x] dbt tests
- [x] Dual-target dbt runs (Docker + local) via Airflow
- [ ] Incremental dbt models to avoid full refreshes
- [ ] Data quality checks (dbt custom tests / Great Expectations)
- [ ] Dashboard / BI layer (Tableau, Power BI, Amazon QuickSight, Metabase, Sigma, etc.)
