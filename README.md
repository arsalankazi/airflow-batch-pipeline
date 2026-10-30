# Enterprise E-Commerce Batch ETL & Lakehouse Pipeline

An enterprise-grade, fault-tolerant batch ETL pipeline orchestrated with **Apache Airflow 2.8** and backed by **AWS S3 (LocalStack)**. The pipeline ingests daily transactional order batches, enforces schema and data contract validation, isolates anomalies into a quarantine dead-letter layer, and writes snappy-compressed, Hive-partitioned Parquet files into a curated Lakehouse tier.

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph Ingestion_Batch ["Batch Generation & Extraction"]
        A[Daily Synthetic Batch<br>1,000 Records / Day]
    end

    subgraph Orchestration ["Apache Airflow Orchestrator"]
        B[Task 1: extract_and_generate_batch] --> C[Task 2: ingest_to_raw_landing]
        C --> D[Task 3: validate_and_quarantine]
        D --> E[Task 4: load_partitioned_parquet]
        E --> F[Task 5: log_pipeline_metrics]
    end

    subgraph Storage_Tier ["AWS S3 LocalStack Lakehouse"]
        S3_Raw[(s3://ecommerce-raw-landing<br>Raw Batch CSV)]
        S3_Curated[(s3://ecommerce-curated-lake<br>Hive Snappy Parquet<br>year=YYYY/month=MM/day=DD/)]
        S3_Quarantine[(s3://ecommerce-quarantine<br>Quarantine Layer / DLQ<br>Breached Contracts)]
    end

    A --> B
    C -. Raw Upload .-> S3_Raw
    S3_Raw -. Contract Read .-> D
    D -- Anomalies 4.3% --> S3_Quarantine
    D -- Clean Records 95.7% --> E
    E -. Partitioned Sink .-> S3_Curated


    ---

## Key Engineering Highlights

* **Automated Data Contracts:** Evaluates incoming records against non-null customer keys, positive inventory quantities (`quantity > 0`), and valid unit prices (`unit_price > 0`).
* **Quarantine / Dead-Letter Pattern:** Corrupted records are automatically isolated into `s3://ecommerce-quarantine/` without throwing unhandled exceptions or disrupting downstream tasks.
* **Fail-Fast Circuit Breaker:** Evaluates runtime anomaly ratios; if violations breach the 20% SLA ceiling, execution terminates immediately via `AirflowFailException`.
* **Hive-Partitioned Lakehouse:** Writes clean batches into Snappy-compressed Apache Parquet using Hive partition paths (`year=YYYY/month=MM/day=DD/`) to optimize partition pruning in downstream analytical queries.
* **Automated Testing Suite:** Implements programmatic assertion scripts (`test_pipeline_assertions.py`) to verify zero data pollution in the curated lake and 100% anomaly routing accuracy in the quarantine layer.

---

## Tech Stack

| Component | Technology | Version |
| :--- | :--- | :--- |
| **Workflow Orchestrator** | Apache Airflow | 2.8.1 |
| **Airflow Executor** | LocalExecutor | - |
| **Metadata Database** | PostgreSQL | 13 |
| **Cloud Storage Mock** | AWS S3 via LocalStack | 3.4 |
| **Data Processing** | Python, Pandas, PyArrow, Boto3 | 3.10 / 2.1 / 14.0 / 1.33 |
| **File Format** | Snappy-compressed Apache Parquet | - |
| **Containerization** | Docker, Docker Compose | - |

---

## Directory Structure

```text
airflow-batch-pipeline/
├── dags/
│   └── ecommerce_batch_etl.py       # DAG tasks, contract logic & S3 loader
├── include/
│   └── landing_data/                # Local runtime staging directory
├── logs/                            # Task execution logs
├── plugins/
├── tests/
│   └── test_pipeline_assertions.py  # Automated contract verification suite
├── docker-compose.yaml              # Multi-container orchestration specification
├── TROUBLESHOOTING.md               # Root cause analysis & deployment logs
└── README.md                        # Architectural documentation
```


---

## Quickstart & Local Setup

### 1. Start Docker Containers
```powershell
docker compose up -d
```
```markdown
### 2. Create LocalStack S3 Buckets
```powershell
docker exec -it s3_storage awslocal s3 mb s3://ecommerce-raw-landing
docker exec -it s3_storage awslocal s3 mb s3://ecommerce-curated-lake
docker exec -it s3_storage awslocal s3 mb s3://ecommerce-quarantine
```
```markdown
### 3. Register AWS Connection in Airflow
```powershell
docker exec -it airflow_webserver airflow connections add "aws_default" --conn-type "aws" --conn-extra '{"endpoint_url": "http://s3_storage:4566", "aws_access_key_id": "test", "aws_secret_access_key": "test", "region_name": "us-east-1"}'
```
```markdown
### 4. Trigger the DAG
1. Navigate to the Airflow Web UI at `http://localhost:8080`.
2. Login with configured Airflow Admin credentials (as specified in `docker-compose.yaml`).
3. Unpause `ecommerce_batch_etl_pipeline` and trigger the DAG.
```


---

## Storage & Contract Verification

Verify raw landing files, Hive partitions, and isolated anomalies directly from terminal:

```powershell
# Raw Landing Layer
docker exec -it s3_storage awslocal s3 ls s3://ecommerce-raw-landing/raw/ --recursive

# Curated Lakehouse Layer (Hive Parquet)
docker exec -it s3_storage awslocal s3 ls s3://ecommerce-curated-lake/orders/ --recursive

# Quarantine Layer (Anomalies)
docker exec -it s3_storage awslocal s3 ls s3://ecommerce-quarantine/quarantine/ --recursive
```


```markdown
### Run Automated Data Assertions
```powershell
Get-Content .\tests\test_pipeline_assertions.py | docker exec -i airflow_scheduler python -
```


```markdown
**Expected Output:**
```text
PASS: Curated Lake Integrity (957 clean rows verified, 0 data contract violations)
PASS: Quarantine Integrity (43 anomalies correctly filtered without pipeline failure)
```


```markdown
---

## Author

* **Name:** Arsalan Kazi
* **Role:** Data Engineer
* **Focus:** Distributed Systems, Lakehouse Architecture & Batch ETL Pipelines


