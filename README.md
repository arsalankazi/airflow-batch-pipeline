# Enterprise E-Commerce Batch ETL & Lakehouse Pipeline

An enterprise-grade, fault-tolerant batch ETL pipeline orchestrated with **Apache Airflow 2.8** and backed by **AWS S3 (LocalStack)**. The pipeline ingests daily transactional order batches, enforces schema and data contract validation, isolates anomalies into a quarantine dead-letter layer, and writes snappy-compressed, Hive-partitioned Parquet files into a curated Lakehouse tier.


---

## Architecture Overview
                  +-----------------------------+
                  |   Daily Synthetic Batch     |
                  |   (1,000 Records / Day)     |
                  +--------------+--------------+
                                 |
                                 v
                  +-----------------------------+
                  |   Airflow Orchestrator      |
                  |   (LocalExecutor + Postgres)|
                  +--------------+--------------+
                                 |
                                 v
                  +-----------------------------+
                  |   s3://ecommerce-raw-landing|
                  |   (Raw CSV Batch Drop)      |
                  +--------------+--------------+
                                 |
                                 v
                  +-----------------------------+
                  |   Data Contract Validation  |
                  |   & Fail-Fast Guardrail     |
                  +-------+-------------+-------+
                          |             |
       Anomalies (4.3%)   |             |  Clean Records (95.7%)
                          v             v
   +-----------------------+   +-----------------------------+
   | s3://ecommerce-       |   | s3://ecommerce-curated-lake |
   | quarantine/           |   | (Hive Snappy Parquet Lake)  |
   | (Dead-Letter DLQ)     |   | year=YYYY/month=MM/day=DD/  |
   +-----------------------+   +-----------------------------+

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


---

## Quickstart & Local Setup

### 1. Start Docker Containers
```powershell
docker compose up -d


2. Create LocalStack S3 Buckets
PowerShell
docker exec -it s3_storage awslocal s3 mb s3://ecommerce-raw-landing
docker exec -it s3_storage awslocal s3 mb s3://ecommerce-curated-lake
docker exec -it s3_storage awslocal s3 mb s3://ecommerce-quarantine


3. Register AWS Connection in Airflow
PowerShell
docker exec -it airflow_webserver airflow connections add "aws_default" --conn-type "aws" --conn-extra '{"endpoint_url": "http://s3_storage:4566", "aws_access_key_id": "test", "aws_secret_access_key": "test", "region_name": "us-east-1"}'


### 4. Trigger the DAG
1. Navigate to the Airflow Web UI at `http://localhost:8080`.
2. Login with configured Airflow Admin credentials (as specified in docker-compose.yaml).
3. Unpause `ecommerce_batch_etl_pipeline` and trigger the DAG.

Storage & Contract Verification
Verify raw landing files, Hive partitions, and isolated anomalies directly from terminal:

PowerShell
# Raw Landing Layer
docker exec -it s3_storage awslocal s3 ls s3://ecommerce-raw-landing/raw/ --recursive

# Curated Lakehouse Layer (Hive Parquet)
docker exec -it s3_storage awslocal s3 ls s3://ecommerce-curated-lake/orders/ --recursive

# Quarantine Layer (Anomalies)
docker exec -it s3_storage awslocal s3 ls s3://ecommerce-quarantine/quarantine/ --recursive
Run Automated Data Assertions
PowerShell
Get-Content .\tests\test_pipeline_assertions.py | docker exec -i airflow_scheduler python -
Expected Output:

Plaintext
PASS: Curated Lake Integrity (957 clean rows verified, 0 data contract violations)
PASS: Quarantine Integrity (43 anomalies correctly filtered without pipeline failure)

---
---

## Author

* **Name:** Arsalan Kazi
* **Role:** Data Engineer
* **Focus:** Distributed Systems, Lakehouse Architecture & Batch ETL Pipelines