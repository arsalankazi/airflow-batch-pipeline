# Engineering Incident & Troubleshooting Log

This document records operational hurdles, root causes, and architectural resolutions encountered during the local deployment of the Airflow Batch Lakehouse Pipeline.

---

### Incident 1: Airflow Database Initialization Hang
* **Symptom:** Webserver and scheduler containers exited or rebooted endlessly during boot.
* **Root Cause:** Race condition where Airflow containers attempted metadata migration before PostgreSQL was ready to accept network sockets.
* **Resolution:** Implemented `pg_isready` container healthchecks in `docker-compose.yaml` combined with explicit service dependencies (`depends_on: { condition: service_healthy }`).

### Incident 2: LocalStack S3 Pro Licensing Conflict (Exit Code 55)
* **Symptom:** LocalStack container exited abruptly with code 55 reporting license activation requirements.
* **Root Cause:** Default latest tags in newer LocalStack releases trigger pro trial handshakes for certain extensions.
* **Resolution:** Pinned the S3 container explicitly to `localstack/localstack:3.4` community edition and isolated pure S3 emulation via `SERVICES=s3`.

### Incident 3: MinIO Deprecation & Access Denial
* **Symptom:** `Error pull access denied for minio/minio`.
* **Root Cause:** Upstream image tag deprecation on Docker Hub requiring authenticated accounts.
* **Resolution:** Standardized the mock storage tier strictly on AWS-compatible LocalStack S3 endpoints (`http://s3-storage:4566`).

### Incident 4: Non-Root Airflow Runtime Package Injection
* **Symptom:** `You are running pip as root` and subsequent `/root/bin/pip: Permission denied`.
* **Root Cause:** Airflow 2.8+ production images run under non-root UID 50000 (`airflow`), disabling direct root package installation within running containers.
* **Resolution:** Injected libraries (`pandas`, `pyarrow`, `boto3`) natively via `_PIP_ADDITIONAL_REQUIREMENTS` in compose configurations to guarantee immutable, persistent container dependencies.

### Incident 5: Scheduler DagBag Discovery Vacuum
* **Symptom:** Web UI displayed `No results` with `dags list` reporting `No data found`.
* **Root Cause:** Working DAG definition had not yet been synchronized into the host `./dags` volume mount path.
* **Resolution:** Placed `ecommerce_batch_etl.py` into `./dags`, triggering automatic DagFileProcessor pick-up within 15 seconds.

### Incident 6: Execution Context Namespace Drift (`ModuleNotFoundError: 'include'`)
* **Symptom:** Task 1 (`extract_and_generate_batch`) failed with attempt 3/3 error.
* **Root Cause:** Container PYTHONPATH did not include relative subpackage paths for internal batch data generators.
* **Resolution:** Consolidated the batch generation logic directly within the DAG file to guarantee atomic execution and zero import path friction across Celery/Local workers.

### Incident 7: Container-Network Endpoint Resolution in Test Harness
* **Symptom:** Assertion tests failed with connection refusal to `localhost:4566`.
* **Root Cause:** Tests running inside Docker containers cannot resolve other containers via `localhost`.
* **Resolution:** Refactored endpoint resolution using `os.getenv("AWS_ENDPOINT_URL", "http://s3-storage:4566")`, enabling seamless execution both locally and inside container networks.