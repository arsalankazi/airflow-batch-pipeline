from datetime import datetime, timedelta
import os
import io
import csv
import random
import uuid
import pandas as pd
import boto3
from botocore.client import Config

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://s3-storage:4566")
AWS_REGION = "us-east-1"
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

RAW_BUCKET = "ecommerce-raw-landing"
CURATED_BUCKET = "ecommerce-curated-lake"
QUARANTINE_BUCKET = "ecommerce-quarantine"

# Synthetic daily fixture size — configurable via env so the same DAG can be
# exercised at different volumes without code edits. Single-node pandas
# in-memory validation → comfortable up to ~100K rows/day; beyond that the
# contract/quarantine logic ports 1:1 to PySpark.
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))

CATEGORIES = ["Electronics", "Fashion", "Home & Kitchen", "Books", "Beauty"]
PAYMENT_METHODS = ["CREDIT_CARD", "UPI", "NET_BANKING", "DEBIT_CARD"]

default_args = {
    "owner": "arsalankazi",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}

# ---------------------------------------------------------------
# Data Contract — single source of truth for validation rules.
# Anything failing this contract is routed to the quarantine DLQ.
# ---------------------------------------------------------------
DATA_CONTRACT = {
    "required_fields": [
        "order_id", "customer_id", "product_category",
        "quantity", "unit_price", "order_timestamp", "payment_method",
    ],
    # Added quantity & unit_price to explicitly catch nulls
    "non_null": ["order_id", "customer_id", "order_timestamp", "quantity", "unit_price"],
    "non_empty_string": ["customer_id"],
    "domain_rules": {
        # .fillna(False) ensures NA never silently drops a row
        "quantity":    lambda s: (s > 0).fillna(False),
        "unit_price":  lambda s: (s > 0).fillna(False),
        "customer_id": lambda s: s.astype(str).str.fullmatch(r"CUST_\d+"),
    },
}


def apply_contract(df):
    """Split df into (valid, quarantined) using DATA_CONTRACT."""
    missing = [c for c in DATA_CONTRACT["required_fields"] if c not in df.columns]
    if missing:
        raise ValueError(f"Schema breach — missing columns: {missing}")

    mask = pd.Series(True, index=df.index)

    for col in DATA_CONTRACT["non_null"]:
        mask &= df[col].notna()

    for col in DATA_CONTRACT["non_empty_string"]:
        mask &= df[col].astype(str).str.strip().ne("")
        mask &= df[col].astype(str).str.lower().ne("nan")

    for col, rule in DATA_CONTRACT["domain_rules"].items():
        # .fillna(False) defensive check in case domain rule generates NA
        mask &= rule(df[col]).fillna(False)

    return df[mask].copy(), df[~mask].copy()


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
        config=Config(s3={"addressing_style": "path"})
    )

def extract_and_generate_batch(ds, **kwargs):
    landing_dir = "/opt/airflow/include/landing_data"
    os.makedirs(landing_dir, exist_ok=True)
    file_path = os.path.join(landing_dir, f"orders_batch_{ds}.csv")
    
    base_date = datetime.strptime(ds, "%Y-%m-%d")
    num_records = BATCH_SIZE
    
    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "order_id",
            "customer_id",
            "product_category",
            "quantity",
            "unit_price",
            "order_timestamp",
            "payment_method"
        ])
        
        for i in range(num_records):
            order_id = str(uuid.uuid4())
            customer_id = f"CUST_{random.randint(1000, 9999)}"
            category = random.choice(CATEGORIES)
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(10.0, 500.0), 2)
            
            random_seconds = random.randint(0, 86399)
            order_ts = (base_date + timedelta(seconds=random_seconds)).isoformat()
            payment = random.choice(PAYMENT_METHODS)
            
            # Controlled Anomaly injection (~3%) for quarantine validation
            if i % 33 == 0:
                customer_id = ""  # Broken: Null customer
            elif i % 55 == 0:
                quantity = -1     # Broken: Negative quantity
                
            writer.writerow([order_id, customer_id, category, quantity, unit_price, order_ts, payment])
            
    print(f"Generated {num_records} records for batch {ds} at: {file_path}")
    return file_path

def ingest_to_raw_landing(ds, ti, **kwargs):
    local_file = ti.xcom_pull(task_ids="extract_and_generate_batch")
    s3 = get_s3_client()
    s3_key = f"raw/batch_date={ds}/orders.csv"
    
    with open(local_file, "rb") as f:
        s3.put_object(Bucket=RAW_BUCKET, Key=s3_key, Body=f)
    print(f"Successfully landed raw batch to s3://{RAW_BUCKET}/{s3_key}")
    return s3_key

def validate_and_quarantine(ds, ti, **kwargs):
    s3_key = ti.xcom_pull(task_ids="ingest_to_raw_landing")
    s3 = get_s3_client()
    
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=s3_key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    
    initial_count = len(df)
    
    # Contract-driven split: valid → curated lake, invalid → quarantine DLQ
    df_valid, df_quarantine = apply_contract(df)
    
    clean_count = len(df_valid)
    bad_count = len(df_quarantine)
    
    if bad_count > 0:
        quarantine_buffer = io.StringIO()
        df_quarantine.to_csv(quarantine_buffer, index=False)
        quarantine_key = f"quarantine/batch_date={ds}/quarantined_orders.csv"
        s3.put_object(
            Bucket=QUARANTINE_BUCKET,
            Key=quarantine_key,
            Body=quarantine_buffer.getvalue().encode("utf-8")
        )
        print(f"Quarantined {bad_count} anomaly records to s3://{QUARANTINE_BUCKET}/{quarantine_key}")

    bad_ratio = bad_count / initial_count if initial_count > 0 else 0
    if bad_ratio > 0.20:
        raise AirflowFailException(f"Circuit breaker tripped: Anomaly rate {bad_ratio:.2%} exceeds 20% SLA.")

    ti.xcom_push(key="clean_count", value=clean_count)
    ti.xcom_push(key="bad_count", value=bad_count)
    
    temp_valid_path = f"/opt/airflow/include/landing_data/valid_{ds}.csv"
    df_valid.to_csv(temp_valid_path, index=False)
    return temp_valid_path

def load_partitioned_parquet(ds, ti, **kwargs):
    valid_csv_path = ti.xcom_pull(task_ids="validate_and_quarantine")
    df = pd.read_csv(valid_csv_path)
    
    df["order_timestamp"] = pd.to_datetime(df["order_timestamp"])
    df["total_amount"] = (df["quantity"] * df["unit_price"]).round(2)
    
    dt = datetime.strptime(ds, "%Y-%m-%d")
    partition_key = f"orders/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/orders_{ds}.parquet"
    
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow", compression="snappy", index=False)
    
    s3 = get_s3_client()
    s3.put_object(
        Bucket=CURATED_BUCKET,
        Key=partition_key,
        Body=parquet_buffer.getvalue()
    )
    
    print(f"Loaded {len(df)} records into Curated Lake at s3://{CURATED_BUCKET}/{partition_key}")
    
    if os.path.exists(valid_csv_path):
        os.remove(valid_csv_path)

def log_pipeline_metrics(ds, ti, **kwargs):
    clean_count = ti.xcom_pull(task_ids="validate_and_quarantine", key="clean_count")
    bad_count = ti.xcom_pull(task_ids="validate_and_quarantine", key="bad_count")
    total = clean_count + bad_count
    
    print("=" * 60)
    print(f" PIPELINE EXECUTION AUDIT REPORT FOR BATCH: {ds}")
    print(f" Total Ingested Events : {total}")
    print(f" Curated Clean Events  : {clean_count} ({(clean_count/total)*100:.1f}%)")
    print(f" Quarantined Anomalies : {bad_count} ({(bad_count/total)*100:.1f}%)")
    print(f" Status                : SUCCESS - Curated Layer Clean")
    print("=" * 60)

with DAG(
    dag_id="ecommerce_batch_etl_pipeline",
    default_args=default_args,
    description="Production-Grade Batch Ingestion, Data Contract Validation, and Hive Parquet Lakehouse Pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 10, 25),
    catchup=False,
    tags=["data_engineering", "batch_etl", "localstack", "s3", "parquet"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_and_generate_batch",
        python_callable=extract_and_generate_batch,
    )

    t2 = PythonOperator(
        task_id="ingest_to_raw_landing",
        python_callable=ingest_to_raw_landing,
    )

    t3 = PythonOperator(
        task_id="validate_and_quarantine",
        python_callable=validate_and_quarantine,
    )

    t4 = PythonOperator(
        task_id="load_partitioned_parquet",
        python_callable=load_partitioned_parquet,
    )

    t5 = PythonOperator(
        task_id="log_pipeline_metrics",
        python_callable=log_pipeline_metrics,
    )

    t1 >> t2 >> t3 >> t4 >> t5