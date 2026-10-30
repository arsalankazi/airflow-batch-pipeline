import os
import io
import boto3
import pandas as pd
from botocore.client import Config

# Dynamic endpoint resolution: works inside container and local host
S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://s3-storage:4566")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

CURATED_BUCKET = "ecommerce-curated-lake"
QUARANTINE_BUCKET = "ecommerce-quarantine"

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
        config=Config(s3={"addressing_style": "path"})
    )

def test_curated_lake_integrity():
    s3 = get_s3_client()
    key = "orders/year=2026/month=10/day=30/orders_2026-10-30.parquet"
    
    obj = s3.get_object(Bucket=CURATED_BUCKET, Key=key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    
    # Contract Assertions
    assert len(df) > 900, f"Expected >900 clean rows, got {len(df)}"
    assert df["customer_id"].isna().sum() == 0, "Curated lake contains NULL customer_ids!"
    assert (df["quantity"] <= 0).sum() == 0, "Curated lake contains invalid quantities!"
    assert (df["unit_price"] <= 0).sum() == 0, "Curated lake contains invalid unit prices!"
    assert (df["total_amount"] <= 0).sum() == 0, "Curated lake contains invalid total amounts!"
    print(f" PASS: Curated Lake Integrity ({len(df)} clean rows verified, 0 data contract violations)")

def test_quarantine_integrity():
    s3 = get_s3_client()
    key = "quarantine/batch_date=2026-10-30/quarantined_orders.csv"
    
    obj = s3.get_object(Bucket=QUARANTINE_BUCKET, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    
    assert len(df) > 0, "Quarantine layer should have captured anomalies!"
    # Ensure all isolated records actually violated contract rules
    violations = df[(df["customer_id"].isna()) | (df["quantity"] <= 0) | (df["unit_price"] <= 0)]
    assert len(violations) == len(df), "Quarantine layer contains valid data!"
    print(f" PASS: Quarantine Integrity ({len(df)} anomalies correctly filtered without pipeline failure)")

if __name__ == "__main__":
    test_curated_lake_integrity()
    test_quarantine_integrity()