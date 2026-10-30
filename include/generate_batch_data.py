import csv
import random
import uuid
from datetime import datetime, timedelta
import os

OUTPUT_DIR = "/opt/airflow/include/landing_data"

CATEGORIES = ["Electronics", "Fashion", "Home & Kitchen", "Books", "Beauty"]
PAYMENT_METHODS = ["CREDIT_CARD", "UPI", "NET_BANKING", "DEBIT_CARD"]

def generate_daily_batch(execution_date_str: str, num_records: int = 1000):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_path = os.path.join(OUTPUT_DIR, f"orders_batch_{execution_date_str}.csv")
    
    base_date = datetime.strptime(execution_date_str, "%Y-%m-%d")
    
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
            
            # Anomaly injection for contract testing
            if i % 33 == 0:
                customer_id = ""  # Null customer breach
            elif i % 55 == 0:
                quantity = -1     # Negative quantity breach
                
            writer.writerow([order_id, customer_id, category, quantity, unit_price, order_ts, payment])
            
    print(f"Generated {num_records} records for batch {execution_date_str} at: {file_path}")
    return file_path