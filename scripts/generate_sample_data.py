#!/usr/bin/env python3
"""
Sample Parquet Data Generator for Lakeflow Ingestion Testing
Generates mock Parquet files simulating:
1. Batch 1: Initial load of orders, customers, and clickstream events.
2. Batch 2: CDC updates and deletes (op_type = 'U', op_type = 'D') + bad records for DQ tests.

Can be run locally or in a Databricks notebook.
"""

import os
import datetime
from pathlib import Path

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("Please install pandas and pyarrow to run this script locally: pip install pandas pyarrow")
    exit(1)


def generate_orders_batch1(output_dir: str):
    """Initial load of sales orders."""
    os.makedirs(output_dir, exist_ok=True)
    data = [
        {"order_id": "ORD-1001", "customer_id": "CUST-001", "total_amount": 150.50, "order_date": "2026-09-20", "status": "COMPLETED", "op_type": "I", "updated_at": "2026-09-20 10:00:00"},
        {"order_id": "ORD-1002", "customer_id": "CUST-002", "total_amount": 89.99,  "order_date": "2026-09-21", "status": "PENDING",   "op_type": "I", "updated_at": "2026-09-21 11:30:00"},
        {"order_id": "ORD-1003", "customer_id": "CUST-003", "total_amount": 420.00, "order_date": "2026-09-22", "status": "SHIPPED",   "op_type": "I", "updated_at": "2026-09-22 14:15:00"},
    ]
    df = pd.DataFrame(data)
    file_path = os.path.join(output_dir, "orders_batch_001.parquet")
    df.to_parquet(file_path, index=False)
    print(f"[OK] Generated {file_path} with {len(df)} records.")


def generate_orders_batch2(output_dir: str):
    """CDC batch: Update ORD-1002, Delete ORD-1003, Insert ORD-1004, and add 1 invalid record."""
    os.makedirs(output_dir, exist_ok=True)
    data = [
        # Update ORD-1002 to COMPLETED
        {"order_id": "ORD-1002", "customer_id": "CUST-002", "total_amount": 89.99,  "order_date": "2026-09-21", "status": "COMPLETED", "op_type": "U", "updated_at": "2026-09-23 09:00:00"},
        # Delete ORD-1003 (op_type = 'D')
        {"order_id": "ORD-1003", "customer_id": "CUST-003", "total_amount": 420.00, "order_date": "2026-09-22", "status": "CANCELLED", "op_type": "D", "updated_at": "2026-09-23 10:00:00"},
        # New order
        {"order_id": "ORD-1004", "customer_id": "CUST-001", "total_amount": 310.25, "order_date": "2026-09-23", "status": "PENDING",   "op_type": "I", "updated_at": "2026-09-23 10:30:00"},
        # Invalid record: total_amount < 0 (tests expect_rules warning)
        {"order_id": "ORD-1005", "customer_id": "CUST-004", "total_amount": -50.00, "order_date": "2026-09-23", "status": "PENDING",   "op_type": "I", "updated_at": "2026-09-23 11:00:00"},
        # Invalid record: NULL order_id (tests expect_or_drop_rules)
        {"order_id": None,       "customer_id": "CUST-005", "total_amount": 100.00, "order_date": "2026-09-23", "status": "PENDING",   "op_type": "I", "updated_at": "2026-09-23 11:15:00"},
    ]
    df = pd.DataFrame(data)
    file_path = os.path.join(output_dir, "orders_batch_002_cdc.parquet")
    df.to_parquet(file_path, index=False)
    print(f"[OK] Generated {file_path} with {len(df)} records.")


def generate_customers(output_dir: str):
    """Customer dimension data for SCD Type 2 testing."""
    os.makedirs(output_dir, exist_ok=True)
    data = [
        {"customer_id": "CUST-001", "name": "Acme Corp",   "email": "contact@acme.com", "tier": "Gold",   "modified_at": "2026-09-01 08:00:00"},
        {"customer_id": "CUST-002", "name": "Beta LLC",    "email": "info@beta.com",    "tier": "Silver", "modified_at": "2026-09-01 08:00:00"},
        {"customer_id": "CUST-003", "name": "Gamma Inc",   "email": "ops@gamma.com",    "tier": "Bronze", "modified_at": "2026-09-01 08:00:00"},
    ]
    df = pd.DataFrame(data)
    file_path = os.path.join(output_dir, "customers_init.parquet")
    df.to_parquet(file_path, index=False)
    print(f"[OK] Generated {file_path} with {len(df)} records.")


if __name__ == "__main__":
    base_dir = os.environ.get("MOCK_DATA_DIR", "./sample_landing_data")
    print(f"Generating mock landing Parquet data in: {base_dir}")
    generate_orders_batch1(os.path.join(base_dir, "sales/orders"))
    generate_orders_batch2(os.path.join(base_dir, "sales/orders"))
    generate_customers(os.path.join(base_dir, "sales/customers"))
    print("\nSample Parquet data generation complete.")
