# Databricks notebook source
# ==============================================================================
# Lakeflow Pipeline - Sales Domain Custom Transformations (Gold Curated Layer)
# ==============================================================================
# Demonstrates custom domain business logic joining two Silver tables:
#   - silver_orders: CDC feed (SCD Type 1)
#   - silver_customers: Historical customer dimension (SCD Type 2)
#
# Compiled seamlessly into the unified pipeline DAG alongside dynamic_ingestion_engine.py.
# ==============================================================================

import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="gold_sales_orders_customer_summary",
    comment="Curated gold reporting table: enriched orders with current customer profile and tier",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "customer_id,order_date"
    }
)
def gold_sales_orders_customer_summary():
    """
    Joins silver_orders with the active customer record from silver_customers (SCD2).
    """
    # 1. Read Silver tables produced by dynamic_ingestion_engine.py
    orders = dlt.read("silver_orders")
    customers = dlt.read("silver_customers")

    # 2. Filter SCD Type 2 dimension to active/current version (__END_AT is NULL)
    # If __END_AT column is present from dlt.apply_changes SCD2, filter to current record
    if "__END_AT" in customers.columns:
        current_customers = customers.filter(F.col("__END_AT").isNull())
    else:
        current_customers = customers

    # 3. Perform the business join and select curated analytical columns
    return (
        orders.join(current_customers, on="customer_id", how="inner")
        .select(
            orders.order_id,
            orders.customer_id,
            current_customers.name.alias("customer_name"),
            current_customers.email.alias("customer_email"),
            current_customers.tier.alias("customer_tier"),
            orders.order_date,
            orders.total_amount,
            orders.status.alias("order_status"),
            F.current_timestamp().alias("_transformed_at")
        )
    )
