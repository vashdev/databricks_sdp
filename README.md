# Databricks Lakeflow Dynamic Ingestion Framework

A production-grade, metadata-driven ingestion framework built on **Databricks Lakeflow Pipelines** (Delta Live Tables) to ingest hundreds of tables without creating individual notebooks per table.

---

### Architecture Overview (Hub-and-Spoke Model)

This framework implements a **Hub-and-Spoke** architecture for data ingestion:
- **The Central Hub**: A single, shared declarative ingestion engine ([`pipelines/dynamic_ingestion_engine.py`](file:///Users/gforce/Documents/source/lakeflow/pipelines/dynamic_ingestion_engine.py)) providing unified Auto Loader streaming, SCD1/SCD2/CDC `dlt.apply_changes` mechanics, and Data Quality expectation layers.
- **The Domain Spokes**: Independent business domains (Sales, Finance, etc.) manage their table registrations via declarative YAML files (`configs/domains/*.yml`) and execute on isolated Lakeflow pipelines with dedicated compute and blast-radius boundaries.

```
                  +---------------------------------------------+
                  |         Domain YAML Configurations          |
                  |     configs/domains/{sales,finance}.yml     |
                  +---------------------------------------------+
                                        |
                 Parsed at pipeline initialization (GitOps driven)
                                        v
+-----------------------------------------------------------------------------------------+
| [THE HUB] Shared Engine: dynamic_ingestion_engine.py                                    |
|                                                                                         |
|   1. Bronze Auto Loader Stream                                                          |
|      cloudFiles (Parquet) -> Schema Evolution ('addNewColumns') -> _rescued_data        |
|                                                                                         |
|   2. Data Quality Layer                                                                 |
|      @dlt.expect_all (Warn) | @dlt.expect_all_or_drop (Drop) | @dlt.expect_all_or_fail   |
|                                                                                         |
|   3. Silver Curated Layer                                                               |
|      dlt.apply_changes (SCD1, SCD2, CDC with Deletes) OR Dynamic Append Stream          |
+-----------------------------------------------------------------------------------------+
        |                                                                 |
        v                                                                 v
+----------------------------------+             +----------------------------------+
| [SPOKE 1] Sales Pipeline         |             | [SPOKE 2] Finance Pipeline       |
| (Orders, Customers, Clicks, ...) |             | (Invoices, GL, Payments, ...)    |
| Config: configs/domains/sales.yml|             | Config: configs/domains/finance. |
+----------------------------------+             +----------------------------------+
```

---

## Project Structure

```
lakeflow/
├── README.md                              # Complete setup and operational guide
├── databricks.yml                         # Databricks Asset Bundle (DAB) definition
├── .gitignore
├── configs/
│   └── domains/
│       ├── sales.yml                      # Sales domain table configs (CDC, SCD2, Append)
│       └── finance.yml                    # Finance domain table configs (SCD1)
├── pipelines/
│   └── dynamic_ingestion_engine.py        # Central Hub: Shared Lakeflow dynamic engine
├── transformations/                       # Custom Business Logic & Multi-Table Joins
│   └── sales/
│       └── sales_mart.py                  # Gold layer: joins silver_orders + silver_customers
├── resources/
│   └── lakeflow_pipelines.yml             # DABs resource specs for domain spoke pipelines
├── sql/                                   # Optional / Legacy Unity Catalog control table
│   ├── 01_create_control_table.sql        # DDL for optional SQL metadata control table
│   └── 02_seed_sample_tables.sql          # Idempotent MERGE seed script for SQL table
└── scripts/
    └── generate_sample_data.py            # Local/Databricks mock Parquet test data generator
```

---

## Quick Start & Deployment Guide

### Prerequisites
- Databricks CLI v0.200.0 or later installed and configured (`databricks auth login`).
- Unity Catalog enabled workspace with permissions to create tables and volumes.
- Access to create Delta Live Tables / Lakeflow pipelines.

---

### Step 1: Define Domain Configurations in YAML

Domain configurations live in [`configs/domains/`](file:///Users/gforce/Documents/source/lakeflow/configs/domains/):
- [`configs/domains/sales.yml`](file:///Users/gforce/Documents/source/lakeflow/configs/domains/sales.yml): Configures `sales_orders` (CDC), `sales_customers` (SCD2), and `sales_clickstream` (Append).
- [`configs/domains/finance.yml`](file:///Users/gforce/Documents/source/lakeflow/configs/domains/finance.yml): Configures `finance_invoices` (SCD1).

---

### Step 2: Deploy via Databricks Asset Bundles (DABs)

Validate and deploy the pipelines into your development workspace:

```bash
# Validate bundle configuration
databricks bundle validate -t dev

# Deploy pipelines to development target
databricks bundle deploy -t dev

# Run the Sales pipeline
databricks bundle run sales_ingestion_pipeline -t dev
```

For staging or production:
```bash
databricks bundle deploy -t prod
```

---

## Zero-Code Table Onboarding (GitOps)

To onboard a new table into an existing domain, **domain teams simply submit a Git Pull Request editing their domain YAML file**:

1. **Place Parquet Files**: Deposit incoming Parquet files into the raw landing path or UC Volume:
   ```
   /Volumes/<catalog>/landing/<domain>/<table_name>/
   ```

2. **Add Entry to Domain YAML** (e.g. in `configs/domains/sales.yml`):
   ```yaml
   - table_id: sales_inventory
     target_table_name: inventory
     source_path: "/Volumes/lakehouse_dev/landing/sales/inventory/"
     file_filter_pattern: "*.parquet"
     ingestion_mode: SCD1
     primary_keys:
       - sku_id
       - warehouse_id
     sequence_by: updated_at
     expect_rules:
       positive_stock: "stock_quantity >= 0"
     is_active: true
   ```

3. **Deploy & Automatic Discovery**: Once merged via CI/CD, the next pipeline run reads the YAML, discovers `inventory`, creates `bronze_inventory`, enforces data quality expectations, and creates `silver_inventory` via `dlt.apply_changes`.

---

## Custom Transformations & Multi-Table Joins (Gold Layer)

While [`pipelines/dynamic_ingestion_engine.py`](pipelines/dynamic_ingestion_engine.py) dynamically handles Bronze and Silver ingestion, custom business logic and multi-table joins (Gold layer) are placed in dedicated domain transformation scripts under [`transformations/`](transformations/):

- **Example File**: [`transformations/sales/sales_mart.py`](transformations/sales/sales_mart.py) joins `silver_orders` (CDC) with `silver_customers` (SCD2) to produce `gold_sales_orders_customer_summary`.
- **Pipeline Wiring**: Added to the pipeline's `libraries` in [`resources/lakeflow_pipelines.yml`](resources/lakeflow_pipelines.yml):
  ```yaml
  libraries:
    - notebook:
        path: ../pipelines/dynamic_ingestion_engine.py   # Ingests Bronze & Silver
    - notebook:
        path: ../transformations/sales/sales_mart.py     # Joins Silver -> Gold
  ```
- **Unified DAG**: Delta Live Tables automatically links the dependency graph across both files without any custom orchestration.

---

## Supported Ingestion Modes

| Mode | Behavior | Required Fields in Registry |
| :--- | :--- | :--- |
| **`CDC`** | Handles Inserts, Updates, and Deletes from source logs. | `primary_keys`, `sequence_by`, `delete_condition` (e.g., `op_type = 'D'`) |
| **`SCD1`** | Deduplicates and upserts records to maintain the latest state. | `primary_keys`, `sequence_by` (e.g., `updated_at`) |
| **`SCD2`** | Tracks full historical versioning (`__START_AT`, `__END_AT`). | `primary_keys`, `sequence_by`, `scd_type = '2'` |
| **`APPEND`** | Pure append-only streaming table for high-throughput events. | `ingestion_mode = 'APPEND'` (keys optional) |

---

## Schema Evolution & Rescued Data

- **Automatic Column Additions**: Powered by `cloudFiles.schemaEvolutionMode = 'addNewColumns'`. Any new column arriving in raw Parquet is seamlessly appended to the Bronze Delta table.
- **Corrupt / Colliding Data**: Handled without crashing pipelines via `_rescued_data`. Any column with type mismatches or unparseable JSON is safely retained in the `_rescued_data` column for investigation.
- **Audit Columns**: Every Bronze table automatically records:
  - `_ingested_at`: Timestamp when the record was processed by the pipeline.
  - `_source_file_path`: Exact file URI from `_metadata.file_path`.
  - `_source_file_modification_time`: File modification timestamp from `_metadata.file_modification_time`.

---

## Data Quality Expectations Monitoring

You can inspect expectation metrics and validation rates directly using Delta Live Tables event logs:

```sql
SELECT 
    timestamp,
    details:flow_definition.output_dataset AS dataset,
    details:flow_progress.data_quality.expectations AS expectations
FROM event_log("<pipeline-id>")
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC;
```
