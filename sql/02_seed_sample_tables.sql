-- ==============================================================================
-- Lakeflow Dynamic Ingestion Framework - Seed Sample Configurations
-- ==============================================================================
-- Demonstrates registering different ingestion patterns across domains:
-- 1. CDC with soft/hard deletes (SCD1)
-- 2. Dimension with historical versioning (SCD2)
-- 3. High-throughput event logs (Append-only)
-- 4. Multi-domain isolation (Finance vs Sales)
--
-- This script is idempotent: running it multiple times safely upserts (MERGE)
-- configurations without creating duplicate records or altering created_at.
-- ==============================================================================

MERGE INTO system_metadata.ingestion.table_registry AS target
USING (
    -- --------------------------------------------------------------------------
    -- 1. Sales Orders: CDC feed with updates & deletes (SCD Type 1)
    -- --------------------------------------------------------------------------
    SELECT
        'sales_orders' AS table_id,
        'sales' AS domain,
        'parquet' AS source_format,
        '/Volumes/lakehouse_dev/landing/sales/orders/' AS source_path,
        '*.parquet' AS file_filter_pattern,
        'lakehouse_dev' AS target_catalog,
        'sales' AS target_schema,
        'orders' AS target_table_name,
        'CDC' AS ingestion_mode,
        ARRAY('order_id') AS primary_keys,
        'updated_at' AS sequence_by,
        'op_type = "D"' AS delete_condition,
        '1' AS scd_type,
        'addNewColumns' AS schema_evolution_mode,
        MAP('valid_total_amount', 'total_amount >= 0.0') AS expect_rules,
        MAP('valid_order_id', 'order_id IS NOT NULL') AS expect_or_drop_rules,
        MAP('valid_order_date', 'order_date <= CURRENT_DATE()') AS expect_or_fail_rules,
        TRUE AS is_active

    UNION ALL

    -- --------------------------------------------------------------------------
    -- 2. Sales Customers: Slowly Changing Dimension Type 2 (historical tracking)
    -- --------------------------------------------------------------------------
    SELECT
        'sales_customers' AS table_id,
        'sales' AS domain,
        'parquet' AS source_format,
        '/Volumes/lakehouse_dev/landing/sales/customers/' AS source_path,
        '*.parquet' AS file_filter_pattern,
        'lakehouse_dev' AS target_catalog,
        'sales' AS target_schema,
        'customers' AS target_table_name,
        'SCD2' AS ingestion_mode,
        ARRAY('customer_id') AS primary_keys,
        'modified_at' AS sequence_by,
        CAST(NULL AS STRING) AS delete_condition,
        '2' AS scd_type,
        'addNewColumns' AS schema_evolution_mode,
        MAP('has_email', 'email IS NOT NULL') AS expect_rules,
        MAP('valid_customer_id', 'customer_id IS NOT NULL') AS expect_or_drop_rules,
        CAST(NULL AS MAP<STRING, STRING>) AS expect_or_fail_rules,
        TRUE AS is_active

    UNION ALL

    -- --------------------------------------------------------------------------
    -- 3. Sales Clickstream: Append-only immutable web telemetry events
    -- --------------------------------------------------------------------------
    SELECT
        'sales_clickstream' AS table_id,
        'sales' AS domain,
        'parquet' AS source_format,
        '/Volumes/lakehouse_dev/landing/sales/clickstream/' AS source_path,
        '*.parquet' AS file_filter_pattern,
        'lakehouse_dev' AS target_catalog,
        'sales' AS target_schema,
        'clickstream' AS target_table_name,
        'APPEND' AS ingestion_mode,
        CAST(NULL AS ARRAY<STRING>) AS primary_keys,
        CAST(NULL AS STRING) AS sequence_by,
        CAST(NULL AS STRING) AS delete_condition,
        CAST(NULL AS STRING) AS scd_type,
        'addNewColumns' AS schema_evolution_mode,
        MAP('valid_session', 'session_id IS NOT NULL') AS expect_rules,
        MAP('valid_event_time', 'event_timestamp IS NOT NULL') AS expect_or_drop_rules,
        CAST(NULL AS MAP<STRING, STRING>) AS expect_or_fail_rules,
        TRUE AS is_active

    UNION ALL

    -- --------------------------------------------------------------------------
    -- 4. Finance Invoices: Finance domain with SCD Type 1
    -- --------------------------------------------------------------------------
    SELECT
        'finance_invoices' AS table_id,
        'finance' AS domain,
        'parquet' AS source_format,
        '/Volumes/lakehouse_dev/landing/finance/invoices/' AS source_path,
        '*.parquet' AS file_filter_pattern,
        'lakehouse_dev' AS target_catalog,
        'finance' AS target_schema,
        'invoices' AS target_table_name,
        'SCD1' AS ingestion_mode,
        ARRAY('invoice_id') AS primary_keys,
        'processed_at' AS sequence_by,
        'status = "VOID"' AS delete_condition,
        '1' AS scd_type,
        'addNewColumns' AS schema_evolution_mode,
        MAP('valid_due_date', 'due_date >= invoice_date') AS expect_rules,
        MAP('positive_amount', 'invoice_amount > 0') AS expect_or_drop_rules,
        MAP('valid_invoice_id', 'invoice_id IS NOT NULL') AS expect_or_fail_rules,
        TRUE AS is_active
) AS source
ON target.table_id = source.table_id
WHEN MATCHED THEN
    UPDATE SET
        target.domain = source.domain,
        target.source_format = source.source_format,
        target.source_path = source.source_path,
        target.file_filter_pattern = source.file_filter_pattern,
        target.target_catalog = source.target_catalog,
        target.target_schema = source.target_schema,
        target.target_table_name = source.target_table_name,
        target.ingestion_mode = source.ingestion_mode,
        target.primary_keys = source.primary_keys,
        target.sequence_by = source.sequence_by,
        target.delete_condition = source.delete_condition,
        target.scd_type = source.scd_type,
        target.schema_evolution_mode = source.schema_evolution_mode,
        target.expect_rules = source.expect_rules,
        target.expect_or_drop_rules = source.expect_or_drop_rules,
        target.expect_or_fail_rules = source.expect_or_fail_rules,
        target.is_active = source.is_active,
        target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        table_id,
        domain,
        source_format,
        source_path,
        file_filter_pattern,
        target_catalog,
        target_schema,
        target_table_name,
        ingestion_mode,
        primary_keys,
        sequence_by,
        delete_condition,
        scd_type,
        schema_evolution_mode,
        expect_rules,
        expect_or_drop_rules,
        expect_or_fail_rules,
        is_active,
        created_at,
        updated_at
    ) VALUES (
        source.table_id,
        source.domain,
        source.source_format,
        source.source_path,
        source.file_filter_pattern,
        source.target_catalog,
        source.target_schema,
        source.target_table_name,
        source.ingestion_mode,
        source.primary_keys,
        source.sequence_by,
        source.delete_condition,
        source.scd_type,
        source.schema_evolution_mode,
        source.expect_rules,
        source.expect_or_drop_rules,
        source.expect_or_fail_rules,
        source.is_active,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    );
