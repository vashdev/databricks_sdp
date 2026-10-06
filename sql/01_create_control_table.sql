-- ==============================================================================
-- Lakeflow Dynamic Ingestion Framework - Control Table DDL
-- ==============================================================================
-- This table acts as the centralized control plane for all domain pipelines.
-- Pipelines query this table at initialization to dynamically register
-- Bronze and Silver streaming tables.
-- ==============================================================================

-- Create metadata schema in Unity Catalog
CREATE SCHEMA IF NOT EXISTS system_metadata.ingestion
COMMENT 'Central metadata registry for Lakeflow dynamic ingestion pipelines';

-- Create table registry
CREATE TABLE IF NOT EXISTS system_metadata.ingestion.table_registry (
    table_id STRING NOT NULL COMMENT 'Unique identifier across the enterprise (e.g., sales_orders)',
    domain STRING NOT NULL COMMENT 'Domain grouping for pipeline isolation (e.g., sales, finance, inventory)',
    
    -- Source Storage Configuration
    source_format STRING DEFAULT 'parquet' COMMENT 'File format in raw landing (e.g., parquet, json, csv)',
    source_path STRING NOT NULL COMMENT 'Raw landing path or UC Volume (e.g., /Volumes/catalog/schema/volume/table_name/)',
    file_filter_pattern STRING COMMENT 'Optional glob/file regex pattern (e.g., *.parquet)',
    
    -- Target Storage Configuration
    target_catalog STRING NOT NULL COMMENT 'Target Unity Catalog name (e.g., lakehouse_prod)',
    target_schema STRING NOT NULL COMMENT 'Target UC Schema name (e.g., sales)',
    target_table_name STRING NOT NULL COMMENT 'Base table name without prefix (e.g., orders)',
    
    -- Ingestion Mode & CDC / Upsert Configuration
    -- Options: CDC (Change Data Capture), SCD1 (Type 1 Upsert), SCD2 (Type 2 History), APPEND (Append-only)
    ingestion_mode STRING NOT NULL COMMENT 'SCD1, SCD2, CDC, or APPEND',
    primary_keys ARRAY<STRING> COMMENT 'Array of primary key columns for deduplication and upserts',
    sequence_by STRING COMMENT 'Column used to determine the order of changes (e.g., updated_at, sequence_num)',
    delete_condition STRING COMMENT 'SQL boolean expression identifying deleted records (e.g., op_type = "D" or is_deleted = true)',
    scd_type STRING DEFAULT '1' COMMENT 'SCD type if applicable: "1" for latest state, "2" for track history',
    
    -- Auto Loader Options
    schema_evolution_mode STRING DEFAULT 'addNewColumns' COMMENT 'Auto Loader evolution: addNewColumns, rescue, or failOnNewColumns',
    
    -- Data Quality Expectations (Silver Layer)
    -- MAP of expectation_name -> SQL boolean expression
    expect_rules MAP<STRING, STRING> COMMENT 'Rules that warn/record metrics on violation without dropping data',
    expect_or_drop_rules MAP<STRING, STRING> COMMENT 'Rules that drop records violating the condition',
    expect_or_fail_rules MAP<STRING, STRING> COMMENT 'Rules that halt the pipeline on violation',
    
    -- Operational Flags & Audit
    is_active BOOLEAN DEFAULT TRUE COMMENT 'Toggle ingestion on/off without redeploying pipelines',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp when table was registered',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp when config was last modified'
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
