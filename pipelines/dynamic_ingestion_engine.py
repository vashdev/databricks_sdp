# Databricks notebook source
# ==============================================================================
# Lakeflow Pipelines Dynamic Ingestion Engine
# ==============================================================================
# This single script acts as the declarative engine for ALL domain pipelines.
# It reads domain-scoped metadata from YAML configuration files (or optional
# control table fallback) and dynamically registers Bronze (Auto Loader) and
# Silver (SCD/CDC/Append) tables along with Data Quality Expectations into the
# Lakeflow DAG.
# ==============================================================================

import os
import logging
import yaml
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lakeflow_engine")


# ------------------------------------------------------------------------------
# Data Structure: Table Configuration Wrapper
# ------------------------------------------------------------------------------
class TableConfig:
    """
    Lightweight wrapper providing unified attribute and dictionary access
    to table configuration properties regardless of source (YAML or Control Table).
    """
    def __init__(self, data: dict, domain: str = None, defaults: dict = None, catalog_override: str = None):
        defaults = defaults or {}
        merged = {**defaults, **data}
        if domain and "domain" not in merged:
            merged["domain"] = domain
        if catalog_override:
            merged["target_catalog"] = catalog_override

        self._data = merged
        for k, v in merged.items():
            setattr(self, k, v)

    def __getattr__(self, name):
        return self._data.get(name, None)

    def get(self, name, default=None):
        return self._data.get(name, default)

    def __repr__(self):
        return f"<TableConfig table_id='{self.table_id}' domain='{self.domain}'>"


# ------------------------------------------------------------------------------
# 1. Pipeline Parameter Initialization
# ------------------------------------------------------------------------------
DEFAULT_DOMAIN = "sales"

try:
    current_domain = spark.conf.get("pipeline.domain", DEFAULT_DOMAIN)
except Exception:
    current_domain = DEFAULT_DOMAIN

try:
    config_path_param = spark.conf.get("pipeline.config_path", None)
except Exception:
    config_path_param = None

try:
    target_catalog_override = spark.conf.get("pipeline.catalog", None)
except Exception:
    target_catalog_override = None

logger.info(f"Initializing Lakeflow Dynamic Engine for Domain: '{current_domain}'")


# ------------------------------------------------------------------------------
# 2. Configuration Resolution & Loading (YAML primary, Control Table fallback)
# ------------------------------------------------------------------------------
def resolve_config_file(domain_name: str, explicit_path: str = None) -> str:
    """
    Locates the domain YAML file across standard Databricks bundle, workspace,
    and relative repository paths.
    """
    candidates = []
    if explicit_path:
        candidates.extend([
            explicit_path,
            os.path.join(os.path.dirname(__file__), explicit_path),
            os.path.join(os.path.dirname(__file__), "..", explicit_path),
            os.path.join(os.getcwd(), explicit_path),
            os.path.join(os.getcwd(), "..", explicit_path),
        ])

    candidates.extend([
        f"configs/domains/{domain_name}.yml",
        f"configs/domains/{domain_name}.yaml",
        f"configs/{domain_name}.yml",
        os.path.join(os.path.dirname(__file__), "..", f"configs/domains/{domain_name}.yml"),
        os.path.join(os.path.dirname(__file__), f"configs/domains/{domain_name}.yml"),
        os.path.join(os.getcwd(), f"configs/domains/{domain_name}.yml"),
        os.path.join(os.getcwd(), "..", f"configs/domains/{domain_name}.yml"),
    ])

    for path in candidates:
        if os.path.isfile(path):
            return os.path.abspath(path)

    return None


active_tables = []
resolved_config_file = resolve_config_file(current_domain, config_path_param)

if resolved_config_file:
    logger.info(f"Loading domain metadata from YAML configuration: '{resolved_config_file}'")
    with open(resolved_config_file, "r") as f:
        domain_cfg = yaml.safe_load(f) or {}

    domain_defaults = domain_cfg.get("defaults", {})
    tables_raw = domain_cfg.get("tables", [])

    table_configs = [
        TableConfig(
            data=t,
            domain=domain_cfg.get("domain", current_domain),
            defaults=domain_defaults,
            catalog_override=target_catalog_override
        )
        for t in tables_raw
    ]

    active_tables = [t for t in table_configs if t.is_active is not False]
    logger.info(
        f"Discovered {len(active_tables)} active tables for domain '{current_domain}' from YAML."
    )
else:
    # Optional fallback: If YAML is absent, check if a control table was provided
    try:
        control_table = spark.conf.get("pipeline.control_table", "system_metadata.ingestion.table_registry")
        logger.info(f"YAML config not found. Attempting fallback to Control Table: '{control_table}'")
        active_tables_df = (
            spark.table(control_table)
            .filter(
                (F.col("domain") == current_domain) & (F.col("is_active") == F.lit(True))
            )
        )
        active_tables = active_tables_df.collect()
        logger.info(f"Discovered {len(active_tables)} active tables from control table '{control_table}'.")
    except Exception as e:
        logger.error(
            f"Could not load configuration for domain '{current_domain}'. "
            f"No YAML configuration file found (searched for {config_path_param or current_domain + '.yml'}) "
            f"and control table fallback failed: {str(e)}"
        )
        raise e

if not active_tables:
    logger.warning(
        f"No active tables found for domain '{current_domain}'. "
        f"Ensure table definitions have is_active: true in the configuration."
    )


# ------------------------------------------------------------------------------
# 3. Dynamic Bronze Generator Factory (Auto Loader)
# ------------------------------------------------------------------------------
def register_bronze_table(cfg):
    """
    Factory function to register a Bronze streaming table using Auto Loader.
    Encapsulated to prevent Python loop variable closure capture issues.
    """
    bronze_name = f"bronze_{cfg.target_table_name}"
    source_fmt = cfg.source_format or "parquet"
    evolution_mode = cfg.schema_evolution_mode or "addNewColumns"
    landing_path = cfg.source_path
    glob_filter = cfg.file_filter_pattern
    pk_cols = list(cfg.primary_keys) if cfg.primary_keys else []

    table_props = {
        "quality": "bronze",
        "pipeline.engine": "lakeflow_dynamic_v1",
        "pipeline.source_path": landing_path,
    }
    if pk_cols:
        table_props["pipelines.autoOptimize.zOrderCols"] = ",".join(pk_cols)

    logger.info(f"Registering Bronze table: '{bronze_name}' -> reading from '{landing_path}'")

    @dlt.table(
        name=bronze_name,
        comment=f"Raw Bronze Auto Loader streaming ingestion for {cfg.target_table_name}",
        table_properties=table_props
    )
    def bronze_stream():
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", source_fmt)
            .option("cloudFiles.schemaEvolutionMode", evolution_mode)
            .option("cloudFiles.inferColumnTypes", "true")
        )

        if glob_filter:
            reader = reader.option("pathGlobFilter", glob_filter)

        return (
            reader.load(landing_path)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file_path", F.col("_metadata.file_path"))
            .withColumn("_source_file_modification_time", F.col("_metadata.file_modification_time"))
        )

    return bronze_name


# ------------------------------------------------------------------------------
# 4. Dynamic Silver Generator Factory (Curated, Deduplicated, Expectations)
# ------------------------------------------------------------------------------
def register_silver_table(cfg, bronze_name):
    """
    Factory function to curate Bronze data into Silver.
    - If primary keys are present and mode is CDC / SCD1 / SCD2: Uses dlt.apply_changes.
    - If mode is APPEND or no primary keys: Uses dynamic streaming table with expectation decorators.
    """
    silver_name = f"silver_{cfg.target_table_name}"
    mode = (cfg.ingestion_mode or "SCD1").upper()
    keys = list(cfg.primary_keys) if cfg.primary_keys else []
    seq_col = cfg.sequence_by
    del_expr = cfg.delete_condition
    scd_type = str(cfg.scd_type or "1")

    # Parse Data Quality expectation dictionaries from metadata
    expect_warn = dict(cfg.expect_rules) if cfg.expect_rules else {}
    expect_drop = dict(cfg.expect_or_drop_rules) if cfg.expect_or_drop_rules else {}
    expect_fail = dict(cfg.expect_or_fail_rules) if cfg.expect_or_fail_rules else {}

    logger.info(
        f"Registering Silver table: '{silver_name}' (Mode: {mode}, SCD: {scd_type}, Keys: {keys})"
    )

    if mode in ("CDC", "SCD1", "SCD2") and keys:
        # 4a. Silver via dlt.apply_changes (Upsert / CDC / SCD)
        dlt.create_streaming_table(
            name=silver_name,
            comment=f"Curated Silver table for {cfg.target_table_name} with {mode} (SCD Type {scd_type})",
            expect_all=expect_warn if expect_warn else None,
            expect_all_or_drop=expect_drop if expect_drop else None,
            expect_all_or_fail=expect_fail if expect_fail else None,
            table_properties={
                "quality": "silver",
                "delta.enableChangeDataFeed": "true",
                "pipeline.engine": "lakeflow_dynamic_v1"
            }
        )

        dlt.apply_changes(
            target=silver_name,
            source=bronze_name,
            keys=keys,
            sequence_by=F.col(seq_col) if seq_col else F.col("_ingested_at"),
            apply_as_deletes=F.expr(del_expr) if del_expr else None,
            stored_as_scd_type=scd_type
        )

    else:
        # 4b. Silver via Streaming Append with Expectations
        table_decorator = dlt.table(
            name=silver_name,
            comment=f"Curated Silver append stream for {cfg.target_table_name}",
            table_properties={
                "quality": "silver",
                "pipeline.engine": "lakeflow_dynamic_v1"
            }
        )

        def silver_stream():
            return dlt.read_stream(bronze_name)

        # Apply expectation decorators dynamically if rules exist
        decorated = silver_stream
        if expect_warn:
            decorated = dlt.expect_all(expect_warn)(decorated)
        if expect_drop:
            decorated = dlt.expect_all_or_drop(expect_drop)(decorated)
        if expect_fail:
            decorated = dlt.expect_all_or_fail(expect_fail)(decorated)

        table_decorator(decorated)


# ------------------------------------------------------------------------------
# 5. DAG Construction Loop
# ------------------------------------------------------------------------------
for table_config in active_tables:
    try:
        bronze_table_name = register_bronze_table(table_config)
        register_silver_table(table_config, bronze_table_name)
    except Exception as table_err:
        logger.error(
            f"Failed to register table '{table_config.target_table_name}' in domain '{current_domain}': {str(table_err)}"
        )
        raise table_err
