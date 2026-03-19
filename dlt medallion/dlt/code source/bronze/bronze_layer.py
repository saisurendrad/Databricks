# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC **DLT Object Types in this notebook:**
# MAGIC - **Streaming Table**: `bronze_orders`, `bronze_customers`
# MAGIC - **Empty Table**: `bronze_audit_log`

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC These values come from pipeline configuration settings.

# COMMAND ----------

# Get config from pipeline parameters (set in pipeline UI)
def get_conf(key, default):
    try:
        return spark.conf.get(key, default)
    except:
        return default

CATALOG = get_conf("catalog", "main")
SCHEMA = get_conf("schema", "medallion_demo")
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Table: bronze_orders
# MAGIC
# MAGIC Uses **Auto Loader** (`cloudFiles`) to:
# MAGIC - Automatically detect new JSON files
# MAGIC - Infer schema from data
# MAGIC - Process incrementally

# COMMAND ----------

@dlt.table(
    name="bronze_orders",
    comment="Raw order data ingested from landing zone using Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_orders():
    """
    STREAMING TABLE for raw order ingestion.
    
    - Uses cloudFiles (Auto Loader) for incremental file discovery
    - Adds metadata columns for lineage tracking
    - Schema is inferred automatically from JSON
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{LANDING_PATH}/_schemas/orders")
        .load(f"{LANDING_PATH}/orders")
        .withColumn("_ingested_at", current_timestamp())
        #.withColumn("_source_file", "/Volumes/main/medallion_demo/landing/_schemas/customers/")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Table: bronze_customers

# COMMAND ----------

@dlt.table(
    name="bronze_customers",
    comment="Raw customer data ingested from landing zone",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_customers():
    """
    STREAMING TABLE for raw customer ingestion.
    
    Same pattern as orders - Auto Loader with metadata columns.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{LANDING_PATH}/_schemas/customers")
        .load(f"{LANDING_PATH}/customers")
        .withColumn("_ingested_at", current_timestamp())
        #.withColumn("_source_file", "/Volumes/main/medallion_demo/landing/_schemas/orders/")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Empty Table: bronze_audit_log
# MAGIC
# MAGIC **Empty Tables** are:
# MAGIC - Created with schema only (no data)
# MAGIC - Populated by external processes
# MAGIC - Useful for audit logs, manual entries, external feeds

# COMMAND ----------

@dlt.table(
    name="bronze_audit_log",
    comment="Audit log table - schema only, populated externally via INSERT statements"
)
def bronze_audit_log():
    """
    EMPTY TABLE for manual audit entries.
    
    This table is created with a predefined schema but no data.
    Use INSERT statements from other notebooks to populate it:
    
    Example:
        INSERT INTO catalog.schema.bronze_audit_log 
        VALUES ('AUD_001', 'MANUAL_FIX', current_timestamp(), 'user@company.com', 'Fixed data issue')
    """
    schema = StructType([
        StructField("audit_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp", TimestampType(), nullable=False),
        StructField("user_id", StringType(), nullable=True),
        StructField("details", StringType(), nullable=True)
    ])
    
    # Return empty DataFrame with defined schema
    return spark.createDataFrame([], schema)
