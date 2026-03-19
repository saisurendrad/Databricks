# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Cleaned & Validated Data
# MAGIC
# MAGIC **DLT Object Types in this notebook:**
# MAGIC - **Streaming Table**: `silver_orders`, `silver_customers` (with expectations)
# MAGIC - **View**: `vw_order_enriched` (not persisted to storage)

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, upper, lower, trim, initcap, 
    to_date, to_timestamp, date_format,
    when, concat_ws, current_timestamp, lit
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Table: silver_orders
# MAGIC
# MAGIC **Data Quality Expectations:**
# MAGIC - `@dlt.expect_or_drop` → Drops rows that fail the check
# MAGIC - `@dlt.expect_or_fail` → Fails the entire pipeline if any row fails
# MAGIC - `@dlt.expect` → Just logs/warns, keeps the row

# COMMAND ----------

@dlt.table(
    name="silver_orders",
    comment="Cleaned and validated order data with data quality expectations",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "total_amount > 0")
@dlt.expect("has_customer", "customer_id IS NOT NULL")  # Warn only
@dlt.expect("valid_status", "status IN ('COMPLETED', 'PENDING', 'SHIPPED', 'CANCELLED', 'RETURNED')")
def silver_orders():
    """
    STREAMING TABLE with cleaned orders.
    
    Transformations:
    - Type casting for numeric/date fields
    - Standardization (uppercase status, trimmed strings)
    - Derived columns (order_month, is_high_value)
    
    Expectations:
    - DROP: null order_id, negative/zero amounts
    - WARN: null customer_id, invalid status
    """
    return (
        dlt.read_stream("bronze_orders")
        .select(
            # Keys
            col("order_id"),
            col("customer_id"),
            
            # Product info
            col("product_id"),
            trim(col("product_name")).alias("product_name"),
            col("category").alias("product_category"),
            
            # Amounts - cast to proper types
            col("quantity").cast("int").alias("quantity"),
            col("unit_price").cast("decimal(10,2)").alias("unit_price"),
            col("total_amount").cast("decimal(10,2)").alias("total_amount"),
            col("discount_percent").cast("decimal(5,2)").alias("discount_percent"),
            
            # Dates
            to_date(col("order_date")).alias("order_date"),
            to_timestamp(col("order_timestamp")).alias("order_timestamp"),
            
            # Standardize status to uppercase
            upper(trim(col("status"))).alias("status"),
            col("payment_method"),
            
            # Metadata from bronze
            col("_ingested_at"),
            #col("_source_file"),
            
            # Processing timestamp
            current_timestamp().alias("_processed_at")
        )
        # Derived columns
        .withColumn("order_month", date_format(col("order_date"), "yyyy-MM"))
        .withColumn("is_high_value", when(col("total_amount") >= 500, True).otherwise(False))
        .withColumn(
            "discount_amount",
            when(col("discount_percent").isNotNull(),
                 col("total_amount") * col("discount_percent") / 100
            ).otherwise(lit(0.0))
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Table: silver_customers

# COMMAND ----------

@dlt.table(
    name="silver_customers",
    comment="Cleaned customer master data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%'")
@dlt.expect("has_name", "first_name IS NOT NULL OR last_name IS NOT NULL")
def silver_customers():
    """
    STREAMING TABLE with cleaned customer data.
    
    Transformations:
    - Name standardization (proper case)
    - Email normalization (lowercase)
    - Derived columns (full_name, loyalty_tier)
    """
    return (
        dlt.read_stream("bronze_customers")
        .select(
            col("customer_id"),
            
            # Names - proper case
            initcap(trim(col("first_name"))).alias("first_name"),
            initcap(trim(col("last_name"))).alias("last_name"),
            
            # Email - lowercase
            lower(trim(col("email"))).alias("email"),
            col("phone"),
            
            # Location
            initcap(trim(col("city"))).alias("city"),
            upper(trim(col("state"))).alias("state"),
            col("country"),
            
            # Attributes
            col("customer_segment"),
            col("is_active").cast("boolean").alias("is_active"),
            col("loyalty_points").cast("int").alias("loyalty_points"),
            
            # Dates
            to_date(col("created_date")).alias("customer_since"),
            
            # Metadata
            col("_ingested_at"),
            current_timestamp().alias("_processed_at")
        )
        # Derived columns
        .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
        .withColumn(
            "loyalty_tier",
            when(col("loyalty_points") >= 3000, "Gold")
            .when(col("loyalty_points") >= 1000, "Silver")
            .otherwise("Bronze")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: vw_order_enriched
# MAGIC
# MAGIC **Views** are:
# MAGIC - NOT persisted to storage (no Delta table created)
# MAGIC - Computed on-the-fly when referenced
# MAGIC - Great for intermediate transformations
# MAGIC - Reduce storage costs

# COMMAND ----------

@dlt.view(
    name="vw_order_enriched",
    comment="Orders enriched with customer details - VIEW (not persisted)"
)
def vw_order_enriched():
    """
    VIEW joining orders with customer data.
    
    This is a VIEW, not a TABLE:
    - No storage used
    - Computed when queried by downstream tables
    - Perfect for intermediate joins before aggregation
    """
    orders = dlt.read("silver_orders")
    customers = dlt.read("silver_customers")
    
    return (
        orders.join(
            customers.select(
                col("customer_id"),
                col("full_name").alias("customer_name"),
                col("email").alias("customer_email"),
                col("city").alias("customer_city"),
                col("state").alias("customer_state"),
                col("customer_segment"),
                col("loyalty_tier"),
                col("is_active").alias("customer_is_active")
            ),
            on="customer_id",
            how="left"
        )
    )
