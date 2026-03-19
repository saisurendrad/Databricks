# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Business Aggregations
# MAGIC
# MAGIC **DLT Object Types in this notebook:**
# MAGIC - **Materialized View**: `gold_daily_sales`, `gold_customer_360`, `gold_product_performance`
# MAGIC - **Empty Table**: `gold_data_quality_metrics`

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, count, countDistinct, sum, avg, min, max,
    when, coalesce, lit, current_timestamp, datediff, current_date
)
from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType, DoubleType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialized View: gold_daily_sales
# MAGIC
# MAGIC **Materialized Views** are:
# MAGIC - Precomputed and stored (like tables)
# MAGIC - Automatically refreshed when upstream data changes
# MAGIC - Ideal for aggregations queried frequently
# MAGIC
# MAGIC Note: In DLT Python, we use `@dlt.table` for both tables and materialized views.
# MAGIC The difference is that materialized views read from batch (not stream).

# COMMAND ----------

@dlt.table(
    name="gold_daily_sales",
    comment="Daily sales metrics by date and category - MATERIALIZED VIEW",
    table_properties={"quality": "gold"}
)
def gold_daily_sales():
    """
    MATERIALIZED VIEW with daily sales aggregations.
    
    Metrics:
    - Order counts and unique customers
    - Revenue (gross, discounts, net)
    - Average order value
    - High-value order analysis
    """
    return (
        dlt.read("silver_orders")  # Batch read = materialized view behavior
        .filter(col("status").isin("COMPLETED", "SHIPPED"))
        .groupBy(
            col("order_date"),
            col("product_category")
        )
        .agg(
            # Volume metrics
            count("*").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            sum("quantity").alias("units_sold"),
            
            # Revenue metrics
            sum("total_amount").alias("gross_revenue"),
            sum("discount_amount").alias("total_discounts"),
            
            # Averages
            avg("total_amount").alias("avg_order_value"),
            avg("quantity").alias("avg_units_per_order"),
            
            # High value analysis
            sum(when(col("is_high_value"), 1).otherwise(0)).alias("high_value_orders"),
            sum(when(col("is_high_value"), col("total_amount")).otherwise(0)).alias("high_value_revenue")
        )
        .withColumn("net_revenue", col("gross_revenue") - col("total_discounts"))
        .withColumn("high_value_pct", 
                    when(col("total_orders") > 0, 
                         col("high_value_orders") / col("total_orders") * 100
                    ).otherwise(0))
        .withColumn("_refreshed_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialized View: gold_customer_360

# COMMAND ----------

@dlt.table(
    name="gold_customer_360",
    comment="Customer 360 view with lifetime metrics - MATERIALIZED VIEW",
    table_properties={"quality": "gold"}
)
def gold_customer_360():
    """
    MATERIALIZED VIEW for customer analytics.
    
    Combines customer master data with aggregated order history:
    - Lifetime value
    - Order frequency
    - Recency metrics
    - Value segmentation
    """
    # Aggregate orders per customer
    order_metrics = (
        dlt.read("vw_order_enriched")
        .filter(col("status").isin("COMPLETED", "SHIPPED"))
        .groupBy("customer_id")
        .agg(
            count("*").alias("total_orders"),
            sum("total_amount").alias("lifetime_value"),
            avg("total_amount").alias("avg_order_value"),
            min("order_date").alias("first_order_date"),
            max("order_date").alias("last_order_date"),
            sum("quantity").alias("total_items_purchased")
        )
    )
    
    # Get customer master data
    customers = dlt.read("silver_customers")
    
    # Join and enrich
    return (
        customers
        .join(order_metrics, on="customer_id", how="left")
        .select(
            # Customer info
            col("customer_id"),
            col("full_name"),
            col("email"),
            col("customer_segment"),
            col("loyalty_tier"),
            col("loyalty_points"),
            col("customer_since"),
            col("is_active"),
            col("city"),
            col("state"),
            
            # Order metrics with defaults
            coalesce(col("total_orders"), lit(0)).alias("total_orders"),
            coalesce(col("lifetime_value"), lit(0.0)).alias("lifetime_value"),
            col("avg_order_value"),
            col("first_order_date"),
            col("last_order_date"),
            coalesce(col("total_items_purchased"), lit(0)).alias("total_items")
        )
        # Derived metrics
        .withColumn(
            "days_since_last_order",
            when(col("last_order_date").isNotNull(),
                 datediff(current_date(), col("last_order_date"))
            ).otherwise(lit(None))
        )
        .withColumn(
            "customer_value_tier",
            when(col("lifetime_value") >= 2000, "VIP")
            .when(col("lifetime_value") >= 500, "High Value")
            .when(col("lifetime_value") > 0, "Regular")
            .otherwise("New")
        )
        .withColumn(
            "churn_risk",
            when(col("days_since_last_order") > 60, "High")
            .when(col("days_since_last_order") > 30, "Medium")
            .when(col("days_since_last_order").isNotNull(), "Low")
            .otherwise("New Customer")
        )
        .withColumn("_refreshed_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialized View: gold_product_performance

# COMMAND ----------

@dlt.table(
    name="gold_product_performance",
    comment="Product performance metrics - MATERIALIZED VIEW",
    table_properties={"quality": "gold"}
)
def gold_product_performance():
    """
    MATERIALIZED VIEW for product analytics.
    
    Metrics per product:
    - Sales volume and revenue
    - Customer reach (unique buyers)
    - Time-based analysis (first/last sale)
    """
    return (
        dlt.read("silver_orders")
        .filter(col("status") == "COMPLETED")
        .groupBy(
            col("product_id"),
            col("product_name"),
            col("product_category")
        )
        .agg(
            count("*").alias("times_ordered"),
            sum("quantity").alias("total_units_sold"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_sale_value"),
            countDistinct("customer_id").alias("unique_buyers"),
            min("order_date").alias("first_sale_date"),
            max("order_date").alias("last_sale_date")
        )
        .withColumn("_refreshed_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Empty Table: gold_data_quality_metrics
# MAGIC
# MAGIC **Purpose**: External data quality monitoring processes write metrics here.
# MAGIC
# MAGIC This table is NOT populated by DLT transformations - it's a schema-only table
# MAGIC that external jobs can INSERT into.

# COMMAND ----------

@dlt.table(
    name="gold_data_quality_metrics",
    comment="Data quality metrics - EMPTY TABLE populated by external monitoring jobs"
)
def gold_data_quality_metrics():
    """
    EMPTY TABLE for data quality metrics.
    
    Populated externally by:
    - Data quality monitoring notebooks
    - Scheduled quality checks
    - Alert systems
    
    Example INSERT:
        INSERT INTO catalog.schema.gold_data_quality_metrics 
        VALUES (current_date(), 'silver_orders', 500, 0.98, current_timestamp())
    """
    schema = StructType([
        StructField("metric_date", DateType(), nullable=False),
        StructField("table_name", StringType(), nullable=False),
        StructField("total_records", LongType(), nullable=True),
        StructField("quality_score", DoubleType(), nullable=True),
        StructField("measured_at", TimestampType(), nullable=True)
    ])
    
    return spark.createDataFrame([], schema)
