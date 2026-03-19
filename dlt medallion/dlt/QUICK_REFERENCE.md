# Quick Reference: New ETL Pipeline UI

## Pipeline Creation Steps (New Interface)

### 1. Navigate
```
Workflows → Delta Live Tables → Create pipeline
```

### 2. Fill in Settings

| Setting | Value |
|---------|-------|
| Pipeline name | `medallion_demo_pipeline` |
| Pipeline mode | `Triggered` |

### 3. Add Source Code

Click **"Add source code"** 3 times and select:
1. `bronze_layer`
2. `silver_layer`  
3. `gold_layer`

### 4. Set Destination

| Setting | Value |
|---------|-------|
| Catalog | `main` |
| Target schema | `medallion_demo` |

### 5. Add Configuration

Click **"Add configuration"** and add key-value pairs:

| Key | Value |
|-----|-------|
| `catalog` | `main` |
| `schema` | `medallion_demo` |

### 6. Create & Run

1. Click **Create**
2. Click **Start**

---

## Objects Created Summary

```
BRONZE LAYER
├── bronze_orders        [Streaming Table]  ← Auto Loader ingestion
├── bronze_customers     [Streaming Table]  ← Auto Loader ingestion
└── bronze_audit_log     [Empty Table]      ← Manual INSERT

SILVER LAYER
├── silver_orders        [Streaming Table]  ← Expectations: DROP invalid
├── silver_customers     [Streaming Table]  ← Expectations: DROP invalid
└── vw_order_enriched    [View]             ← NOT stored, computed on read

GOLD LAYER
├── gold_daily_sales         [Materialized View]  ← Daily aggregations
├── gold_customer_360        [Materialized View]  ← Customer lifetime
├── gold_product_performance [Materialized View]  ← Product metrics
└── gold_data_quality_metrics [Empty Table]       ← External DQ writes
```

---

## Key DLT Syntax

### Streaming Table (incremental)
```python
@dlt.table(name="my_table")
def my_table():
    return dlt.read_stream("source_table")  # or spark.readStream
```

### Materialized View (batch)
```python
@dlt.table(name="my_mv")
def my_mv():
    return dlt.read("source_table")  # Batch read
```

### View (not stored)
```python
@dlt.view(name="my_view")
def my_view():
    return dlt.read("source_table").filter(...)
```

### Empty Table
```python
@dlt.table(name="my_empty")
def my_empty():
    schema = StructType([...])
    return spark.createDataFrame([], schema)
```

### Expectations
```python
@dlt.expect("warn", "col IS NOT NULL")           # Warn only
@dlt.expect_or_drop("drop", "amount > 0")        # Drop bad rows
@dlt.expect_or_fail("fail", "id IS NOT NULL")    # Fail pipeline
```

---

## Useful Queries After Pipeline Runs

```sql
-- Check record counts
SELECT 'bronze_orders' as tbl, COUNT(*) FROM main.medallion_demo.bronze_orders
UNION ALL SELECT 'silver_orders', COUNT(*) FROM main.medallion_demo.silver_orders
UNION ALL SELECT 'gold_daily_sales', COUNT(*) FROM main.medallion_demo.gold_daily_sales;

-- See data quality expectations results
SELECT * FROM main.medallion_demo.silver_orders LIMIT 10;

-- Check aggregations
SELECT * FROM main.medallion_demo.gold_daily_sales ORDER BY order_date DESC;

-- Customer analysis
SELECT customer_value_tier, COUNT(*), AVG(lifetime_value)
FROM main.medallion_demo.gold_customer_360
GROUP BY customer_value_tier;
```

---

## Populate Empty Tables

```sql
-- Insert audit entry
INSERT INTO main.medallion_demo.bronze_audit_log 
VALUES ('AUD_001', 'PIPELINE_RUN', current_timestamp(), 'admin', 'Success');

-- Insert quality metric
INSERT INTO main.medallion_demo.gold_data_quality_metrics
VALUES (current_date(), 'silver_orders', 200, 0.98, current_timestamp());
```
