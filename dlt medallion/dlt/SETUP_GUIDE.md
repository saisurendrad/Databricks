# Delta Live Tables - Complete Setup Guide
## Databricks Free Edition (with Unity Catalog) - New ETL Pipeline UI

---

## 📋 What We're Building

A 3-layer medallion architecture with all DLT object types:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│   LANDING ZONE          BRONZE             SILVER            GOLD       │
│   (JSON files)                                                          │
│                                                                         │
│   orders.json  ───►  bronze_orders  ───►  silver_orders  ───►  gold_daily_sales      │
│                      (Streaming)         (Streaming)         (Materialized View)     │
│                                               │                                       │
│   customers.json ─► bronze_customers ─► silver_customers ─► gold_customer_360        │
│                      (Streaming)         (Streaming)         (Materialized View)     │
│                                               │                                       │
│                      bronze_audit_log    vw_order_enriched   gold_data_quality       │
│                      (Empty Table)       (View)              (Empty Table)           │
│                                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 STEP 1: Create a Catalog and Schema

First, set up your Unity Catalog structure.

### 1.1 Go to Catalog
- Click **Catalog** in the left sidebar
- You should see your default catalog (often named after your workspace)

### 1.2 Create a Schema
1. Click on your catalog name
2. Click **Create schema**
3. Name: `medallion_demo`
4. Click **Create**

---

## 🚀 STEP 2: Create Sample Data

We need to create test data before running the pipeline.

### 2.1 Create a Notebook
1. Click **Workspace** in left sidebar
2. Click **Create** → **Notebook**
3. Settings:
   - Name: `00_setup_sample_data`
   - Default Language: **Python**
   - Cluster: Select any available cluster (or create one)
4. Click **Create**

### 2.2 Copy and Run This Code

Paste each block in a separate cell and run them:

**Cell 1: Configuration**
```python
# Configuration - Update catalog name if different
CATALOG = "main"  # Change this to your catalog name
SCHEMA = "medallion_demo"
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing"

# Create volume for landing data
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")
print(f"Landing path: {LANDING_PATH}")
```

**Cell 2: Generate Customer Data**
```python
import json
import random
from datetime import datetime, timedelta

def generate_customers(num_customers=50):
    first_names = ["John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Edward", "Fiona"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
    segments = ["Premium", "Standard", "Basic"]
    
    customers = []
    for i in range(1, num_customers + 1):
        customer = {
            "customer_id": f"CUST_{i:04d}",
            "first_name": random.choice(first_names),
            "last_name": random.choice(last_names),
            "email": f"customer{i}@example.com",
            "phone": f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            "city": random.choice(cities),
            "state": "CA",
            "country": "USA",
            "created_date": (datetime.now() - timedelta(days=random.randint(30, 365))).isoformat(),
            "is_active": random.choice([True, True, True, False]),
            "customer_segment": random.choice(segments),
            "loyalty_points": random.randint(0, 5000) if random.random() > 0.1 else None
        }
        customers.append(customer)
    return customers

# Generate and save
customers = generate_customers(50)
customers_df = spark.createDataFrame(customers)
customers_df.coalesce(1).write.mode("overwrite").json(f"{LANDING_PATH}/customers")
print(f"✅ Created {len(customers)} customers")
display(customers_df.limit(3))
```

**Cell 3: Generate Order Data**
```python
def generate_orders(num_orders=200, customer_ids=None):
    products = [
        {"id": "PROD_001", "name": "Laptop", "category": "Electronics", "price": 999.99},
        {"id": "PROD_002", "name": "Headphones", "category": "Electronics", "price": 149.99},
        {"id": "PROD_003", "name": "Desk Chair", "category": "Furniture", "price": 299.99},
        {"id": "PROD_004", "name": "Monitor", "category": "Electronics", "price": 399.99},
        {"id": "PROD_005", "name": "Keyboard", "category": "Electronics", "price": 79.99},
    ]
    statuses = ["Completed", "Completed", "Completed", "Pending", "Shipped", "Cancelled"]
    
    orders = []
    for i in range(1, num_orders + 1):
        product = random.choice(products)
        quantity = random.randint(1, 3)
        order_date = datetime.now() - timedelta(days=random.randint(0, 60))
        
        order = {
            "order_id": f"ORD_{i:05d}",
            "customer_id": random.choice(customer_ids),
            "product_id": product["id"],
            "product_name": product["name"],
            "category": product["category"],
            "quantity": quantity,
            "unit_price": product["price"],
            "total_amount": round(product["price"] * quantity, 2),
            "order_date": order_date.strftime("%Y-%m-%d"),
            "order_timestamp": order_date.isoformat(),
            "status": random.choice(statuses),
            "payment_method": random.choice(["Credit Card", "PayPal", "Debit Card"]),
            "discount_percent": round(random.uniform(0, 20), 2) if random.random() > 0.3 else None
        }
        
        # Add some bad data for testing expectations (2%)
        if random.random() < 0.02:
            order["total_amount"] = -50  # Invalid negative
        if random.random() < 0.02:
            order["order_id"] = None  # Null ID
            
        orders.append(order)
    return orders

# Generate and save
customer_ids = [c["customer_id"] for c in customers]
orders = generate_orders(200, customer_ids)
orders_df = spark.createDataFrame(orders)
orders_df.coalesce(1).write.mode("overwrite").json(f"{LANDING_PATH}/orders")
print(f"✅ Created {len(orders)} orders")
display(orders_df.limit(3))
```

**Cell 4: Verify Data**
```python
# Verify the data
print("=== Data Verification ===")
print(f"\nCustomers path: {LANDING_PATH}/customers")
print(f"Orders path: {LANDING_PATH}/orders")

# List files
display(dbutils.fs.ls(f"{LANDING_PATH}/customers"))
display(dbutils.fs.ls(f"{LANDING_PATH}/orders"))
```

### 2.3 Run All Cells
- Click **Run all** or run each cell with Shift+Enter
- Verify you see ✅ messages and sample data

---

## 🚀 STEP 3: Create DLT Notebooks

Now create the three layer notebooks. **Important**: DLT notebooks are NOT run directly - the pipeline runs them.

### 3.1 Create Bronze Layer Notebook

1. **Workspace** → **Create** → **Notebook**
2. Name: `bronze_layer`
3. Language: **Python**
4. **Don't attach a cluster** (DLT manages this)
5. Copy code from `bronze/bronze_layer.py` file
6. Click **Save** (Ctrl+S)

### 3.2 Create Silver Layer Notebook

1. **Workspace** → **Create** → **Notebook**
2. Name: `silver_layer`
3. Language: **Python**
4. Copy code from `silver/silver_layer.py` file
5. Click **Save**

### 3.3 Create Gold Layer Notebook

1. **Workspace** → **Create** → **Notebook**
2. Name: `gold_layer`
3. Language: **Python**
4. Copy code from `gold/gold_layer.py` file
5. Click **Save**

---

## 🚀 STEP 4: Create the ETL Pipeline (New UI)

Now let's create the pipeline using the **new ETL Pipeline interface**.

### 4.1 Navigate to Workflows

1. Click **Workflows** in the left sidebar
2. Click the **Delta Live Tables** tab (or **ETL Pipelines** in newer UI)

### 4.2 Create Pipeline

1. Click **Create pipeline** button

### 4.3 Configure Pipeline Settings

You'll see the new pipeline configuration UI. Fill in:

#### General Settings
| Field | Value |
|-------|-------|
| **Pipeline name** | `medallion_demo_pipeline` |
| **Pipeline mode** | `Triggered` |

#### Source Code
1. Click **Add source code** or **Browse**
2. Navigate to your `bronze_layer` notebook → Select it
3. Click **Add source code** again
4. Select `silver_layer` notebook
5. Click **Add source code** again
6. Select `gold_layer` notebook

You should see 3 notebooks listed.

#### Destination
| Field | Value |
|-------|-------|
| **Catalog** | `main` (or your catalog) |
| **Target schema** | `medallion_demo` |

#### Configuration (Key-Value pairs)
Click **Add configuration** and add:
| Key | Value |
|-----|-------|
| `catalog` | `main` |
| `schema` | `medallion_demo` |

### 4.4 Create Pipeline

1. Review settings
2. Click **Create**

---

## 🚀 STEP 5: Run the Pipeline

### 5.1 Start Pipeline

1. After creation, you'll see the pipeline page
2. Click **Start** (top right)

### 5.2 Watch the DAG

The UI will show a visual DAG (Directed Acyclic Graph):

```
bronze_orders ────────► silver_orders ──────► gold_daily_sales
                              │
                              ├──────────────► gold_customer_360
                              │                      ▲
bronze_customers ────► silver_customers ─────────────┘
                              │
                              └──► vw_order_enriched
                              
bronze_audit_log (empty)
gold_data_quality_metrics (empty)
gold_product_performance
```

### 5.3 Monitor Progress

- Each node shows status: ⏳ Running → ✅ Completed
- Click on any table to see:
  - Row counts
  - Data quality metrics (expectations)
  - Processing time

### 5.4 Wait for Completion

- First run takes 2-5 minutes
- All nodes should show ✅ green checkmarks

---

## 🚀 STEP 6: Query Your Tables

### 6.1 Using SQL Editor

1. Click **SQL Editor** in left sidebar
2. Run these queries:

```sql
-- Check bronze layer
SELECT COUNT(*) as count, 'bronze_orders' as table_name FROM main.medallion_demo.bronze_orders
UNION ALL
SELECT COUNT(*), 'bronze_customers' FROM main.medallion_demo.bronze_customers;

-- Check silver layer
SELECT * FROM main.medallion_demo.silver_orders LIMIT 10;

-- Check gold aggregations
SELECT * FROM main.medallion_demo.gold_daily_sales ORDER BY order_date DESC;

-- Customer 360 view
SELECT * FROM main.medallion_demo.gold_customer_360 
ORDER BY lifetime_value DESC LIMIT 10;
```

---

## ✅ Summary of Objects Created

| Layer | Table Name | Type |
|-------|------------|------|
| Bronze | `bronze_orders` | Streaming Table |
| Bronze | `bronze_customers` | Streaming Table |
| Bronze | `bronze_audit_log` | Empty Table |
| Silver | `silver_orders` | Streaming Table |
| Silver | `silver_customers` | Streaming Table |
| Silver | `vw_order_enriched` | View |
| Gold | `gold_daily_sales` | Materialized View |
| Gold | `gold_customer_360` | Materialized View |
| Gold | `gold_product_performance` | Materialized View |
| Gold | `gold_data_quality_metrics` | Empty Table |
