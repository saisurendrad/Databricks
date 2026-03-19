# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: Create Sample Data
# MAGIC
# MAGIC **Run this notebook FIRST** before creating the DLT pipeline.
# MAGIC
# MAGIC This creates:
# MAGIC - A Unity Catalog Volume for landing data
# MAGIC - Sample customer JSON files
# MAGIC - Sample order JSON files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# UPDATE THESE VALUES for your environment
CATALOG = "main"  # Your catalog name
SCHEMA = "medallion_demo"  # Schema we'll create

# Paths
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing"

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Landing Path: {LANDING_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema and Volume

# COMMAND ----------

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"✅ Schema {CATALOG}.{SCHEMA} ready")

# Create volume for landing zone
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")
print(f"✅ Volume {LANDING_PATH} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Customer Data

# COMMAND ----------

import random
from datetime import datetime, timedelta

def generate_customers(num_customers=50):
    """Generate sample customer records."""
    first_names = ["John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Edward", "Fiona", "George", "Hannah"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Seattle", "Denver", "Boston"]
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
            "is_active": random.choice([True, True, True, False]),  # 75% active
            "customer_segment": random.choice(segments),
            "loyalty_points": random.randint(0, 5000) if random.random() > 0.1 else None
        }
        customers.append(customer)
    return customers

# Generate customers
customers = generate_customers(50)
print(f"Generated {len(customers)} customers")

# Convert to DataFrame
customers_df = spark.createDataFrame(customers)
display(customers_df.limit(5))

# COMMAND ----------

# Save customers as JSON
customers_df.coalesce(1).write.mode("overwrite").json(f"{LANDING_PATH}/customers")
print(f"✅ Customers saved to {LANDING_PATH}/customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Order Data

# COMMAND ----------

# DBTITLE 1,Generate order data
def generate_orders(num_orders=200, customer_ids=None):
    """Generate sample order records."""
    products = [
        {"id": "PROD_001", "name": "Laptop", "category": "Electronics", "price": 999.99},
        {"id": "PROD_002", "name": "Headphones", "category": "Electronics", "price": 149.99},
        {"id": "PROD_003", "name": "Desk Chair", "category": "Furniture", "price": 299.99},
        {"id": "PROD_004", "name": "Monitor", "category": "Electronics", "price": 399.99},
        {"id": "PROD_005", "name": "Keyboard", "category": "Electronics", "price": 79.99},
        {"id": "PROD_006", "name": "Mouse", "category": "Electronics", "price": 49.99},
        {"id": "PROD_007", "name": "Desk Lamp", "category": "Furniture", "price": 59.99},
    ]
    statuses = ["Completed", "Completed", "Completed", "Pending", "Shipped", "Cancelled"]
    payment_methods = ["Credit Card", "Debit Card", "PayPal"]

    orders = []
    for i in range(1, num_orders + 1):
        product = random.choice(products)
        quantity = float(random.randint(1, 3))   # Cast to float to unify type
        order_date = datetime.now() - timedelta(days=random.randint(0, 60))

        unit_price = float(product["price"])
        total_amount = float(round(product["price"] * quantity, 2))
        discount_percent = float(round(random.uniform(0, 20), 2)) if random.random() > 0.3 else None

        order = {
            "order_id": f"ORD_{i:05d}",
            "customer_id": random.choice(customer_ids),
            "product_id": product["id"],
            "product_name": product["name"],
            "category": product["category"],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "order_date": order_date.strftime("%Y-%m-%d"),
            "order_timestamp": order_date.isoformat(),
            "status": random.choice(statuses),
            "payment_method": random.choice(payment_methods),
            "discount_percent": discount_percent
        }

        # Add some intentionally bad data (2%) to test expectations
        if random.random() < 0.02:
            order["total_amount"] = float(-50)  # Invalid negative amount, cast to float
        if random.random() < 0.02:
            order["order_id"] = None  # Null order ID

        orders.append(order)
    return orders

# Generate orders
customer_ids = [c["customer_id"] for c in customers]
orders = generate_orders(200, customer_ids)
print(f"Generated {len(orders)} orders")

# Convert to DataFrame
orders_df = spark.createDataFrame(orders)
display(orders_df.limit(5))

# COMMAND ----------

# Save orders as JSON
orders_df.coalesce(1).write.mode("overwrite").json(f"{LANDING_PATH}/orders")
print(f"✅ Orders saved to {LANDING_PATH}/orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data

# COMMAND ----------

print("=" * 50)
print("DATA VERIFICATION")
print("=" * 50)

# Check files exist
print(f"\n📁 Customer files:")
display(dbutils.fs.ls(f"{LANDING_PATH}/customers"))

print(f"\n📁 Order files:")
display(dbutils.fs.ls(f"{LANDING_PATH}/orders"))

# Quick counts
print(f"\n📊 Record counts:")
print(f"   Customers: {customers_df.count()}")
print(f"   Orders: {orders_df.count()}")

# Check for bad data we inserted
from pyspark.sql.functions import col, when, count

bad_orders = orders_df.agg(
    count(when(col("order_id").isNull(), 1)).alias("null_order_ids"),
    count(when(col("total_amount") < 0, 1)).alias("negative_amounts")
).collect()[0]

print(f"\n⚠️ Intentionally bad data (for testing expectations):")
print(f"   Null order IDs: {bad_orders['null_order_ids']}")
print(f"   Negative amounts: {bad_orders['negative_amounts']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup Complete!
# MAGIC
# MAGIC Next steps:
# MAGIC 1. Create the DLT notebooks (bronze_layer, silver_layer, gold_layer)
# MAGIC 2. Create the DLT pipeline in Workflows
# MAGIC 3. Run the pipeline
