# Databricks notebook source
# MAGIC %md
# MAGIC # Lab Setup — Common Includes
# MAGIC
# MAGIC This notebook is run via `%run ./Includes/Setup` from every module. It:
# MAGIC
# MAGIC - Computes a `LAB_PREFIX` so all artifacts are user-scoped (no collisions in shared workspaces)
# MAGIC - Creates the lab catalog and schema
# MAGIC - Seeds the synthetic e-commerce sample data
# MAGIC - Exposes a `preflight_check()` helper that validates the workspace meets prerequisites
# MAGIC - Imports `lakebase_helpers` which has reusable connection/token patterns

# COMMAND ----------

import os
import re
from databricks.sdk import WorkspaceClient

# ----------------------------------------------------------------------------
# Identity-scoped naming
# ----------------------------------------------------------------------------
_w = WorkspaceClient()
USER_EMAIL = _w.current_user.me().user_name or "anonymous@databricks.com"
USER_HANDLE = re.sub(r"[^a-z0-9]+", "_",
                      USER_EMAIL.split("@")[0].lower()).strip("_")
LAB_PREFIX = f"lakebase300_{USER_HANDLE}"
LAB_CATALOG = f"lakebase300_{USER_HANDLE}_catalog"
LAB_SCHEMA = "lab"

print(f"User: {USER_EMAIL}")
print(f"LAB_PREFIX: {LAB_PREFIX}")
print(f"LAB_CATALOG.LAB_SCHEMA: {LAB_CATALOG}.{LAB_SCHEMA}")

# COMMAND ----------

# Make these importable from `from Includes.Setup import LAB_PREFIX` etc.
__all__ = [
    "USER_EMAIL", "USER_HANDLE", "LAB_PREFIX", "LAB_CATALOG", "LAB_SCHEMA",
    "preflight_check", "ensure_lab_catalog", "seed_sample_data",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog + sample data

# COMMAND ----------

def ensure_lab_catalog():
    """Create the lab catalog and schema if they don't exist."""
    # Check if catalog exists
    try:
        spark.sql(f"DESCRIBE CATALOG {LAB_CATALOG}")
        catalog_exists = True
    except Exception:
        catalog_exists = False

    if catalog_exists:
        print(f"  ↪ Catalog already exists: {LAB_CATALOG}")
    else:
        try:
            spark.sql(f"CREATE CATALOG {LAB_CATALOG}")
            print(f"  ✅ Catalog created: {LAB_CATALOG}")
        except Exception as e:
            print(f"  ❌ Failed to create catalog: {LAB_CATALOG} — {e}")
            return

    spark.sql(f"USE CATALOG {LAB_CATALOG}")

    # Check if schema exists
    try:
        spark.sql(f"DESCRIBE SCHEMA {LAB_SCHEMA}")
        schema_exists = True
    except Exception:
        schema_exists = False

    if schema_exists:
        print(f"  ↪ Schema already exists: {LAB_CATALOG}.{LAB_SCHEMA}")
    else:
        try:
            spark.sql(f"CREATE SCHEMA {LAB_SCHEMA}")
            print(f"  ✅ Schema created: {LAB_CATALOG}.{LAB_SCHEMA}")
        except Exception as e:
            print(f"  ❌ Failed to create schema: {LAB_CATALOG}.{LAB_SCHEMA} — {e}")
            return

    spark.sql(f"USE SCHEMA {LAB_SCHEMA}")


def seed_sample_data():
    """Populate orders / customers / products tables for the lab.

    Idempotent — only inserts if tables are empty.
    """
    #ensure_lab_catalog()

    # Drop stale table entries whose underlying Delta data no longer exists
    for tbl in ['customers', 'products', 'orders']:
        fqn = f"{LAB_CATALOG}.{LAB_SCHEMA}.{tbl}"
        try:
            spark.table(fqn).limit(0).collect()
        except Exception:
            spark.sql(f"DROP TABLE IF EXISTS {fqn}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {LAB_CATALOG}.{LAB_SCHEMA}.customers (
            customer_id   BIGINT,
            email         STRING,
            full_name     STRING,
            country       STRING,
            created_at    TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {LAB_CATALOG}.{LAB_SCHEMA}.products (
            product_id  BIGINT,
            sku         STRING,
            name        STRING,
            category    STRING,
            price_aud   DECIMAL(10,2),
            description STRING
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {LAB_CATALOG}.{LAB_SCHEMA}.orders (
            order_id     BIGINT,
            customer_id  BIGINT,
            product_id   BIGINT,
            quantity     INT,
            order_total  DECIMAL(12,2),
            ordered_at   TIMESTAMP,
            status       STRING
        ) USING DELTA
    """)

    n_customers = spark.table(f"{LAB_CATALOG}.{LAB_SCHEMA}.customers").count()
    if n_customers > 0:
        print(f"  ↪ sample data already present ({n_customers} customers); skipping seed")
        return

    n_products = spark.table(f"{LAB_CATALOG}.{LAB_SCHEMA}.products").count()
    if n_products > 0:
        print(f"  ↪ sample data already present ({n_products} products); skipping seed")
        return
    
    n_orders = spark.table(f"{LAB_CATALOG}.{LAB_SCHEMA}.orders").count()
    if n_orders > 0:
        print(f"  ↪ sample data already present ({n_orders} orders); skipping seed")
        return
       
    print("  Seeding sample data...")
    spark.sql(f"""
        INSERT INTO {LAB_CATALOG}.{LAB_SCHEMA}.customers
        SELECT
          id AS customer_id,
          concat('user_', id, '@example.com') AS email,
          concat('Customer ', id) AS full_name,
          element_at(array('AU','US','UK','DE','JP','SG','IN'), CAST(pmod(id, 7)+1 AS INT)) AS country,
          current_timestamp() - (CAST(id AS BIGINT) * INTERVAL '1' MINUTE) AS created_at
        FROM range(1000) AS T(id)
    """)
    spark.sql(f"""
        INSERT INTO {LAB_CATALOG}.{LAB_SCHEMA}.products
        SELECT
          id AS product_id,
          concat('SKU-', lpad(cast(id AS STRING), 6, '0')) AS sku,
          element_at(array('Smart Watch','Headphones','Laptop','Desk Lamp','Notebook',
                            'Coffee Mug','T-Shirt','Backpack','Sunglasses','Power Bank'),
                      CAST(pmod(id, 10)+1 AS INT)) AS name,
          element_at(array('Electronics','Apparel','Stationery','Home','Outdoor'),
                      CAST(pmod(id, 5)+1 AS INT)) AS category,
          cast(rand(42) * 500 + 10 AS DECIMAL(10,2)) AS price_aud,
          concat('Premium ', element_at(array('quality','design','build','features','value'),
                                          CAST(pmod(id,5)+1 AS INT))) AS description
        FROM range(200) AS T(id)
    """)
    spark.sql(f"""
        INSERT INTO {LAB_CATALOG}.{LAB_SCHEMA}.orders
        SELECT
          id AS order_id,
          pmod(cast(rand(7) * 999 AS BIGINT), 1000) AS customer_id,
          pmod(cast(rand(11) * 199 AS BIGINT), 200) AS product_id,
          cast(rand(13) * 5 + 1 AS INT) AS quantity,
          cast(rand(17) * 1000 + 20 AS DECIMAL(12,2)) AS order_total,
          current_timestamp() - (CAST(id AS BIGINT) * INTERVAL '1' MINUTE) AS ordered_at,
          element_at(array('PLACED','PAID','SHIPPED','DELIVERED','CANCELLED'),
                      cast(rand(19) * 5 + 1 AS INT)) AS status
        FROM range(10000) AS T(id)
    """)
    print(f"  ✅ Seeded 1,000 customers · 200 products · 10,000 orders")


# Ensure setup ran when this notebook is %run-d.
ensure_lab_catalog()
seed_sample_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preflight check

# COMMAND ----------

def preflight_check():
    """Verify the workspace meets the lab prerequisites. Prints ✅ or ❌ per check."""
    print("Running preflight checks...\n")
    ok = True

    # 1. SDK version
    try:
        import databricks.sdk
        v = databricks.sdk.version.__version__ if hasattr(databricks.sdk, "version") else "unknown"
        good = True  # any SDK is fine; we just check it imports
        print(f"  ✅ databricks-sdk: {v}")
    except Exception as e:
        print(f"  ❌ databricks-sdk: import failed — {e}")
        ok = False

    # 2. psycopg available (for direct Postgres connections)
    try:
        import psycopg
        print(f"  ✅ psycopg: {psycopg.__version__}")
    except Exception:
        print("  ❌ psycopg not installed. Run `%pip install 'psycopg[binary]>=3.2'`")
        ok = False

    # 3. SQLAlchemy available
    try:
        import sqlalchemy
        print(f"  ✅ sqlalchemy: {sqlalchemy.__version__}")
    except Exception:
        print("  ❌ sqlalchemy not installed. Run `%pip install sqlalchemy>=2`")
        ok = False

    # 4. Workspace client
    try:
        w = WorkspaceClient()
        print(f"  ✅ workspace: {w.config.host}")
    except Exception as e:
        print(f"  ❌ WorkspaceClient: {e}")
        ok = False

    # 5. Lakebase API reachable
    try:
        instances = list(w.database.list_database_instances())
        print(f"  ✅ Lakebase API reachable ({len(instances)} existing instance(s))")
    except Exception as e:
        msg = str(e)[:200]
        if "feature" in msg.lower() or "not enabled" in msg.lower():
            print(f"  ❌ Lakebase feature not enabled in this workspace. Contact admin.")
        else:
            print(f"  ⚠️  Lakebase API check inconclusive: {msg}")
        ok = False

    # 6. Catalog
    try:
        spark.sql(f"DESCRIBE CATALOG {LAB_CATALOG}").show(1, truncate=False)
        print(f"  ✅ Lab catalog exists: {LAB_CATALOG}")
    except Exception as e:
        print(f"  ❌ Lab catalog missing: {LAB_CATALOG} — re-run setup")
        ok = False

    print()
    if ok:
        print("All checks passed. You're ready to start the lab.")
    else:
        print("Some checks failed. Fix the items above before starting Module 01.")
    return ok

print("Setup loaded. Symbols: LAB_PREFIX, LAB_CATALOG, LAB_SCHEMA, "
      "ensure_lab_catalog(), seed_sample_data(), preflight_check().")
