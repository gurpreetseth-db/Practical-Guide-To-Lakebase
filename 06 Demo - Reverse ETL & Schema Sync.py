# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Demo — Reverse ETL & Schema Sync
# MAGIC
# MAGIC **Duration:** ~30 min · Lakebase **Synced Tables** keep a Delta table in UC continuously synchronized to a Postgres table. This module covers:
# MAGIC
# MAGIC - One-shot snapshot vs continuous sync
# MAGIC - Watermarks and CDC under the hood
# MAGIC - Schema evolution — what's automatic, what isn't
# MAGIC - Performance / lag characteristics
# MAGIC - Reverse direction (Postgres → Delta) via CDC

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2" "sqlalchemy>=2"
# MAGIC %pip install --upgrade "databricks-sdk>=0.40"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · Set up: source Delta table in UC
# MAGIC
# MAGIC The setup notebook already created `customers`, `products`, `orders` Delta tables. We'll sync `customers` to a Lakebase database.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · Create a synced table
# MAGIC
# MAGIC The Sync API takes a UC source, a Lakebase destination, and a sync mode.

# COMMAND ----------

# DBTITLE 1,Cell 6
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import (
    SyncedDatabaseTable, SyncedTableSpec, SyncedTableSchedulingPolicy, DatabaseInstance
)
from Includes.lakebase_helpers import wait_for_instance

w = WorkspaceClient()

# Synced Tables requires a legacy provisioned instance (not an autoscaling project).
# Create one if it doesn't already exist.
sync_instance_name = f"{LAB_PREFIX}-sync".replace("_", "-")
try:
    w.database.get_database_instance(name=sync_instance_name)
    print(f"Reusing existing instance: {sync_instance_name}")
except Exception:
    print(f"Creating instance: {sync_instance_name} ...")
    w.database.create_database_instance(DatabaseInstance(name=sync_instance_name, capacity="CU_1"))
    wait_for_instance(sync_instance_name)
    print(f"  ✅ {sync_instance_name} ready")

# create_synced_database_table takes a SyncedDatabaseTable object, not keyword args.
# Fields are split: instance/database info on SyncedDatabaseTable; source/sync config on SyncedTableSpec.
w.database.create_synced_database_table(
    SyncedDatabaseTable(
        name=f"{LAB_CATALOG}.{LAB_SCHEMA}.customers_synced",
        database_instance_name=sync_instance_name,
        logical_database_name="databricks_postgres",
        spec=SyncedTableSpec(
            source_table_full_name=f"{LAB_CATALOG}.{LAB_SCHEMA}.customers",
            primary_key_columns=["customer_id"],
            # CONTINUOUS requires Auto CDF (preview). Use SNAPSHOT until the preview is enabled.
            scheduling_policy=SyncedTableSchedulingPolicy.SNAPSHOT,
            create_database_objects_if_missing=True,
        ),
    )
)
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · Sync modes
# MAGIC
# MAGIC | Mode | When it runs | Use case |
# MAGIC |---|---|---|
# MAGIC | `SNAPSHOT` | Manual or scheduled | One-time bulk load |
# MAGIC | `CONTINUOUS` | Streaming via CDC | Live OLTP-shaped data |
# MAGIC | `TRIGGERED` | On Delta table commit | Cost-efficient near-real-time |
# MAGIC
# MAGIC **Continuous sync internals:** Lakebase reads Delta's change data feed (CDF) and applies upserts to Postgres. Lag is typically 5-30 seconds.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D · Schema evolution

# COMMAND ----------

# Add a column to the source Delta table:
spark.sql(f"""
  ALTER TABLE {LAB_CATALOG}.{LAB_SCHEMA}.customers
  ADD COLUMN signup_source STRING
""")
spark.sql(f"""
  UPDATE {LAB_CATALOG}.{LAB_SCHEMA}.customers
  SET signup_source = element_at(array('web','app','referral'), (pmod(customer_id,3)).cast('int')+1)
""")

# What happens in Lakebase:
# - Continuous sync detects the new column
# - Adds it as nullable to the synced Postgres table
# - Backfills new rows; existing rows stay null until they're updated in source
#
# What's NOT automatic:
# - Type widening that's not strictly compatible (INT → BIGINT works; STRING → INT does not)
# - Renames (manifest as drop + add)
# - Constraint changes

# COMMAND ----------

# MAGIC %md
# MAGIC ## E · Verify the sync

# COMMAND ----------

# DBTITLE 1,Cell 11
import time
from databricks.sdk import WorkspaceClient
from sqlalchemy import create_engine, text

w = WorkspaceClient()

# Get Lakebase instance connection info
instance = w.database.get_database_instance(name=sync_instance_name)
# DatabaseInstance has read_write_dns, not connection_uri; credential must be generated separately
token = w.database.generate_database_credential(
    request_id=f"sync-verify-{int(time.time())}",
    instance_names=[sync_instance_name],
).token
conn_uri = (
    f"postgresql+psycopg://{w.current_user.me().user_name}:{token}"
    f"@{instance.read_write_dns}:5432/databricks_postgres?sslmode=require"
)

engine = create_engine(conn_uri)

with engine.begin() as cn:
    rows = cn.execute(text(
        "SELECT count(*) FROM lab.customers_synced"
    )).first()
    print(f"Postgres rows in customers_synced: {rows[0] if rows else '?'}")
    cols = cn.execute(text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name='customers_synced' ORDER BY ordinal_position"
    )).all()
    print(f"Columns: {[c.column_name for c in cols]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## F · Reverse direction — Postgres → Delta
# MAGIC
# MAGIC When the OLTP app writes back (e.g. order status changes), to flow that into your lakehouse:
# MAGIC
# MAGIC 1. Use **Lakehouse Federation** for read-time access (no copy)
# MAGIC 2. OR use Postgres logical replication into a streaming pipeline that writes Delta
# MAGIC 3. OR poll a `last_modified` column from Spark structured streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ## G · Production notes
# MAGIC
# MAGIC - Sync **does not honour deletes** unless source is set up with CDF + `_change_type` column. For tables that experience deletes, ensure CDF is enabled.
# MAGIC - The synced table has Postgres-native types — `BIGINT` for `BIGINT`, `JSONB` for `MAP/STRUCT`, etc. Some Spark types (e.g. arrays of complex structs) translate awkwardly. Plan schema accordingly.
# MAGIC - Sync respects UC governance — only tables the workspace identity can read can be synced.
# MAGIC - Cost: paid as standard Lakebase compute; sync workers run as part of the instance.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** **07 Demo - pgvector for AI Workloads**.