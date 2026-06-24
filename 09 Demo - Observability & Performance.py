# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Demo — Observability & Performance
# MAGIC
# MAGIC **Duration:** ~30 min · Three lenses on Lakebase performance: **Postgres internals** (`pg_stat_*`, `EXPLAIN`), **Databricks system tables** (cost, capacity), and **practical tuning** patterns.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2" "sqlalchemy>=2"
# MAGIC %pip install --upgrade "databricks-sdk>=0.40"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from Includes.lakebase_helpers import wait_for_instance
from databricks.sdk.service.database import (
    SyncedDatabaseTable, SyncedTableSpec, SyncedTableSchedulingPolicy, DatabaseInstance
)
w = WorkspaceClient()

# Synced Tables requires a legacy provisioned instance (not an autoscaling project).
# Create one if it doesn't already exist.
sync_instance_name = f"{LAB_PREFIX}-obs".replace("_", "-")
try:
    w.database.get_database_instance(name=sync_instance_name)
    print(f"Reusing existing instance: {sync_instance_name}")
except Exception:
    print(f"Creating instance: {sync_instance_name} ...")
    w.database.create_database_instance(DatabaseInstance(name=sync_instance_name, capacity="CU_1"))
    wait_for_instance(sync_instance_name)
    print(f"  ✅ {sync_instance_name} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · `pg_stat_statements` — find slow queries

# COMMAND ----------


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
    # The extension is pre-installed; enable it for the session
    cn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_stat_statements"))

    # Generate some load
    cn.execute(text("CREATE TABLE IF NOT EXISTS perf_demo (id bigserial PRIMARY KEY, val text)"))
    cn.execute(text("INSERT INTO perf_demo (val) SELECT 'v' || g FROM generate_series(1, 100000) g"))

    # Pull top slow queries
    rows = cn.execute(text("""
        SELECT query, calls, mean_exec_time, total_exec_time, rows
        FROM pg_stat_statements
        ORDER BY total_exec_time DESC
        LIMIT 10
    """)).all()

for r in rows:
    print(f"  calls={r.calls:>5} mean={r.mean_exec_time:.1f}ms  {r.query[:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · `EXPLAIN ANALYZE` — read the query plan

# COMMAND ----------


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
    plan = cn.execute(text("""
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT count(*) FROM perf_demo WHERE val LIKE 'v123%'
    """)).first()
    import json
    print(json.dumps(plan[0], indent=2)[:1500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · Index hygiene

# COMMAND ----------

# DBTITLE 1,Cell 10

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
    cn.execute(text("CREATE INDEX IF NOT EXISTS perf_demo_val_idx ON perf_demo (val)"))

with engine.execution_options(isolation_level="AUTOCOMMIT").connect() as cn:
    cn.execute(text("VACUUM ANALYZE perf_demo"))

with engine.begin() as cn:
    # Find unused indexes (size + low usage = candidates for drop)
    unused = cn.execute(text("""
        SELECT s.indexrelname, s.idx_scan, pg_size_pretty(pg_relation_size(s.indexrelid)) AS size
        FROM pg_stat_user_indexes s
        ORDER BY s.idx_scan ASC, pg_relation_size(s.indexrelid) DESC
        LIMIT 10
    """)).all()
    for r in unused:
        print(f"  {r.indexrelname:<40} scans={r.idx_scan:<6} size={r.size}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D · Connection + lock visibility

# COMMAND ----------

with engine.begin() as cn:
    rows = cn.execute(text("""
        SELECT pid, usename, application_name, state, query_start, wait_event
        FROM pg_stat_activity
        WHERE state IS NOT NULL
        LIMIT 20
    """)).all()
    for r in rows:
        print(f"  pid={r.pid} state={r.state} app={r.application_name or '-'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## E · Databricks system tables — capacity + cost

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lakebase usage in system tables (workspace must have system access enabled)
# MAGIC SELECT 
# MAGIC     usage_date,
# MAGIC     sku_name, 
# MAGIC     SUM(usage_quantity) AS `DBUs_Consumed`
# MAGIC FROM 
# MAGIC     system.billing.usage
# MAGIC WHERE 
# MAGIC     sku_name LIKE '%LAKEBASE%'
# MAGIC GROUP BY 
# MAGIC     usage_date, 
# MAGIC     sku_name
# MAGIC ORDER BY 
# MAGIC     usage_date DESC;;

# COMMAND ----------

# MAGIC %md
# MAGIC ## F · Tuning checklist
# MAGIC
# MAGIC - [ ] `EXPLAIN ANALYZE` for every query > 100ms — confirm index use
# MAGIC - [ ] `pg_stat_statements` reset weekly; review top-10 by `total_exec_time`
# MAGIC - [ ] `VACUUM ANALYZE` after bulk loads (Lakebase auto-vacuums but stats drift on big writes)
# MAGIC - [ ] HNSW index `ef_search` tuning if pgvector recall/latency is off
# MAGIC - [ ] Partition large tables by date if query patterns are time-bounded
# MAGIC - [ ] Set `maintenance_work_mem` higher for index builds (session-level)

# COMMAND ----------

# MAGIC %md
# MAGIC ## G · Cleanup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance
from Includes.lakebase_helpers import wait_for_instance

w = WorkspaceClient()

try:
    w.database.delete_database_instance(
        name=sync_instance_name
    )
    wait_for_instance(sync_instance_name, timeout_seconds=600)
    print(f"✅ {sync_instance_name} Deleted Successfully ")
except Exception as e:
    if "not found" in str(e).lower():
        print(f"ℹ️  {sync_instance_name} does not exist (already deleted).")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** **10 Demo - HA, Backup & PITR**.