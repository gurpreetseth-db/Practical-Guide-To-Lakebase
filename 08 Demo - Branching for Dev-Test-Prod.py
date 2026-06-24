# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Demo — Branching for Dev-Test-Prod
# MAGIC
# MAGIC **Duration:** ~30 min · Lakebase branches are **copy-on-write forks** of a database. Cheap, instant, isolated. This module shows you how to use them for:
# MAGIC
# MAGIC 1. Running a destructive migration in dev
# MAGIC 2. Validating schema changes against prod-shaped data without copying TBs
# MAGIC 3. A/B testing schemas
# MAGIC 4. PITR-style time-travel branches

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
sync_instance_name = f"{LAB_PREFIX}-branching-prod".replace("_", "-")
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
# MAGIC ## A · Prepare some "prod" data

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
    cn.execute(text("DROP TABLE IF EXISTS subscribers"))
    cn.execute(text("""
        CREATE TABLE subscribers (
            id BIGSERIAL PRIMARY KEY,
            email TEXT UNIQUE,
            tier TEXT,
            signed_up_at TIMESTAMPTZ DEFAULT now()
        )
    """))
    cn.execute(text("""
        INSERT INTO subscribers (email, tier)
        SELECT 'user' || g || '@example.com',
               (ARRAY['free','pro','enterprise'])[1 + (g % 3)]
        FROM generate_series(1, 10000) AS g
    """))
    print("  ✅ 10,000 subscribers in prod")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · Create a dev branch

# COMMAND ----------

# DBTITLE 1,Cell 8
# Branching is a copy-on-write feature of autoscaling (Postgres) projects.
# Discover the autoscaling project via raw REST.
projects = w.api_client.do("GET", "/api/2.0/postgres/projects").get("projects", [])
if not projects:
    raise RuntimeError("No autoscaling projects found — run Notebook 02 first to create one.")

# Use the project_id (display name) which is the API resource identifier, not the uid
proj_id = projects[0]["status"]["project_id"]
print(f"Using autoscaling project: {proj_id}")

from databricks.sdk.service.postgres import Branch, BranchSpec

branch_label = "dev"
try:
    op = w.postgres.create_branch(
        parent=f"projects/{proj_id}",
        branch=Branch(
            spec=BranchSpec(
                source_branch=f"projects/{proj_id}/branches/production",
                no_expiry=True,
            )
        ),
        branch_id=branch_label,
    )
    op.wait()
    print(f"  Branch created: {branch_label}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"  Reusing existing branch: {branch_label}")
    else:
        raise

dev_branch_name = f"projects/{proj_id}/branches/{branch_label}"
print(f"\n\u2705 Branch '{branch_label}' ready (copy-on-write \u2014 instant, no data movement)")
print(f"   Resource path: {dev_branch_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · Run a destructive migration in dev

# COMMAND ----------

# DBTITLE 1,Cell 10
def _branch_engine(branch_path):
    """Build a SQLAlchemy engine for a Lakebase autoscaling branch."""
    eps = w.api_client.do(
        "GET", f"/api/2.0/postgres/{branch_path}/endpoints"
    ).get("endpoints", [])
    if not eps:
        raise RuntimeError(f"No endpoints found for {branch_path}")
    ep_name = eps[0]["name"]
    host = eps[0]["status"]["hosts"]["host"]
    cred = w.api_client.do(
        "POST", "/api/2.0/postgres/credentials", body={"endpoint": ep_name}
    )
    token = cred["token"]
    user = w.current_user.me().user_name
    return create_engine(
        f"postgresql+psycopg://{user}:{token}@{host}:5432/databricks_postgres?sslmode=require"
    )

dev_eng  = _branch_engine(dev_branch_name)
prod_eng = _branch_engine(f"projects/{proj_id}/branches/production")

with dev_eng.begin() as cn:
    # Add a column with a default that requires a table rewrite — risky on prod!
    cn.execute(text("ALTER TABLE subscribers ADD COLUMN trial_ends_at TIMESTAMPTZ DEFAULT now() + INTERVAL '14 days'"))
    # Verify it worked + measure time
    cn.execute(text("SELECT count(*) FROM subscribers WHERE trial_ends_at IS NOT NULL"))

# # PROD is unaffected:
with prod_eng.begin() as cn:
    cols = cn.execute(text(
        "SELECT column_name FROM information_schema.columns WHERE table_name='subscribers'"
    )).all()
    print("Prod columns:", [c.column_name for c in cols])
    # → ['id', 'email', 'tier', 'signed_up_at'] — no trial_ends_at

# COMMAND ----------

# MAGIC %md
# MAGIC ## D · A/B test schemas
# MAGIC
# MAGIC Run two app variants pointing at branch A and branch B, drive synthetic load, compare metrics, pick a winner.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E · Time-travel / PITR-style branch
# MAGIC
# MAGIC Create a branch from a specific timestamp to inspect "what did the data look like 3 hours ago?":

# COMMAND ----------

# DBTITLE 1,Cell 13
from databricks.sdk.service.postgres import Branch, BranchSpec
from datetime import datetime, timedelta
from google.protobuf.timestamp_pb2 import Timestamp as PbTimestamp

pitr_label = "pitr-2h"
try:
    op = w.postgres.create_branch(
        parent=f"projects/{proj_id}",
        branch=Branch(
            spec=BranchSpec(
                source_branch=f"projects/{proj_id}/branches/production",
                source_branch_time=PbTimestamp(seconds=int((datetime.utcnow() - timedelta(minutes=30)).timestamp())),
                no_expiry=True,
            )
        ),
        branch_id=pitr_label,
    )
    op.wait()
    print(f"  PITR branch created: {pitr_label}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"  Reusing existing PITR branch: {pitr_label}")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## F · Promoting / merging a branch
# MAGIC
# MAGIC There's no `git merge` for branches — they're independent. To promote dev to prod:
# MAGIC
# MAGIC 1. Apply the same migration to prod via CI
# MAGIC 2. OR snapshot dev → restore over prod (downtime; rare in practice)
# MAGIC 3. OR use blue-green: rename branches via app config flip

# COMMAND ----------

# MAGIC %md
# MAGIC ## G · Cleanup: branches auto-expire (with TTL); manual delete:

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
# MAGIC **Next:** **09 Demo - Observability & Performance**.