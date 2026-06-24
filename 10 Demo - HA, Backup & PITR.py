# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Demo — HA, Backup & PITR
# MAGIC
# MAGIC **Duration:** ~30 min · Lakebase's storage-decoupled architecture changes the HA/DR conversation. Here's what you actually need to know — and one drill.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2" "sqlalchemy>=2"
# MAGIC %pip install --upgrade "databricks-sdk>=0.40"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · Built-in durability properties
# MAGIC
# MAGIC | Layer | Durability | RPO | RTO |
# MAGIC |---|---|---|---|
# MAGIC | Compute (CU) | Stateless beyond cache | n/a | seconds (auto-restart) |
# MAGIC | Page server cache | SSD; rebuilt from object storage | n/a | minutes |
# MAGIC | Object storage | 99.999999999% (cloud-provider) | sync write at commit | n/a |
# MAGIC | WAL | Synced to object storage on commit | 0 | n/a |
# MAGIC | Backups (continuous) | Replayable to any point in retention window | < 1 sec | minutes |
# MAGIC
# MAGIC **What this means:** Lakebase has effectively **zero RPO** out of the box for normal failure modes. RTO for a compute crash is "the time to restart compute" — typically tens of seconds.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · Backup retention
# MAGIC
# MAGIC Default retention windows:
# MAGIC
# MAGIC - **Provisioned**: 7 days continuous PITR window
# MAGIC - **Autoscale**: 7 days continuous PITR window
# MAGIC
# MAGIC Both are configurable up to 35 days. Set at instance creation or via update.

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · Drill — Point-in-Time Restore (PITR)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from Includes.lakebase_helpers import wait_for_instance
from databricks.sdk.service.database import (
    SyncedDatabaseTable, SyncedTableSpec, SyncedTableSchedulingPolicy, DatabaseInstance
)
w = WorkspaceClient()

# Synced Tables requires a legacy provisioned instance (not an autoscaling project).
# Create one if it doesn't already exist.
sync_instance_name = f"{LAB_PREFIX}_pitr_demo".replace("_", "-")
try:
    w.database.get_database_instance(name=sync_instance_name)
    print(f"Reusing existing instance: {sync_instance_name}")
except Exception:
    print(f"Creating instance: {sync_instance_name} ...")
    w.database.create_database_instance(DatabaseInstance(name=sync_instance_name, capacity="CU_1"))
    wait_for_instance(sync_instance_name)
    print(f"  ✅ {sync_instance_name} ready")

# COMMAND ----------


import time
from databricks.sdk import WorkspaceClient
from sqlalchemy import create_engine, text
from datetime import datetime, timezone, timedelta

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

# Set up some data
with engine.begin() as cn:
    cn.execute(text("DROP TABLE IF EXISTS pitr_demo"))
    cn.execute(text("CREATE TABLE pitr_demo (id bigserial PRIMARY KEY, val text, ts timestamptz default now())"))
    cn.execute(text("INSERT INTO pitr_demo (val) VALUES ('initial-state')"))

# Capture timestamp BEFORE the destructive change
SAFE_TIMESTAMP = datetime.now(timezone.utc) - timedelta(seconds=5)
time.sleep(10)  # ensure clear separation



# COMMAND ----------

# MAGIC %md
# MAGIC **Now do something destructive**

# COMMAND ----------

# Now do something destructive
with engine.begin() as cn:
    cn.execute(text("DELETE FROM pitr_demo"))
    cn.execute(text("INSERT INTO pitr_demo (val) VALUES ('post-delete-state')"))
    rows = cn.execute(text("SELECT * FROM pitr_demo")).all()
    print(f"After 'oops': {rows}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restore via branch from a past timestamp
# MAGIC
# MAGIC The cleanest pattern for "I deleted data, need it back": create a **branch from before the delete**, copy the rows you need, drop the branch.

# COMMAND ----------

# DBTITLE 1,Cell 12
# PITR branching requires an autoscaling project (Postgres API).
# Discover the autoscaling project in this workspace.
from databricks.sdk.service.postgres import Branch, BranchSpec
from google.protobuf.timestamp_pb2 import Timestamp as PbTimestamp

projects = w.api_client.do("GET", "/api/2.0/postgres/projects").get("projects", [])
if not projects:
    raise RuntimeError("No autoscaling project found — run Notebook 02 first.")
pitr_proj_id = projects[0]["status"]["project_id"]

pitr_label = f"{sync_instance_name}-pitr"
try:
    op = w.postgres.create_branch(
        parent=f"projects/{pitr_proj_id}",
        branch=Branch(
            spec=BranchSpec(
                source_branch=f"projects/{pitr_proj_id}/branches/production",
                source_branch_time=PbTimestamp(seconds=int(SAFE_TIMESTAMP.timestamp())),
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

restore_branch = f"projects/{pitr_proj_id}/branches/{pitr_label}"
print(f"\n\u2705 PITR branch ready: {restore_branch}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D · Compute failover
# MAGIC
# MAGIC On a compute crash, Lakebase auto-restarts the compute pod. Your client connections die; OAuth-token-based clients with retry logic reconnect transparently.
# MAGIC
# MAGIC **Drill: simulate a failover**:

# COMMAND ----------

# Force a restart by resizing — triggers a brief downtime as the new pod comes up
print("Triggering compute restart via resize...")
w.database.update_database_instance(
    name=sync_instance_name,
    database_instance=DatabaseInstance(name=sync_instance_name, capacity="CU_2"),
    update_mask="capacity",
)
wait_for_instance(sync_instance_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## E · Multi-region — the bigger DR conversation
# MAGIC
# MAGIC Lakebase storage is regional (S3 / ADLS / GCS). Cross-region DR options:
# MAGIC
# MAGIC | Pattern | RPO | RTO | Cost |
# MAGIC |---|---|---|---|
# MAGIC | Sync to a second-region Delta table (via Federation Sync) | minutes | hours | low |
# MAGIC | Restore from continuous backup in the same region | < 1 sec | minutes | included |
# MAGIC | Replicate to a Lakebase instance in another region | seconds | seconds | 2× |
# MAGIC
# MAGIC Most teams don't need multi-region for non-tier-0 workloads. Always test your assumed DR path before committing to an SLA.

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Cleanup

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
# MAGIC **Next:** **11 Demo - Databricks Apps Integration**.