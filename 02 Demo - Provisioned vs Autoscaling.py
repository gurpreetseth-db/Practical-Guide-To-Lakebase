# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Demo — Provisioned vs Autoscaling
# MAGIC
# MAGIC **Duration:** ~30 min · **Type:** Hands-on · **Prerequisite:** Module 01
# MAGIC
# MAGIC Lakebase offers two compute modes. This module helps you pick the right one for any given workload, demonstrates each, and shows how to migrate between them.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2" "sqlalchemy>=2"
# MAGIC %pip install --upgrade "databricks-sdk>=0.40"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · Decision matrix
# MAGIC
# MAGIC | Workload characteristic | Provisioned | Autoscaling |
# MAGIC |---|---|---|
# MAGIC | Predictable traffic 24×7 | ✅ best | OK |
# MAGIC | Bursty / event-driven | ❌ wastes $ | ✅ best |
# MAGIC | Strict tail-latency SLO (p99 < 50ms) | ✅ no scale-up cold start | ⚠️ first request after scale-out adds ~100ms |
# MAGIC | Dev / test / non-prod | ❌ over-provisioned | ✅ scale to 0.25 (or 0) |
# MAGIC | RAG / batch ML inference jobs | OK | ✅ scales with the batch |
# MAGIC | OLTP for high-traffic app | ✅ predictable | ✅ if traffic is bursty enough to justify |
# MAGIC | You can predict capacity needs ±20% | ✅ pick the right CU and lock in | overhead not worth it |
# MAGIC | You really can't predict | ⚠️ size for peak (expensive) | ✅ |
# MAGIC | Multi-region replication | ✅ easier to reason about | ✅ |
# MAGIC | "I want to scale to zero overnight" | ❌ not supported | ✅ minimum can be very low |
# MAGIC
# MAGIC **Default recommendation for a new project**: start with autoscaling at min=0.5, max=4 CU. Watch utilization for 2 weeks. If average utilization is consistently >70% AND scale events are frequent, switch to provisioned at the steady-state size.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · Create a Provisioned instance

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance
from Includes.lakebase_helpers import wait_for_instance

w = WorkspaceClient()

prov_name = f"{LAB_PREFIX}-provisioned".replace("_", "-")
print(f"Creating PROVISIONED instance: {prov_name}")

w.database.create_database_instance(
    DatabaseInstance(
        name=prov_name,
        capacity="CU_1",   # fixed 1 CU — won't scale
    )
)
wait_for_instance(prov_name, timeout_seconds=600)
print(f"  ✅ {prov_name} ready")

# COMMAND ----------

# MAGIC %md 
# MAGIC [Lakebase Provisioned ](https://docs.databricks.com/aws/en/oltp/instances/)is the original Lakebase offering that uses provisioned compute you scale manually. For supported regions, see Region availability. For the latest version of Lakebase, with autoscaling compute, scale-to-zero, branching, and instant restore, see Lakebase Autoscaling.
# MAGIC
# MAGIC Since March 12, 2026, new Lakebase instances are created as Autoscaling projects. Existing Provisioned instances are being upgraded automatically to Autoscaling, starting in June 2026. For details, see [Upgrade to Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/upgrade-to-autoscaling).
# MAGIC
# MAGIC

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance
from Includes.lakebase_helpers import wait_for_instance

w = WorkspaceClient()

try:
    w.database.delete_database_instance(
        name=prov_name
    )
    wait_for_instance(prov_name, timeout_seconds=600)
    print(f"✅ {prov_name} Deleted Successfully ")
except Exception as e:
    if "not found" in str(e).lower():
        print(f"ℹ️  {prov_name} does not exist (already deleted).")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · Create an Autoscaling instance

# COMMAND ----------

# Lakebase Autoscaling uses the Postgres API (w.postgres) — not the legacy Database Instance API.
# Autoscaling is configured per-endpoint within a project.

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Project, ProjectSpec, Endpoint, EndpointSpec, EndpointType, Duration, FieldMask
)

w = WorkspaceClient()

auto_project_id = f"{LAB_PREFIX}-autoscale".replace("_", "-")
print(f"Creating AUTOSCALE project: {auto_project_id}")

# Step 1: Create the project
try:
    operation = w.postgres.create_project(
        project=Project(
            spec=ProjectSpec(
                display_name=f"{LAB_PREFIX} Autoscale Demo",
                pg_version=17,
            )
        ),
        project_id=auto_project_id,
    )
    result = operation.wait()
    print(f"  ✅ Project created: {result.name}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"  ℹ️  Project already exists, continuing...")
        result = w.postgres.get_project(name=f"projects/{auto_project_id}")
    else:
        raise e

# Step 2: Configure autoscaling on the endpoint (min 0.5 CU, max 1 CU, scale-to-zero)
# Discover the default branch dynamically
branch_name = result.status.default_branch if result.status and result.status.default_branch else f"projects/{auto_project_id}/branches/production"
branch_id = branch_name.split("/")[-1]

# List endpoints to find the default read-write endpoint
endpoints = list(w.postgres.list_endpoints(parent=f"projects/{auto_project_id}/branches/{branch_id}"))
print(f"  Found {len(endpoints)} endpoint(s)")

if endpoints:
    ep = endpoints[0]
    ep_name = ep.name  # full resource name
    print(f"  Updating endpoint '{ep_name}' with autoscale min=0.5 CU, max=1 CU, scale-to-zero...")

    # Update endpoint with autoscaling limits and suspend timeout (scale-to-zero)
    w.postgres.update_endpoint(
        name=ep_name,
        endpoint=Endpoint(
            spec=EndpointSpec(
                endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                autoscaling_limit_min_cu=0.5,
                autoscaling_limit_max_cu=1.0,
                # Scale-to-zero: suspend after 5 minutes of inactivity
                suspend_timeout_duration=Duration(seconds=300),
            )
        ),
        update_mask=FieldMask(field_mask=["spec.autoscaling_limit_min_cu", "spec.autoscaling_limit_max_cu", "spec.suspension"]),
    ).wait()
    print(f"  ✅ Autoscale configured: min 0.5 CU, max 1 CU, scale-to-zero enabled (5 min timeout)")
else:
    print("  ⚠️ No endpoints found — the project may still be provisioning. Retry shortly.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D · Connect With Autoscale Instance
# MAGIC
# MAGIC

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from sqlalchemy import create_engine, text

w = WorkspaceClient()
user = w.current_user.me().user_name
auto_project_id=auto_project_id.replace("_", "-")

# Retrieve endpoint information for the autoscaling project
result = w.postgres.get_project(name=f"projects/{auto_project_id}")
branch_name = result.status.default_branch if result.status and result.status.default_branch else f"projects/{auto_project_id}/branches/production"
branch_id = branch_name.split("/")[-1]
endpoints = list(w.postgres.list_endpoints(parent=f"projects/{auto_project_id}/branches/{branch_id}"))
ep = endpoints[0]
ep_name = ep.name
host = ep.status.hosts.host
cred = w.postgres.generate_database_credential(endpoint=ep_name)

auto_eng = create_engine(
    f"postgresql+psycopg://{user}:{cred.token}@{host}:5432/postgres?sslmode=require",
    pool_pre_ping=True,
)

with auto_eng.connect() as cn:
    rows = cn.execute(text("SELECT current_database(), current_schema()")).all()
    print("Test select result:", rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## E · Resize a autoscale instance (online, no downtime)

# COMMAND ----------

import time
from databricks.sdk.service.postgres import Endpoint, EndpointSpec, EndpointType, Duration, FieldMask

print(f"Resizing autoscale limits for {auto_project_id} endpoint: min=0.5 CU → max=2 CU...")

# Retrieve project and endpoint info
result = w.postgres.get_project(name=f"projects/{auto_project_id}")
branch_name = result.status.default_branch if result.status and result.status.default_branch else f"projects/{auto_project_id}/branches/production"
branch_id = branch_name.split("/")[-1]
endpoints = list(w.postgres.list_endpoints(parent=f"projects/{auto_project_id}/branches/{branch_id}"))
ep = endpoints[0]
ep_name = ep.name

# Update autoscaling limits (max CU increased to 2)
w.postgres.update_endpoint(
    name=ep_name,
    endpoint=Endpoint(
        spec=EndpointSpec(
            endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
            autoscaling_limit_min_cu=0.5,
            autoscaling_limit_max_cu=2.0,
            suspend_timeout_duration=Duration(seconds=300),
        )
    ),
    update_mask=FieldMask(field_mask=["spec.autoscaling_limit_min_cu", "spec.autoscaling_limit_max_cu", "spec.suspension"]),
).wait()

print("  ✅ Autoscale limits updated: min 0.5 CU, max 2 CU, scale-to-zero enabled (5 min timeout)")



# COMMAND ----------

# MAGIC %md
# MAGIC **Resize back to 0.5 - 1**

# COMMAND ----------

import time
from databricks.sdk.service.postgres import Endpoint, EndpointSpec, EndpointType, Duration, FieldMask

print(f"Resizing autoscale limits for {auto_project_id} endpoint: min=0.5 CU → max=1 CU...")

# Retrieve project and endpoint info
result = w.postgres.get_project(name=f"projects/{auto_project_id}")
branch_name = result.status.default_branch if result.status and result.status.default_branch else f"projects/{auto_project_id}/branches/production"
branch_id = branch_name.split("/")[-1]
endpoints = list(w.postgres.list_endpoints(parent=f"projects/{auto_project_id}/branches/{branch_id}"))
ep = endpoints[0]
ep_name = ep.name

# Update autoscaling limits (max CU decreased to 1)
w.postgres.update_endpoint(
    name=ep_name,
    endpoint=Endpoint(
        spec=EndpointSpec(
            endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
            autoscaling_limit_min_cu=0.5,
            autoscaling_limit_max_cu=1.0,
            suspend_timeout_duration=Duration(seconds=300),
        )
    ),
    update_mask=FieldMask(field_mask=["spec.autoscaling_limit_min_cu", "spec.autoscaling_limit_max_cu", "spec.suspension"]),
).wait()

print("  ✅ Autoscale limits updated: min 0.5 CU, max 1 CU, scale-to-zero enabled (5 min timeout)")



# COMMAND ----------

# MAGIC %md
# MAGIC ## F · Generate load and observe autoscale (optional, ~10 min)
# MAGIC
# MAGIC Skip this if the autoscale instance creation failed above. Otherwise:

# COMMAND ----------

# DBTITLE 1,F · Stress test — scale to 2 CU
# ─────────────────────────────────────────────────────────────────────────────
# F · Stress test — trigger autoscaling to 2 CU on databricks_postgres
#
# Open Lakebase UI during execution:
#   • Monitoring tab  → compute graph should show a step-up to 2 CU
#   • Active Queries  → live query list confirms concurrent load
# ─────────────────────────────────────────────────────────────────────────────
import time, concurrent.futures
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import Endpoint, EndpointSpec, EndpointType, Duration, FieldMask
from sqlalchemy import create_engine, text

# ── 1. Self-contained connection setup (fresh token & endpoint info) ──────────
w      = WorkspaceClient()
user   = w.current_user.me().user_name

result     = w.postgres.get_project(name=f"projects/{auto_project_id}")
branch_id  = result.status.default_branch.split("/")[-1]
ep         = list(w.postgres.list_endpoints(
               parent=f"projects/{auto_project_id}/branches/{branch_id}"))[0]
ep_name    = ep.name
host       = ep.status.hosts.host
print(f"Endpoint : {ep_name}")
print(f"Host     : {host}")

def _engine(db: str):
    """Return a fresh SQLAlchemy engine with a new OAuth token."""
    cred = w.postgres.generate_database_credential(endpoint=ep_name)
    return create_engine(
        f"postgresql+psycopg://{user}:{cred.token}@{host}:5432/{db}?sslmode=require",
        pool_pre_ping=True, pool_size=50, max_overflow=10,
    )

# ── 2. Ensure autoscale ceiling is 2 CU ──────────────────────────────────────
#print("\nSetting autoscale ceiling to max=2 CU...")
#w.postgres.update_endpoint(
#    name=ep_name,
#    endpoint=Endpoint(spec=EndpointSpec(
#        endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
#        autoscaling_limit_min_cu=0.5,
#        autoscaling_limit_max_cu=2.0,
#        suspend_timeout_duration=Duration(seconds=300),
#    )),
#    update_mask=FieldMask(field_mask=[
#        "spec.autoscaling_limit_min_cu",
#        "spec.autoscaling_limit_max_cu",
#        "spec.suspension",
#    ]),
#).wait()
#print("  ✅ Autoscale: min=0.5 CU, max=2 CU, scale-to-zero=5 min")

# ── 3. Connect to databricks_postgres (create if absent) ─────────────────────
print("\nConnecting to databricks_postgres database...")
try:
    eng = _engine("databricks_postgres")
    with eng.connect() as cn:
        db_used = cn.execute(text("SELECT current_database()")).scalar()
    print(f"  ✅ Connected to: {db_used}")
except Exception:
    # Database doesn't exist yet — create it from the default postgres db
    base_eng = _engine("postgres")
    with base_eng.execution_options(isolation_level="AUTOCOMMIT").connect() as cn:
        exists = cn.execute(
            text("SELECT 1 FROM pg_database WHERE datname='databricks_postgres'")
        ).scalar()
        if not exists:
            cn.execute(text("CREATE DATABASE databricks_postgres"))
            print("  ✅ Created database: databricks_postgres")
    base_eng.dispose()
    eng = _engine("databricks_postgres")
    print("  ✅ Connected to: databricks_postgres")

# ── 4. Seed 500k-row stress table ────────────────────────────────────────────
print("\nSeeding stress_test table (500k rows)...")
with eng.begin() as cn:
    cn.execute(text("DROP TABLE IF EXISTS stress_test"))
    cn.execute(text("""
        CREATE TABLE stress_test AS
        SELECT
            gs                                         AS id,
            md5(random()::text)                        AS label,
            (random() * 10000)::int                    AS score,
            now() - (random() * interval '1 year')     AS created_at
        FROM generate_series(1, 500000) gs
    """))
    cn.execute(text("CREATE INDEX ON stress_test (score)"))
print("  ✅ stress_test ready: 500k rows + index")

# ── 5. Stress test: 50 concurrent workers × 500 CPU-heavy queries ─────────────
WORKERS   = 50
ROUNDS    = 500
POLL_SECS = 15

# Window-function aggregation query — heavy CPU, visible in Active Queries UI
stress_q = text("""
    WITH ranked AS (
        SELECT
            score / 500                                  AS bucket,
            label,
            score,
            row_number() OVER (
                PARTITION BY score / 500 ORDER BY score DESC
            ) AS rn
        FROM stress_test
    )
    SELECT
        bucket,
        count(*)                    AS total,
        avg(score)::numeric(10,2)   AS avg_score,
        max(label)                  AS top_label
    FROM ranked
    WHERE rn <= 200
    GROUP BY bucket
    ORDER BY bucket
""")

def run_one(_):
    with eng.connect() as cn:
        cn.execute(stress_q).fetchall()

print(f"\n🔥 Starting stress test: {WORKERS} workers × {ROUNDS} queries")
print("   Open Lakebase UI → Active Queries and Monitoring to observe scaling\n")

t0, done, last_poll = time.time(), [0], [time.time()]

with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
    for _ in ex.map(run_one, range(ROUNDS)):
        done[0] += 1
        now = time.time()
        if now - last_poll[0] >= POLL_SECS:
            last_poll[0] = now
            s = w.postgres.get_endpoint(name=ep_name).status
            print(
                f"  [{int(now - t0):>4}s] {done[0]:>4}/{ROUNDS} queries done"
                f"  |  state={s.current_state.value}"
                f"  |  CU range={s.autoscaling_limit_min_cu}–{s.autoscaling_limit_max_cu}"
            )

elapsed = time.time() - t0
print(f"\n✅ Stress test complete in {elapsed:.1f}s")

# Final endpoint status
s = w.postgres.get_endpoint(name=ep_name).status
print(f"Final state : {s.current_state.value}")
print(f"Autoscale   : {s.autoscaling_limit_min_cu}–{s.autoscaling_limit_max_cu} CU")

# ── 6. Cleanup ────────────────────────────────────────────────────────────────
with eng.begin() as cn:
    cn.execute(text("DROP TABLE IF EXISTS stress_test"))
eng.dispose()
print("\nstress_test table dropped. Done.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## G · Cost characteristics
# MAGIC
# MAGIC Approximate $/CU-hour as of mid-2026 (your contract may differ):
# MAGIC
# MAGIC | Mode | $/CU-hour | When you save |
# MAGIC |---|---|---|
# MAGIC | Provisioned | base rate | n/a — you pay for what you provision |
# MAGIC | Autoscale | base rate × (avg CU / max CU) | When peak ≫ average; rule of thumb: if avg < 50% of peak, autoscale wins |
# MAGIC
# MAGIC ### Quick math
# MAGIC
# MAGIC Workload that needs 4 CU during 9-5 weekdays (40 hrs/week), idle 0.5 CU rest:
# MAGIC
# MAGIC - **Provisioned at 4 CU**: 4 × 168 = 672 CU-hours/week
# MAGIC - **Autoscale 0.5–4**: 4 × 40 + 0.5 × 128 = 224 CU-hours/week → **~67% saving**
# MAGIC
# MAGIC Workload that needs 2 CU steady 24×7:
# MAGIC
# MAGIC - **Provisioned at 2 CU**: 2 × 168 = 336 CU-hours/week
# MAGIC - **Autoscale 1–2**: ~ 2 × 168 = 336 CU-hours/week → **same cost, more risk** (autoscale always within band but mostly at 2 anyway)

# COMMAND ----------

# MAGIC %md
# MAGIC ## H · Cleanup demo instances (optional — keep for next module)

# COMMAND ----------

# Keep these instances around if you're proceeding to module 03 or beyond;
# the CMK module creates its own dedicated instance.
# Run only when you're done with this module:
# w.database.delete_database_instance(name=prov_name, force=True)
# w.database.delete_database_instance(name=auto_name, force=True)
print("Skipping deletion — re-use these instances in modules 04+ or run "
      "`%run ./Includes/Cleanup` at session end.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** **03 Demo - Customer-Managed Keys (CMK)** — the centerpiece of this lab.