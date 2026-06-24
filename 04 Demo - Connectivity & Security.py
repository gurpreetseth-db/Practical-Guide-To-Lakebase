# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Demo — Connectivity & Security
# MAGIC
# MAGIC **Duration:** ~45 min · **Type:** Hands-on · **Prerequisite:** Modules 01-03

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2" "sqlalchemy>=2"
# MAGIC %pip install --upgrade "databricks-sdk>=0.40"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · OAuth IAM tokens — the production auth method
# MAGIC
# MAGIC OAuth tokens **expire after 1 hour** by design. Production code must refresh.

# COMMAND ----------

# DBTITLE 1,Cell 5
from databricks.sdk import WorkspaceClient
from Includes.lakebase_helpers import get_oauth_engine, wait_for_instance
from sqlalchemy import text
import time

w = WorkspaceClient()

# All new Lakebase instances are autoscaling projects — use the Postgres API via raw REST
# (w.postgres is not yet surfaced on this SDK/runtime; w.database returns no instances)
projects_resp = w.api_client.do("GET", "/api/2.0/postgres/projects")
projects = projects_resp.get("projects", [])
if not projects:
    raise Exception("No Lakebase autoscaling project found.")

project = projects[0]
auto_project_id = project["name"].split("/")[-1]
branch_id = project["status"].get("default_branch", "").split("/")[-1] or "production"

# Get the primary read-write endpoint
eps_resp = w.api_client.do(
    "GET", f"/api/2.0/postgres/projects/{auto_project_id}/branches/{branch_id}/endpoints"
)
ep = eps_resp["endpoints"][0]
ep_name = ep["name"]          # full resource path e.g. projects/.../endpoints/primary
inst_name = auto_project_id   # kept for downstream cells that reference inst_name

# Fetch an OAuth token scoped to this endpoint (expires in ~3600s)
cred = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": ep_name})
naive_token = cred["token"]
print(f"Token starts with: {naive_token[:20]}... (expires in ~3600s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · Auto-refreshing engine
# MAGIC
# MAGIC Direct engine build using `w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": ep_name})` for the token and `ep["status"]["hosts"]["host"]` for the hostname.

# COMMAND ----------

# DBTITLE 1,Cell 7
from sqlalchemy import create_engine, text

# get_oauth_engine uses the legacy database API which cannot find autoscaling projects.
# Build the engine directly using endpoint info already fetched in Cell 5.
cred = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": ep_name})
host = ep["status"]["hosts"]["host"]
engine = create_engine(
    f"postgresql+psycopg://{w.current_user.me().user_name}:{cred['token']}"
    f"@{host}:5432/databricks_postgres?sslmode=require",
    pool_pre_ping=True,
)

with engine.begin() as cn:
    cn.execute(text("CREATE TABLE IF NOT EXISTS auth_demo (k TEXT, v TEXT)"))
    cn.execute(text("INSERT INTO auth_demo VALUES ('hello', 'world')"))
    print(cn.execute(text("SELECT * FROM auth_demo")).all())

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · Service principal authentication (for Databricks Apps)
# MAGIC
# MAGIC When running inside a Databricks App, the SP is the identity. The SDK auto-detects:

# COMMAND ----------

# DBTITLE 1,C · Service principal auth demo
# ─────────────────────────────────────────────────────────────────────────────
# C · Databricks Apps → Lakebase via Service Principal (SDK auth)
#
# How it works:
#   • In a Databricks App  → SDK auto-detects the SP OAuth token injected by
#     the platform (DATABRICKS_HOST + DATABRICKS_TOKEN env vars set for you).
#     w.current_user.me() returns the SP identity (e.g. sp-my-app@...).
#   • In a notebook (this demo) → same code runs as the human user.
#     The auth flow is identical; only the identity changes.
#
# The key security property: NO credentials are hard-coded. The SDK resolves
# the identity at runtime from the execution environment.
# ─────────────────────────────────────────────────────────────────────────────
import time, threading, pandas as pd
from databricks.sdk import WorkspaceClient
from sqlalchemy import create_engine, event, text

# ── 1. Resolve identity from SDK (auto-detects SP in App, user in notebook) ──
w   = WorkspaceClient()
me  = w.current_user.me()
print("Auth context")
print(f"  Identity  : {me.user_name}")
print(f"  Display   : {me.display_name}")
print(f"  SP groups : {[g.display for g in (me.groups or [])] or '(none / running as human user)'}")

# ── 2. Auto-refreshing token factory ─────────────────────────────────────────
# Tokens expire in 1 hour. Databricks Apps are long-running — refresh
# proactively 5 minutes before expiry so connections never hit an expired token.
REFRESH_BUFFER = 300   # seconds before expiry to refresh
_tok_state = {"token": None, "expires_at": 0.0}
_tok_lock  = threading.Lock()

def _fresh_token() -> str:
    """Return a live credential token; silently refresh when near expiry."""
    with _tok_lock:
        if _tok_state["token"] is None or time.time() > _tok_state["expires_at"] - REFRESH_BUFFER:
            cred = w.api_client.do(
                "POST", "/api/2.0/postgres/credentials",
                body={"endpoint": ep_name},
            )
            _tok_state["token"]      = cred["token"]
            _tok_state["expires_at"] = time.time() + 3600   # 1-hour lifetime
        return _tok_state["token"]

# ── 3. Build SQLAlchemy engine with per-checkout token refresh ────────────────
sp_engine = create_engine(
    f"postgresql+psycopg://{me.user_name}:{_fresh_token()}"
    f"@{host}:5432/databricks_postgres?sslmode=require",
    pool_pre_ping=True,
    pool_recycle=1800,   # retire pooled conns every 30 min to pick up fresh tokens
)

@event.listens_for(sp_engine, "do_connect")
def _inject_fresh_token(dialect, conn_rec, cargs, cparams):
    """Re-check and inject a fresh token on every new connection checkout."""
    cparams["password"] = _fresh_token()

print(f"\nEngine        : {sp_engine.url.render_as_string(hide_password=True)}")
print(f"Token TTL     : ~{(_tok_state['expires_at'] - time.time()) / 60:.0f} min remaining")

# ── 4. Run a demo query ───────────────────────────────────────────────────────
with sp_engine.begin() as cn:
    cn.execute(text("DROP TABLE IF EXISTS sp_demo"))
    cn.execute(text("""
        CREATE TABLE sp_demo (
            id     SERIAL PRIMARY KEY,
            actor  TEXT,
            ts     TIMESTAMPTZ DEFAULT now()
        )
    """))
    cn.execute(text(f"INSERT INTO sp_demo (actor) VALUES ('{me.user_name}')"))
    rows = cn.execute(text(
        "SELECT id, actor, ts FROM sp_demo ORDER BY ts DESC LIMIT 5"
    )).fetchall()

df = pd.DataFrame(rows, columns=["id", "actor", "ts"])
print("\nLatest rows in sp_demo (actor = identity that inserted the row):")
display(df)
print("\n✅ In a Databricks App, 'actor' would show the SP's application identity.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D · IP allowlist
# MAGIC
# MAGIC Restrict Lakebase to specific source IPs (e.g. office VPN, Databricks workspace egress).

# COMMAND ----------

# Allowlist is set at instance level via update_database_instance.
# Format: list of CIDRs. To lock to Databricks workspace egress only, use the
# workspace's documented egress CIDR for your region.

# Example (commented; uncomment + edit to apply):
# w.database.update_database_instance(
#     name=inst_name,
#     database_instance=DatabaseInstance(
#         name=inst_name,
#         allowed_ip_cidrs=["203.0.113.0/24"],
#     ),
#     update_mask="allowed_ip_cidrs",
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## E · PrivateLink (AWS) / Private Endpoint (Azure/GCP)
# MAGIC
# MAGIC Production deployments often disable public Lakebase endpoints entirely. The setup is cloud-specific:
# MAGIC
# MAGIC ### AWS PrivateLink
# MAGIC 1. Account admin creates a Network Connectivity Configuration (NCC) in the Account Console
# MAGIC 2. Provisions a VPC endpoint in your VPC pointing at the Databricks Lakebase service
# MAGIC 3. Updates the workspace to use the NCC
# MAGIC 4. Disables public access on the Lakebase instance
# MAGIC
# MAGIC ### Azure Private Endpoint
# MAGIC Similar pattern — Workspace settings → Networking → Private Endpoint.
# MAGIC
# MAGIC ### GCP Private Service Connect
# MAGIC Similar — workspace-level config.
# MAGIC
# MAGIC See [Databricks docs — Lakebase networking](https://docs.databricks.com/lakebase/networking.html) for the current procedure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## F · TLS verification (sslmode)
# MAGIC
# MAGIC Lakebase requires TLS 1.2+. Always use `sslmode=require` minimum; `sslmode=verify-full` for paranoid mode (validates cert chain).

# COMMAND ----------

# Verify what sslmode our connection uses:
with engine.begin() as cn:
    rows = cn.execute(text("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")) .all()
    print(f"TLS active: {rows}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## G · Secrets management
# MAGIC
# MAGIC **Don't put long-lived passwords in code.** Use one of:
# MAGIC
# MAGIC - **OAuth tokens** (preferred — already shown above)
# MAGIC - **Databricks secrets** for break-glass passwords:
# MAGIC   ```bash
# MAGIC   databricks secrets put-secret lakebase-prod superuser-password --string-value '<...>'
# MAGIC   ```
# MAGIC   Read in code: `dbutils.secrets.get("lakebase-prod", "superuser-password")`

# COMMAND ----------

# MAGIC %md
# MAGIC ## H · Connection pooling
# MAGIC
# MAGIC Lakebase has a built-in pooler (each CU handles ~100 connections). For higher fan-out, put **PgBouncer** in transaction-pooling mode in front:
# MAGIC
# MAGIC ```ini
# MAGIC [databases]
# MAGIC mydb = host=<lakebase-host> port=5432 dbname=postgres
# MAGIC
# MAGIC [pgbouncer]
# MAGIC pool_mode = transaction
# MAGIC max_client_conn = 1000
# MAGIC default_pool_size = 50
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## I · Production checklist
# MAGIC - [ ] All apps use OAuth token + auto-refresh
# MAGIC - [ ] No password auth except for break-glass (in Databricks secrets)
# MAGIC - [ ] PrivateLink / private endpoint configured if data sensitivity warrants
# MAGIC - [ ] IP allowlist restricts to known egress points
# MAGIC - [ ] `sslmode=verify-full` for paranoid deployments
# MAGIC - [ ] Connection pool sized for expected fan-out

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** **05 Demo - Lakehouse Federation Pushdown**.