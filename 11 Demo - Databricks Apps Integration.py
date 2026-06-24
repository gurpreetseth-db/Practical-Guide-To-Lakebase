# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Demo — Databricks Apps Integration
# MAGIC
# MAGIC **Duration:** ~30 min · The 200-level course's bonus lab covered "create an app and connect to Lakebase." This module focuses on **production patterns**:
# MAGIC
# MAGIC - Service-principal authentication (no user passwords in the app)
# MAGIC - Auto-rotating OAuth tokens
# MAGIC - Connection pooling
# MAGIC - Blue-green deployment via branches
# MAGIC - Wiring Lakebase as an app **resource** (env-var injection)

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2" "sqlalchemy>=2"
# MAGIC %pip install --upgrade "databricks-sdk>=0.40"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · Resource binding pattern (the right way)
# MAGIC
# MAGIC In your app's `app.yaml`, declare Lakebase as a resource:
# MAGIC
# MAGIC ```yaml
# MAGIC command:
# MAGIC   - python
# MAGIC   - app.py
# MAGIC
# MAGIC env:
# MAGIC   - name: LAKEBASE_INSTANCE_NAME
# MAGIC     valueFrom: my-lakebase
# MAGIC
# MAGIC resources:
# MAGIC   - name: my-lakebase
# MAGIC     description: "Production OLTP database"
# MAGIC     database:
# MAGIC       instance_name: csi-prod-db
# MAGIC       database_name: postgres
# MAGIC       permission: CAN_USE
# MAGIC ```
# MAGIC
# MAGIC The app SP gets `CAN_USE` access automatically. App code reads `LAKEBASE_INSTANCE_NAME` and uses the SP's credentials — no human passwords involved.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · App code template

# COMMAND ----------

# This goes in your app.py (production-ready):
APP_TEMPLATE = '''
import os
from databricks.sdk import WorkspaceClient
from sqlalchemy import create_engine, event, text

INSTANCE = os.environ["LAKEBASE_INSTANCE_NAME"]
DATABASE = os.environ.get("LAKEBASE_DATABASE_NAME", "postgres")

w = WorkspaceClient()  # auto-uses SP token in Apps env
USER = w.current_user.me().user_name
HOST = w.database.get_database_instance(name=INSTANCE).read_write_dns

def get_token():
    return w.database.generate_database_credential(
        request_id="app", instance_names=[INSTANCE]
    ).token

# Initial token used to construct engine
engine = create_engine(
    f"postgresql+psycopg://{USER}:{get_token()}@{HOST}:5432/{DATABASE}?sslmode=require",
    pool_size=10, pool_pre_ping=True, pool_recycle=1800,
)

@event.listens_for(engine, "do_connect")
def _refresh(dialect, conn_rec, cargs, cparams):
    cparams["password"] = get_token()  # always fresh

# Use it
def query():
    with engine.begin() as cn:
        return cn.execute(text("SELECT count(*) FROM orders")).scalar()
'''
print(APP_TEMPLATE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · Blue-green deployment via branches
# MAGIC
# MAGIC Deploy schema changes safely:
# MAGIC
# MAGIC 1. Create branch `prod_v2` from prod
# MAGIC 2. Apply migration to `prod_v2`
# MAGIC 3. Update app's `LAKEBASE_INSTANCE_NAME` to `prod_v2`
# MAGIC 4. Watch metrics; if good, drop old prod; if bad, flip back

# COMMAND ----------

# MAGIC %md
# MAGIC ## D · MLflow + Lakebase
# MAGIC
# MAGIC When logging an MLflow model that uses Lakebase, register it as a resource so the model serving endpoint inherits access:
# MAGIC
# MAGIC ```python
# MAGIC import mlflow
# MAGIC from mlflow.models.resources import DatabaseResource
# MAGIC
# MAGIC mlflow.pyfunc.log_model(
# MAGIC     "model",
# MAGIC     python_model=MyRagModel(),
# MAGIC     resources=[DatabaseResource(database_name="my-lakebase")],
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** **12 Capstone - End-to-End RAG App on Lakebase**.