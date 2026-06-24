# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Demo — Lakehouse Federation Pushdown
# MAGIC
# MAGIC **Duration:** ~20 min · Lakebase appears as a **foreign catalog** in Unity Catalog. SQL warehouses + notebooks query it like any other catalog. This module focuses on **predicate pushdown** — what runs in Postgres vs in Spark.
# MAGIC
# MAGIC The 200-level course covered "create a foreign catalog and SELECT from it." This module goes deeper on:
# MAGIC
# MAGIC 1. Creating the foreign catalog
# MAGIC 2. Reading the query plan (`EXPLAIN`)
# MAGIC 3. Identifying what pushes down vs what spills to Spark
# MAGIC 4. Hybrid joins between Lakebase and Delta tables
# MAGIC 5. When to use Federation vs Sync vs direct connect

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2" "sqlalchemy>=2"
# MAGIC %pip install --upgrade "databricks-sdk>=0.40"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · Create the foreign catalog
# MAGIC
# MAGIC In the workspace UI: **Catalog Explorer → Create a catalog → Catalog Name → Type = Lakebase Postgres → Database Type = Autoscaling → Project → Branch → Postreg database**. Behind the scenes, it will automatically create a connection check **Catalog Explore → Connections**
# MAGIC
# MAGIC ![Pic](./Includes/create_Catalog_img.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · Query the foreign catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Once the foreign catalog exists, this just works:
# MAGIC SELECT count(*) FROM lakebase_foreign.public.stress_test;

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · What pushes down vs what doesn't
# MAGIC
# MAGIC | Operation | Pushes to Postgres | Runs in Spark |
# MAGIC |---|---|---|
# MAGIC | `WHERE` with simple predicates (=, <, BETWEEN, IN) | ✅ | |
# MAGIC | `LIMIT` | ✅ | |
# MAGIC | `ORDER BY` (when LIMIT present) | ✅ | |
# MAGIC | Simple aggregations (COUNT, SUM, MAX, MIN, AVG) | ✅ | |
# MAGIC | `GROUP BY` (low cardinality) | ✅ | |
# MAGIC | UDFs / window functions | ❌ | ✅ |
# MAGIC | `LIKE` with leading wildcard | Sometimes | ✅ |
# MAGIC | `JOIN` between two foreign tables | ❌ (today) | ✅ |
# MAGIC | `JOIN` between foreign + Delta | ❌ | ✅ |
# MAGIC | `STRING_AGG` / array funcs | Push | varies |
# MAGIC
# MAGIC ### How to verify
# MAGIC
# MAGIC ```sql
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT score, count(*) FROM lakebase_foreign.public.stress_test
# MAGIC WHERE created_at > current_timestamp() - INTERVAL 1 DAY
# MAGIC GROUP BY customer_id
# MAGIC HAVING count(*) > 10;
# MAGIC ```
# MAGIC
# MAGIC Look for **`PostgresScan`** with **`PushedFilters`** — that's the pushdown evidence. If you see `Filter` outside the scan, that predicate ran in Spark.

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT score, count(*) FROM lakebase_foreign.public.stress_test
# MAGIC WHERE created_at > current_timestamp() - INTERVAL 1 DAY
# MAGIC GROUP BY score
# MAGIC HAVING count(*) > 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## D · Hybrid join: Lakebase OLTP + Delta analytics
# MAGIC
# MAGIC The killer use case — join live OLTP rows in Lakebase with analytical Delta tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example pattern (uncomment after creating the foreign catalog):
# MAGIC --
# MAGIC -- WITH active_customers AS (
# MAGIC --     -- recent rows in OLTP (small set, pushdown scoped)
# MAGIC --     SELECT customer_id, max(ordered_at) AS last_seen
# MAGIC --     FROM lakebase_foreign.public.orders
# MAGIC --     WHERE ordered_at > current_timestamp() - INTERVAL 1 HOUR
# MAGIC --     GROUP BY customer_id
# MAGIC -- )
# MAGIC -- SELECT a.customer_id, a.last_seen, c.lifetime_value, c.churn_risk
# MAGIC -- FROM active_customers a
# MAGIC -- JOIN main.analytics.customer_lifetime_value c USING (customer_id)
# MAGIC -- ORDER BY c.churn_risk DESC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E · Federation vs Sync vs Direct connect — choosing
# MAGIC
# MAGIC | Use case | Federation | Sync (UC → Lakebase) | Direct connect (psycopg) |
# MAGIC |---|---|---|---|
# MAGIC | Analyst writes ad-hoc SQL against OLTP | ✅ | n/a | n/a |
# MAGIC | App needs sub-second OLTP reads | ❌ (Spark overhead) | ✅ | ✅ |
# MAGIC | Hybrid query joining OLTP + Delta | ✅ | n/a | n/a |
# MAGIC | Bulk ML feature load → Postgres | n/a | ✅ | OK for medium scale |
# MAGIC | Periodic snapshot of OLTP for analytics | Federation OK; Sync also valid | ✅ | n/a |
# MAGIC | Real-time customer 360 in app | n/a | n/a | ✅ |
# MAGIC
# MAGIC **Rule of thumb**: Federation = read-only analytics. Sync = bulk Delta → Postgres. Direct = live OLTP from apps.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** **06 Demo - Reverse ETL & Schema Sync**.