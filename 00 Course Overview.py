# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Deep Dive — Course Overview (Level 300)
# MAGIC
# MAGIC Welcome to the **300-level Lakebase lab**. This course assumes you're already familiar with Lakebase fundamentals (creating an instance, basic queries, Delta sync) and are ready to go deep on production patterns.
# MAGIC
# MAGIC ## Why this course exists
# MAGIC
# MAGIC In real customer rollouts, the questions that come up *after* "how do I create an instance?" are:
# MAGIC
# MAGIC 1. **"Can I bring my own encryption key?"** → Module 03 (CMK)
# MAGIC 2. **"How do I keep my access secrets out of code?"** → Module 04 (OAuth IAM tokens)
# MAGIC 3. **"How do I use Lakebase as the vector store for our RAG app?"** → Module 07 (pgvector)
# MAGIC 4. **"How do dev/staging/prod work without breaking each other?"** → Module 08 (Branching)
# MAGIC 5. **"What does HA + DR look like for OLTP at this scale?"** → Module 10 (HA/Backup/PITR)
# MAGIC 6. **"How does this all come together as a real app?"** → Module 12 (Capstone)
# MAGIC
# MAGIC The 200-level course covers the "what" — this course covers the **"how do I ship this to production"**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning objectives
# MAGIC
# MAGIC By the end of this course you will be able to:
# MAGIC
# MAGIC - Describe Lakebase's internal architecture (page server, copy-on-write storage, branch isolation)
# MAGIC - Choose between **Provisioned** and **Autoscaling** modes given a workload's characteristics
# MAGIC - **Provision a Lakebase instance encrypted with your own KMS key**, validate the encryption, rotate the key, and recover from a key-access incident
# MAGIC - Implement OAuth IAM token authentication with proper refresh handling for long-running connections
# MAGIC - Configure PrivateLink + IP allowlists for network-isolated Lakebase access
# MAGIC - Use Lakehouse Federation to query Lakebase from SQL Warehouses with predicate pushdown
# MAGIC - Set up bidirectional sync between Delta tables and Lakebase Postgres tables
# MAGIC - Use **pgvector** to store and query embeddings for a RAG application on Lakebase
# MAGIC - Run dev/staging workflows on **isolated branches** with copy-on-write storage
# MAGIC - Diagnose query performance using `pg_stat_*`, `EXPLAIN`, and Databricks system tables
# MAGIC - Validate HA, run a failover drill, and execute point-in-time restore (PITR)
# MAGIC - Deploy a production-grade Databricks App backed by Lakebase
# MAGIC - **Capstone**: Build an end-to-end RAG chatbot powered by Lakebase + pgvector + CMK + a Databricks App

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module map
# MAGIC
# MAGIC | # | Module | Type | Duration | New vs 200-level |
# MAGIC |---|---|---|---|---|
# MAGIC | 00 | Course Overview *(this notebook)* | Lecture | 15 min | Setup |
# MAGIC | 01 | Architecture Deep Dive | Lecture | 45 min | Deeper |
# MAGIC | 02 | Provisioned vs Autoscaling | Demo | 30 min | New |
# MAGIC | **03** | **Customer-Managed Keys (CMK)** | **Demo** | **60 min** | **NEW** |
# MAGIC | 04 | Connectivity & Security | Demo | 45 min | Deeper |
# MAGIC | 05 | Lakehouse Federation Pushdown | Demo | 20 min | Deeper |
# MAGIC | 06 | Reverse ETL & Schema Sync | Demo | 30 min | Deeper |
# MAGIC | **07** | **pgvector for AI Workloads** | **Demo** | **45 min** | **NEW** |
# MAGIC | **08** | **Branching for Dev-Test-Prod** | **Demo** | **30 min** | **NEW** |
# MAGIC | 09 | Observability & Performance | Demo | 30 min | New |
# MAGIC | 10 | HA, Backup & PITR | Demo | 30 min | New |
# MAGIC | 11 | Databricks Apps Integration | Demo | 30 min | Deeper |
# MAGIC | **12** | **Capstone — End-to-End RAG App** | **Demo** | **45 min** | **NEW** |
# MAGIC
# MAGIC **Bold modules are the headline 300-level additions** — invest extra time here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run a setup check
# MAGIC
# MAGIC The cell below verifies your workspace has everything needed. If any check fails, fix it before starting Module 01.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2" "sqlalchemy>=2"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

#from Includes.Setup import preflight_check
preflight_check()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Course conventions
# MAGIC
# MAGIC - **Sample dataset** — we use a synthetic e-commerce dataset (`orders`, `customers`, `products`) seeded by `Includes/Setup.py`. Keeps every module self-contained.
# MAGIC - **Naming** — all artifacts are prefixed with your username so multi-user labs don't collide. The setup module computes `LAB_PREFIX` for you.
# MAGIC - **Cleanup** — at the end of each session, run `Includes/Cleanup.py` to tear down billable resources. The CMK module specifically needs a clean tear-down to avoid orphaned encrypted volumes.
# MAGIC - **Cells marked 🧪 EXERCISE** — your turn to write code. The next cell shows the expected output.
# MAGIC - **Cells marked 💡 INSIGHT** — important production gotchas worth pausing on.
# MAGIC - **Cells marked ⚠️ WARNING** — destructive or expensive operations; confirm before running.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ready?
# MAGIC
# MAGIC When the preflight check above shows ✅ for every line, proceed to **`01 Lecture - Architecture Deep Dive`**.
# MAGIC
# MAGIC If something is failing, see the Troubleshooting section in [README.md](./README.md).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC <details>
# MAGIC <summary>📖 Reference material consulted while building this lab</summary>
# MAGIC
# MAGIC - [Databricks Lakebase docs (overview)](https://docs.databricks.com/lakebase/index.html)
# MAGIC - [Lakebase architecture](https://docs.databricks.com/lakebase/architecture.html)
# MAGIC - [Customer-managed keys for Lakebase](https://docs.databricks.com/security/customer-managed-keys/index.html)
# MAGIC - [pgvector](https://github.com/pgvector/pgvector)
# MAGIC - [Postgres docs](https://www.postgresql.org/docs/)
# MAGIC - [`databricks-sdk` Lakebase API](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/database/database.html)
# MAGIC
# MAGIC </details>
