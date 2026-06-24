# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Lecture — Lakebase Architecture Deep Dive
# MAGIC
# MAGIC **Duration:** ~45 minutes · **Type:** Lecture (no code execution) · **Prerequisite:** 200-level Lakebase Concepts
# MAGIC
# MAGIC The 200-level course covered "Lakebase is managed Postgres on Databricks." This lecture goes one layer deeper: **how the storage actually works**, **why branching is cheap**, **what you give up vs vanilla Postgres**, and **what to tune**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · The "two-database problem" Lakebase exists to solve
# MAGIC
# MAGIC Most enterprise architectures look like this:
# MAGIC
# MAGIC ```
# MAGIC  ┌─────────────┐    ┌─────────────┐    ┌──────────────┐
# MAGIC  │ Application │───▶│ Postgres /  │───▶│  ETL daily   │───▶ Lakehouse (Delta)
# MAGIC  │             │    │  MySQL OLTP │    │  / streaming │     analytics + ML
# MAGIC  └─────────────┘    └─────────────┘    └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC The problems compound:
# MAGIC
# MAGIC | Pain | Symptom |
# MAGIC |---|---|
# MAGIC | **Two networks, two clouds** | Egress fees; cross-VPC peering; ops complexity |
# MAGIC | **Two governance planes** | UC governs the lake; *something else* governs the OLTP side |
# MAGIC | **Stale analytics** | Daily ETL means dashboards are 24h behind operational reality |
# MAGIC | **Reverse-ETL maze** | Models trained on Delta need to be served from OLTP — separate sync stack |
# MAGIC | **Two on-call rotas** | Postgres DBA + Spark engineer don't share tooling |
# MAGIC
# MAGIC **Lakebase collapses the OLTP side into Databricks** while keeping Postgres compatibility. Same Unity Catalog governance, same workspace, same identities, same observability stack.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · What Lakebase actually is
# MAGIC
# MAGIC Lakebase = **Postgres-protocol-compatible service** that:
# MAGIC
# MAGIC - Speaks the **PostgreSQL wire protocol** (any client that connects to Postgres connects to Lakebase)
# MAGIC - Runs a **modified Postgres engine** (the engine layer is Postgres-compatible; storage is *not* vanilla Postgres)
# MAGIC - Stores all data on **Databricks-managed object storage** (S3 / ADLS / GCS) under the hood
# MAGIC - Decouples **compute** from **storage** — instances scale independently of data volume
# MAGIC - Integrates with **Unity Catalog** for governance, lineage, and access control
# MAGIC - Is **fully managed** — no patching, no vacuuming, no failover playbooks to write yourself
# MAGIC
# MAGIC The compatibility wins matter: existing ORMs, BI tools, ETL connectors, and Postgres-shaped knowledge all transfer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · Architecture — the layered view
# MAGIC
# MAGIC ```
# MAGIC  ┌──────────────────────────────────────────────────────────────────┐
# MAGIC  │                    Databricks workspace                          │
# MAGIC  │                                                                  │
# MAGIC  │  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌──────────────┐  │
# MAGIC  │  │ Notebook  │  │ DB SQL    │  │ Apps      │  │ Federation    │  │
# MAGIC  │  │ (psycopg) │  │ Warehouse │  │           │  │ foreign cat.  │  │
# MAGIC  │  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘  └───────┬──────┘  │
# MAGIC  │        │              │              │                │         │
# MAGIC  │        ▼              ▼              ▼                ▼         │
# MAGIC  │     ┌─────────────────────────────────────────────────────┐    │
# MAGIC  │     │             Postgres wire protocol                   │    │
# MAGIC  │     └─────────────────────┬───────────────────────────────┘    │
# MAGIC  │                           │                                    │
# MAGIC  │  ┌────────────────────────▼────────────────────────────────┐   │
# MAGIC  │  │              Lakebase compute (autoscaling CUs)         │   │
# MAGIC  │  │              [Postgres-compatible engine]               │   │
# MAGIC  │  └────────────────────────┬────────────────────────────────┘   │
# MAGIC  │                           │                                    │
# MAGIC  │  ┌────────────────────────▼────────────────────────────────┐   │
# MAGIC  │  │   Page server  ←───  catalog/branch metadata service    │   │
# MAGIC  │  │   (translates pg_class pages ↔ object storage chunks)   │   │
# MAGIC  │  └────────────────────────┬────────────────────────────────┘   │
# MAGIC  │                           │                                    │
# MAGIC  │  ┌────────────────────────▼────────────────────────────────┐   │
# MAGIC  │  │       Object storage (S3 / ADLS / GCS)                  │   │
# MAGIC  │  │       Encrypted at rest with platform OR customer key   │ ◀─┼── CMK lives here
# MAGIC  │  └─────────────────────────────────────────────────────────┘   │
# MAGIC  └──────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Why this matters
# MAGIC
# MAGIC - **Compute can scale to 0** — no idle Postgres VM burning $
# MAGIC - **Branches are cheap** — they share storage via copy-on-write at the page-server layer (more in section F)
# MAGIC - **Encryption** is enforced at the object-storage boundary; CMK swaps the key used to encrypt those pages
# MAGIC - **Crash recovery** is just "spin compute back up and re-attach to storage" — no WAL replay, no fsck

# COMMAND ----------

# MAGIC %md
# MAGIC ## D · Compute units (CUs) and capacity
# MAGIC
# MAGIC Lakebase compute is measured in **CUs (Compute Units)**:
# MAGIC
# MAGIC | CU | vCPU | RAM (rough) | Concurrent connections | Use case |
# MAGIC |---|---|---|---|---|
# MAGIC | 0.25 | 0.25 | 1 GB | ~25 | Dev / test / quiet apps |
# MAGIC | 0.5 | 0.5 | 2 GB | ~50 | Light prod |
# MAGIC | 1 | 1 | 4 GB | ~100 | Mid-size app |
# MAGIC | 2 | 2 | 8 GB | ~200 | Production OLTP |
# MAGIC | 4 | 4 | 16 GB | ~400 | High-traffic |
# MAGIC | 8 | 8 | 32 GB | ~800 | Heavy aggregations + OLTP |
# MAGIC | 16 | 16 | 64 GB | ~1600 | Large-scale |
# MAGIC
# MAGIC Two modes of running CUs:
# MAGIC
# MAGIC - **Provisioned** — you pick a fixed CU size; you pay for it 24×7. Lower latency variability. Best for **predictable production** workloads.
# MAGIC - **Autoscaling** — Lakebase scales between a min and max CU. Min can be **0.25** (or even 0 in some configs — scale-to-zero between bursts). Best for **bursty / dev / batch-influenced** workloads.
# MAGIC
# MAGIC Module 02 covers the picking-which trade-off in depth.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E · The page server — the secret sauce
# MAGIC
# MAGIC Vanilla Postgres tightly couples its **buffer cache** (in-memory) and its **on-disk pages** (typically local SSD). This is what makes vanilla Postgres scaling hard: storage and compute share a fate.
# MAGIC
# MAGIC Lakebase splits these:
# MAGIC
# MAGIC ### Compute-side:
# MAGIC - The Postgres engine still talks to "pages" but the **page reader** is a Lakebase shim
# MAGIC - The buffer cache is local to the compute pod (volatile)
# MAGIC - When a page misses cache, the shim fetches it from the **page server**
# MAGIC
# MAGIC ### Page server:
# MAGIC - A separate distributed service that lives between Postgres and object storage
# MAGIC - Stores **page deltas** in object storage (similar to Delta Lake's transaction log philosophy)
# MAGIC - Materializes the latest version of any page on demand by replaying deltas from a snapshot
# MAGIC - Caches hot pages in its own SSD layer
# MAGIC
# MAGIC ### Object storage:
# MAGIC - The durable home of all data (S3 / ADLS / GCS)
# MAGIC - Encrypted at rest — **platform-managed key OR customer-managed key (CMK)**
# MAGIC - Survives compute restart, scale-to-zero, region failure (with appropriate replication config)
# MAGIC
# MAGIC ### Why this matters for ops
# MAGIC
# MAGIC | Property | Vanilla Postgres | Lakebase |
# MAGIC |---|---|---|
# MAGIC | Durability of writes | fsync to local disk + WAL replication | Immediate to object storage; durable when commit returns |
# MAGIC | Restore time | restore from base + replay WAL (hours-days) | Re-point compute at storage (seconds) |
# MAGIC | Branching | Logical replication or full restore | Copy-on-write at page level (instant, cheap) |
# MAGIC | Encryption rotation | Online but operationally heavy | Re-encrypt page server's KEK (fast) |
# MAGIC | Vacuum / autovacuum | DBA's job | Managed |

# COMMAND ----------

# MAGIC %md
# MAGIC ## F · Branching — copy-on-write semantics
# MAGIC
# MAGIC A **branch** in Lakebase is a logical fork of a database that **shares its storage** with the parent until you write.
# MAGIC
# MAGIC ```
# MAGIC main branch:    [page A]──[page B]──[page C]
# MAGIC                                       │
# MAGIC                                       │ create dev branch
# MAGIC                                       ▼
# MAGIC dev branch:                         [page C]    ← shares pages A, B, C with main
# MAGIC                                       │
# MAGIC                                       │ write to page B in dev
# MAGIC                                       ▼
# MAGIC dev branch:    [page A]──[page B']──[page C]    ← page B' is a new copy
# MAGIC main branch:   [page A]──[page B ]──[page C]    ← unchanged
# MAGIC ```
# MAGIC
# MAGIC ### What you can do with branches
# MAGIC
# MAGIC - **Run a destructive migration in `dev` without touching `main`**
# MAGIC - **Test a query against prod-shaped data without copying TBs**
# MAGIC - **Roll back instantly** — drop the branch
# MAGIC - **A/B test schema changes** — point one app to `main`, another to `dev`
# MAGIC - **Time-travel** — create a branch from a specific timestamp (PITR-style)
# MAGIC
# MAGIC ### What's preserved across a branch
# MAGIC
# MAGIC - Schema (DDL)
# MAGIC - Data (every row)
# MAGIC - Indexes (and statistics)
# MAGIC - Sequences
# MAGIC - Extensions enabled (e.g. pgvector, postgis if installed)
# MAGIC
# MAGIC ### What's NOT preserved
# MAGIC
# MAGIC - Active connections (each branch has its own)
# MAGIC - Live transactions (committed only — uncommitted at branch-time are dropped)
# MAGIC - Backup history (branches are short-lived, typically ≤30 days)
# MAGIC
# MAGIC Module 08 walks through the workflow end-to-end.

# COMMAND ----------

# MAGIC %md
# MAGIC ## G · Connectivity model
# MAGIC
# MAGIC Three ways to connect:
# MAGIC
# MAGIC | Method | Auth | When to use |
# MAGIC |---|---|---|
# MAGIC | **OAuth IAM token** | Workspace identity (PAT or service principal); via `w.database.generate_database_credential()` | Production — preferred. No long-lived secret. Token TTL 1 hour, must be refreshed. |
# MAGIC | **Postgres password** | Set on the role at creation time; stored in Databricks secrets ideally | Legacy clients that can't refresh OAuth tokens; emergency break-glass |
# MAGIC | **Lakehouse Federation** | UC catalog connection; uses workspace identity | Ad-hoc analytical queries from SQL Warehouses; cross-engine joins |
# MAGIC
# MAGIC Network layer:
# MAGIC
# MAGIC | Mode | Description |
# MAGIC |---|---|
# MAGIC | **Public endpoint** | Default; Lakebase has a public DNS hostname; access controlled by IP allowlist |
# MAGIC | **PrivateLink (AWS)** | VPC endpoint into Databricks; no public IP needed |
# MAGIC | **Private endpoint (Azure/GCP)** | Equivalent constructs on each cloud |
# MAGIC
# MAGIC Module 04 has the full connectivity walkthrough.

# COMMAND ----------

# MAGIC %md
# MAGIC ## H · Encryption layers (where CMK fits)
# MAGIC
# MAGIC Three levels of encryption operate on Lakebase data:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────┐
# MAGIC │  1. TLS in transit                                       │
# MAGIC │     ▶ Wire protocol always TLS 1.2+; Postgres clients   │
# MAGIC │       must use sslmode=require                           │
# MAGIC ├──────────────────────────────────────────────────────────┤
# MAGIC │  2. Buffer cache + page server SSD cache                 │
# MAGIC │     ▶ Encrypted at rest with platform key                │
# MAGIC │       (volatile — survives restart but not key rotation) │
# MAGIC ├──────────────────────────────────────────────────────────┤
# MAGIC │  3. Object storage at rest                               │ ◀── This is where CMK applies
# MAGIC │     ▶ Default: Databricks-managed key (DBKE)            │
# MAGIC │     ▶ Optional: Customer-Managed Key (CMK)              │
# MAGIC │       — your AWS KMS / Azure KV / GCP KMS key           │
# MAGIC │       — wraps the data encryption key (envelope encryption)│
# MAGIC │       — you control rotation, revocation, audit         │
# MAGIC └──────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Envelope encryption — how CMK actually protects data
# MAGIC
# MAGIC ```
# MAGIC  Object storage bucket
# MAGIC      │
# MAGIC      └─ holds encrypted data pages
# MAGIC          │
# MAGIC          ▼ each page encrypted with
# MAGIC  Data Encryption Key (DEK)
# MAGIC      │
# MAGIC      └─ wrapped (encrypted) by
# MAGIC          │
# MAGIC          ▼
# MAGIC  Key Encryption Key (KEK) ── this is YOUR KMS key
# MAGIC ```
# MAGIC
# MAGIC When you rotate the CMK:
# MAGIC - The DEK is re-wrapped (cheap, fast, no data re-encrypted)
# MAGIC - Existing data pages stay encrypted with their original DEK
# MAGIC - You can revoke access to the KMS key; all access stops immediately, even for already-decrypted pages on cache
# MAGIC
# MAGIC When you rotate the DEK (rare):
# MAGIC - Data pages get re-encrypted lazily as they're rewritten
# MAGIC - Or proactively via background re-encryption (not a typical operation)
# MAGIC
# MAGIC **Module 03 walks through the full setup, rotation, and recovery procedure.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## I · Comparison cheat-sheet — Lakebase vs alternatives
# MAGIC
# MAGIC Customers ask "why not just use X". Here's the honest matrix:
# MAGIC
# MAGIC | | **Lakebase** | RDS Postgres | Aurora Postgres | Cloud SQL Postgres | Self-hosted Postgres |
# MAGIC |---|---|---|---|---|---|
# MAGIC | Postgres compat | High | Highest | High | Highest | 100% |
# MAGIC | UC governance | ✅ native | ❌ | ❌ | ❌ | ❌ |
# MAGIC | Lakehouse data sharing | ✅ Federation + Sync | ETL only | ETL only | ETL only | ETL only |
# MAGIC | Compute autoscale | ✅ (incl. to 0) | Manual | ✅ (limited) | Manual | Manual |
# MAGIC | Storage decoupled | ✅ | ❌ | ✅ | ❌ | ❌ |
# MAGIC | Branches (copy-on-write) | ✅ instant | ❌ | Aurora clones (limited) | ❌ | Manual snapshots |
# MAGIC | CMK | ✅ (this lab) | ✅ | ✅ | ✅ | ✅ (you manage) |
# MAGIC | Single billing / IAM | ✅ Databricks | AWS | AWS | GCP | self |
# MAGIC | Postgres extensions | Curated set (incl pgvector) | All available | Most | Most | All |
# MAGIC | Bring-your-own DBA | Not needed | Optional | Optional | Optional | Required |
# MAGIC
# MAGIC ### When *not* to use Lakebase
# MAGIC
# MAGIC - You need an extension Lakebase doesn't ship (e.g. PostGIS at full feature parity, citus, timescaledb)
# MAGIC - You need >16 CU sustained workloads (talk to your account team — limits move)
# MAGIC - Your governance plane is fundamentally non-Databricks and won't change
# MAGIC - You need a specific Postgres major version that Lakebase isn't on yet

# COMMAND ----------

# MAGIC %md
# MAGIC ## J · What you'll touch in each upcoming module
# MAGIC
# MAGIC Now that you have the mental model, here's how it maps to the rest of the course:
# MAGIC
# MAGIC | Module | Architecture concept it exercises |
# MAGIC |---|---|
# MAGIC | 02 | Compute layer — provisioned vs autoscaling CUs |
# MAGIC | **03** | **Object storage encryption — wraps the page server's DEK with your CMK** |
# MAGIC | 04 | Connectivity layer — OAuth + PrivateLink + IP allowlist |
# MAGIC | 05 | Federation pathway from SQL Warehouse → Lakebase compute |
# MAGIC | 06 | The Sync pipeline that bridges Lakebase storage and Delta storage |
# MAGIC | 07 | pgvector — a Postgres extension running inside the Lakebase compute layer |
# MAGIC | 08 | Branching mechanics — page server's copy-on-write capabilities |
# MAGIC | 09 | Postgres engine internals — `pg_stat_*`, `EXPLAIN`, etc |
# MAGIC | 10 | Object storage durability + replication |
# MAGIC | 11 | Connectivity from a Databricks App service principal |
# MAGIC | 12 | All of the above stitched together |
# MAGIC
# MAGIC Proceed to **02 Demo - Provisioned vs Autoscaling**.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC <details>
# MAGIC <summary>📚 Suggested deeper reading (optional)</summary>
# MAGIC
# MAGIC - [Architecture of a Database System (Hellerstein, Stonebraker, Hamilton)](https://dsf.berkeley.edu/papers/fntdb07-architecture.pdf) — classic; sections 4–6 explain WAL, buffer management, query exec
# MAGIC - [Aurora paper](https://web.stanford.edu/class/cs245/win2020/readings/aurora.pdf) — different system, similar split-storage philosophy
# MAGIC - [Neon (the open-source Postgres-on-S3 model Lakebase resembles)](https://neon.tech/docs/introduction)
# MAGIC - [Databricks Lakebase docs — Architecture page](https://docs.databricks.com/lakebase/architecture.html)
# MAGIC
# MAGIC </details>
