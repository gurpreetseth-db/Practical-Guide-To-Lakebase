# Databricks Lakebase — Deep Dive (Level 300)

End-to-end hands-on lab covering every key capability of **Databricks Lakebase**, the fully-managed Postgres service inside the Databricks Data Intelligence Platform — with explicit deep-dive coverage of **Customer-Managed Keys (CMK)**.

| Field | Details |
|---|---|
| **Duration** | ~7–8 hours (split across 2–3 sessions recommended) |
| **Level** | 200/300 (Target difficulty level for participants (100 = beginner, 200 = intermediate, 300 = advanced)) |
| **Format** | Lecture notebooks + hands-on demos + capstone |
| **Lab Status** | Active — Module 12 (Capstone) in development |
| **Scope** | CMK deep-dive · pgvector for RAG · Agent memory (LangGraph) · Branching workflows · HA/Backup · Networking · Stateful end-to-end capstone |

---

## What you'll build

By the end of this lab you will have stood up — from scratch, in your workspace — a production-grade Lakebase deployment that:

1. Hosts a **Lakebase Autoscaling database** encrypted with your **own AWS KMS / Azure Key Vault key** (CMK)
2. Continuously syncs Delta tables from Unity Catalog into Postgres for sub-second OLTP reads (**Reverse ETL**)
3. Stores embeddings in **pgvector** for a Retrieval-Augmented Generation (RAG) chatbot
4. Holds **agent short-term memory** (LangGraph `PostgresSaver` checkpointer) and **long-term memory** (`DatabricksStore` over pgvector) — same database, same governance
5. Has a **dev branch** isolated from prod for safe schema migrations (copy-on-write, instant)
6. Is queried from a **Databricks App** with auto-rotated OAuth tokens
7. Is monitored via system tables for query latency, connection pool health, and cost
8. Has tested **point-in-time restore (PITR)** and **failover** procedures
9. **Capstone** *(coming soon)*: a stateful customer-support agent that uses *all* of the above in a single deployable LangGraph app

This is the production architecture you'd ship internally — not a toy demo.

---

## What this lab covers

Lakebase fundamentals (OLTP vs OLAP, creating a project, querying via Federation, syncing Delta tables, basic Python CRUD) are 200-level material. **This lab assumes all that knowledge** and goes deeper on the topics customers ask about during real production rollouts:

| Topic | This lab (300-level) |
|---|---|
| Architecture | Internal page-server, branching mechanics, copy-on-write at the storage layer |
| Provisioning | Provisioned (legacy) vs Autoscaling trade-off matrix; **note:** new instances are Autoscaling-only since March 2026 (`w.postgres` API); Provisioned instances auto-migrate starting June 2026 |
| **Customer-Managed Keys** | **Full module — KMS setup, rotation, scoped grants, recovery** |
| Connectivity | OAuth IAM tokens, refresh patterns, PrivateLink, IP allowlist |
| Federation | Pushdown analysis, query planning, hybrid OLTP+OLAP joins |
| Sync | Both directions, watermark management, schema evolution |
| **pgvector** | **Full module — RAG pattern with Mosaic AI Vector Search alternative analysis** |
| **Agent memory (LangGraph)** | **Module 11b — short-term checkpointer + long-term `DatabricksStore`** |
| **Branching** | **Full module — dev/test/prod patterns, schema migration via branches** |
| Observability | System tables, pg_stat_*, EXPLAIN, pgBadger-style analysis |
| HA / Backup | Replication, failover drill, PITR validation |
| Apps integration | Production patterns — connection pooling, secret-less IAM auth, blue-green |
| Capstone | **End-to-end *stateful* RAG agent — RAG + CMK + memory + ops data + Apps** |

---

## Prerequisites

### Required

- A **Databricks workspace** with the **Lakebase** feature enabled (most workspaces have it; verify under **Compute → Database instances**)
- **Workspace admin** OR **Account admin** role for the CMK module (needs cloud-key admin too)
- **Unity Catalog** enabled and configured
- A **SQL Warehouse** (2X-Small is sufficient) for Federation queries
- An **All-purpose** or **Serverless** notebook cluster on Databricks Runtime 15.4 LTS or higher
- **Cloud KMS access** for the CMK module:
  - **AWS**: ability to create a KMS key in the workspace's region, OR use an existing key your org has provisioned
  - **Azure**: ability to create a Key Vault key (or use existing), with the workspace's managed identity granted access
  - **GCP**: Cloud KMS key with Cloud KMS CryptoKey Encrypter/Decrypter role
- **Databricks CLI ≥ 0.250.0** installed locally (for bundle deployment steps)
- **Python 3.11+** with the following packages (each notebook installs what it needs via `%pip`):
  - Core: `databricks-sdk>=0.40`, `psycopg[binary]>=3.2`, `sqlalchemy>=2`
  - Module 07 (pgvector): additionally `pgvector>=0.3`, `mlflow>=2.16`
  - Module 11b (Agent memory): additionally `langgraph`, `langgraph-checkpoint-postgres`, `databricks-langchain`, `langchain-core`

### Recommended pre-reading

- Lakebase docs: [https://docs.databricks.com/lakebase/](https://docs.databricks.com/lakebase/)
- Postgres knowledge: indexes, EXPLAIN, MVCC, transaction isolation

---

## Lab structure

```
lakebase-lab-300/
├── README.md                                        ← this file
├── databricks.yml                                   ← Declarative Automation Bundle config
├── image_1779764439828.png                          ← architecture diagram
├── 00 Course Overview.py                            ← welcome, structure, goals
├── 01 Lecture - Architecture Deep Dive.py           ← internals, page server, branching
├── 02 Demo - Provisioned vs Autoscaling.py          ← legacy Provisioned + new Autoscaling API (w.postgres)
├── 03 Demo - Customer-Managed Keys (CMK).py         ← FULL CMK setup, rotation, recovery
├── 04 Demo - Connectivity & Security.py             ← OAuth, IAM tokens, PrivateLink, allowlist
├── 05 Demo - Lakehouse Federation Pushdown.py       ← query planning, hybrid joins
├── 06 Demo - Reverse ETL & Schema Sync.py           ← UC↔Lakebase, watermarks, evolution
├── 07 Demo - pgvector for AI Workloads.py           ← RAG on Lakebase with embeddings
├── 08 Demo - Branching for Dev-Test-Prod.py         ← isolated migrations, A/B, PITR-style branch
├── 09 Demo - Observability & Performance.py         ← pg_stat_*, EXPLAIN, system tables, cost
├── 10 Demo - HA, Backup & PITR.py                   ← replication, failover drill, recovery
├── 11 Demo - Databricks Apps Integration.py         ← production patterns, secret-less auth
├── 11b Demo - Agent Memory with LangGraph.py        ← PostgresSaver + DatabricksStore (NEW)
├── 12 Capstone - End-to-End RAG App on Lakebase.py  ← ⚠️ COMING SOON
└── Includes/
    ├── Setup.py                                     ← classroom setup helper
    ├── Cleanup.py                                   ← tear down all artifacts at lab end
    ├── lakebase_helpers.py                          ← reusable Python — connect, token refresh
    ├── __init__.py                                  ← makes Includes importable as a package
    └── Lakebase_External_Catalog.jpg                ← Federation architecture diagram
```

> **Note:** `12 Capstone` is referenced in course materials but not yet included in the current deployment. All modules 00–11b are available and self-contained.

Each notebook is a self-contained module. Notebooks 03 (CMK), 07 (pgvector), 08 (Branching), 11b (Agent Memory), and 12 (Capstone) are the **"new" 300-level content** not present in the 200-level reference. Notebooks 04–06 and 09–11 build on 200-level material with deeper production patterns.

**API note:** Two Lakebase SDK namespaces are used:
- `w.database` — legacy Provisioned API; still required for **Synced Tables** (Modules 06, 07)
- `w.postgres` — new Autoscaling API; used for project creation (Module 02), branching and PITR branches (Module 08)

---

## How to run this lab

### Option A — Databricks Workspace (recommended)

1. **Clone this repo into a Databricks Repo**:
   ```
   Workspace → Repos → Add Repo → enter the URL of this repo
   ```
2. **Open `00 Course Overview.py`** in the cloned Repo and follow inline instructions — it runs a preflight check to validate your environment
3. Each notebook contains a `%run ./Includes/Setup` cell that creates the catalog/schema/sample data needed for that specific module — you can run modules independently as long as you've completed modules 00 and 01 first

### Option B — Databricks CLI bundle deploy

If you want to deploy this lab as a **Declarative Automation Bundle** into a workspace at a fixed path:

```bash
cd lakebase-lab-300
databricks bundle deploy --target dev --profile <your-profile>
```

See `databricks.yml` for bundle configuration. Notebooks deploy to `/Workspace/Users/<you>/.bundle/lakebase-lab-300/dev/files/`.

### Option C — Local development

You can run modules 01 (lecture, all markdown) locally; modules 02+ require workspace access.

---

## Module roadmap — recommended sequence

**Session 1 (~3.5 hours)** — Foundation + Provisioning + CMK
- 00 Course Overview *(15 min)*
- 01 Architecture Deep Dive *(45 min — lecture)*
- 02 Provisioned vs Autoscaling *(30 min — includes new Autoscaling API)*
- 03 **Customer-Managed Keys** *(60 min — the centerpiece module)*
- 04 Connectivity & Security *(45 min)*

**Session 2 (~4.5 hours)** — Capabilities + Agent Memory
- 05 Federation Pushdown *(20 min)*
- 06 Reverse ETL *(30 min)*
- 07 pgvector for AI *(45 min)*
- 08 Branching for Dev-Test-Prod *(30 min)*
- 09 Observability & Performance *(30 min)*
- 10 HA, Backup & PITR *(30 min — mostly conceptual + one drill)*
- 11 Databricks Apps Integration *(30 min)*
- 11b **Agent Memory with LangGraph** *(45 min — short-term + long-term)*

**Session 3 (~45 min) — Capstone** *(coming soon)*
- 12 **Capstone** *(~45 min — stateful agent end-to-end)*

---

## Cost estimate

Running this lab end-to-end in a non-prod workspace costs roughly **$15–$30** depending on how long instances are kept up:

| Resource | Approximate cost |
|---|---|
| Lakebase Provisioned database (CU.2, ~3 hours — Synced Tables modules) | ~$8 |
| Lakebase Autoscaling database (CU.0.5–CU.2, ~6 hours, scales to zero when idle) | ~$6 |
| SQL Warehouse 2X-Small (~2 hours) | ~$3 |
| Compute notebook cluster (~6 hours) | ~$5–$10 |
| Cloud KMS key (1 month, $1 + per-call charges) | ~$1 |
| Total | **~$22** |

The **Cleanup notebook (`Includes/Cleanup.py`)** at the end tears down every billable resource. Run it as soon as you finish.

---

## Capabilities matrix — what each module teaches

| Capability | Module | Hands-on |
|---|---|---|
| Lakebase architecture (page server, copy-on-write, branching) | 01 | Read |
| Provisioned (legacy) vs Autoscaling decision | 02 | ✓ create both |
| Autoscaling project creation via `w.postgres` API | 02 | ✓ |
| **Customer-Managed Keys (CMK)** | **03** | **✓ create KMS key, attach, rotate, validate** |
| OAuth IAM token authentication | 04 | ✓ |
| Token refresh patterns for long-running apps | 04 | ✓ |
| PrivateLink networking | 04 | Read (cloud-specific) |
| Lakehouse Federation read | 05 | ✓ |
| Reverse ETL (UC → Lakebase) | 06 | ✓ |
| Schema evolution handling | 06 | ✓ |
| pgvector embeddings | 07 | ✓ |
| RAG pattern on Postgres | 07 | ✓ |
| Branching for isolated dev | 08 | ✓ |
| Schema migration via branches | 08 | ✓ |
| Performance EXPLAIN + pg_stat | 09 | ✓ |
| System tables for cost analysis | 09 | ✓ |
| HA replication concepts | 10 | Read |
| Failover drill | 10 | ✓ |
| Point-in-time recovery | 10 | ✓ |
| Databricks Apps integration | 11 | ✓ deploy app |
| **Agent short-term memory (`PostgresSaver`)** | **11b** | ✓ |
| **Agent long-term memory (`DatabricksStore` + pgvector)** | **11b** | ✓ |
| **`InjectedStore` + `save_memory`/`recall_memories` tools** | **11b** | ✓ |
| End-to-end **stateful** agent capstone | 12 | ⚠️ coming soon |

---

## Authoring notes

- **Notebook format**: All notebooks are written in Databricks `.py` source format (`# Databricks notebook source` header, `# COMMAND ----------` cell separators, `# MAGIC %md` for markdown). Databricks imports these natively. Convert to `.ipynb` via `jupytext --to ipynb *.py` if your team prefers Jupyter format.
- **No external resources required for module 01** (architecture lecture). Modules 02+ require a workspace.
- **CMK module (03) is destructive of state** — it requires re-creating instances. Plan to run it cleanly start-to-finish.
- **Capstone module (12) consumes ~30 min of compute** as it builds embeddings — scale your cluster up before starting. Module 12 is not yet in the current deployment.
- **Two Lakebase APIs coexist in this lab:**
  - `w.database` — legacy Provisioned API. Still required for **Synced Tables** (Modules 06, 07) as of this writing.
  - `w.postgres` — new Autoscaling API. Used for project creation/resize (Module 02), branching and PITR branches (Module 08).
- **Autoscaling is now the default:** Since March 12, 2026, new Lakebase instances are Autoscaling projects. Use `w.postgres.create_project()` for all new instances. Existing Provisioned instances auto-migrate starting June 2026.

---

## Troubleshooting (most common issues)

| Symptom | Fix |
|---|---|
| `Lakebase feature not enabled` when creating instance | Workspace admin enables under **Settings → Workspace settings → Lakebase**; if not present, file a Databricks support ticket |
| `permission denied` connecting to Postgres | OAuth token expired (default 1 hr) — regenerate via `w.database.generate_database_credential(...)` |
| `KMS access denied` in CMK module | Cloud-side IAM grant didn't propagate yet; wait 60 sec and retry, or check the workspace SP has been granted Encrypt/Decrypt on the key |
| Federation query returns empty | Foreign catalog stale — run `REFRESH FOREIGN CATALOG <name>` |
| `relation does not exist` after sync | Synced table created in `public.<table>` by default; specify schema in your query |
| pgvector extension not available | Lakebase has it pre-installed; if missing, `CREATE EXTENSION IF NOT EXISTS vector` in `postgres` database first |
| `'WorkspaceClient' object has no attribute 'postgres'` | SDK too old — run `%pip install --upgrade "databricks-sdk>=0.40"` then `%restart_python` |
| Branching cell fails with `No autoscaling projects found` | Module 08 requires an Autoscaling project from Module 02 — run Module 02 first |
| Synced Tables cell fails | Synced Tables requires a **Provisioned** (`w.database`) instance; the Autoscaling API does not yet support Synced Tables |

---

## Feedback & contributions

This lab is iterative. If a module breaks for you, add a note in the corresponding notebook's "Issues encountered" section at the bottom, then PR back. The CMK module especially benefits from real-world feedback as cloud KMS UX evolves.

---

## Acknowledgments

- Reference materials: [Databricks Lakebase docs](https://docs.databricks.com/lakebase/), [Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/instances/), [Postgres docs](https://www.postgresql.org/docs/), [pgvector](https://github.com/pgvector/pgvector), [LangGraph](https://langchain-ai.github.io/langgraph/), [`langgraph-checkpoint-postgres`](https://pypi.org/project/langgraph-checkpoint-postgres/), [`databricks-langchain`](https://pypi.org/project/databricks-langchain/)
