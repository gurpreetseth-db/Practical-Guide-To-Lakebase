# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Demo — pgvector for AI Workloads
# MAGIC
# MAGIC **Duration:** ~45 min · **Type:** Hands-on · **Prerequisite:** Modules 01-04
# MAGIC
# MAGIC Lakebase ships with the **`pgvector`** extension, turning your Postgres into a vector database for embeddings. This module walks you through building a Retrieval-Augmented Generation (RAG) pattern entirely on Lakebase, and contrasts it with using **Databricks Mosaic AI Vector Search** for the same use case.
# MAGIC
# MAGIC ### When to use pgvector vs Mosaic AI Vector Search
# MAGIC
# MAGIC | Aspect | pgvector on Lakebase | Mosaic AI Vector Search |
# MAGIC |---|---|---|
# MAGIC | Embedding storage | Same DB as your OLTP data | Separate managed index |
# MAGIC | Joins with relational data | ✅ native SQL JOIN | Federation needed |
# MAGIC | Operational simplicity | One DB to back up & secure | Managed index simpler in some ways |
# MAGIC | Performance at huge scale (10M+ vectors) | Good with HNSW; Vector Search may be faster | Optimized for billions |
# MAGIC | Multi-tenancy | Just rows in a table; tenant_id filter | Index per tenant or filter at query |
# MAGIC | Integration with `%sql` | ✅ direct SQL | Via UI or SDK |
# MAGIC | RAG with structured filters (e.g. user-level access) | ✅ trivial in WHERE clause | Possible, more friction |
# MAGIC
# MAGIC **Rule of thumb**: if your RAG app needs to combine vector similarity with structured filters and your scale is ≤10M docs per tenant, **pgvector is simpler**. For billion-scale or pure-vector workloads, **Mosaic AI Vector Search** wins.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2" "sqlalchemy>=2" "pgvector>=0.3" "mlflow>=2.16"
# MAGIC %pip install --upgrade "databricks-sdk>=0.40"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · Setup — get a Lakebase instance ready

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import (
    SyncedDatabaseTable, SyncedTableSpec, SyncedTableSchedulingPolicy, DatabaseInstance
)
from Includes.lakebase_helpers import wait_for_instance

w = WorkspaceClient()

# Synced Tables requires a legacy provisioned instance (not an autoscaling project).
# Create one if it doesn't already exist.
sync_instance_name = f"{LAB_PREFIX}-pgvector".replace("_", "-")
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
# MAGIC ## B · Enable the vector extension
# MAGIC
# MAGIC `pgvector` is pre-installed on Lakebase but must be explicitly enabled per database.

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
    cn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
    ext = cn.execute(text(
        "SELECT extname, extversion FROM pg_extension WHERE extname='vector'"
    )).first()
    print(f"  ✅ pgvector enabled: version {ext.extversion}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · Schema design for a RAG corpus
# MAGIC
# MAGIC Two tables:
# MAGIC
# MAGIC - **`documents`** — source content (with metadata for filtering: tenant, source, dates, ACL)
# MAGIC - **`chunks`** — chunked text + embedding vectors

# COMMAND ----------

EMBED_DIM = 1024  # databricks-bge-large-en

with engine.begin() as cn:
    cn.execute(text("DROP TABLE IF EXISTS chunks CASCADE"))
    cn.execute(text("DROP TABLE IF EXISTS documents CASCADE"))
    cn.execute(text("""
        CREATE TABLE documents (
            doc_id        BIGSERIAL PRIMARY KEY,
            tenant_id     TEXT NOT NULL,
            source_uri    TEXT NOT NULL,
            title         TEXT,
            ingested_at   TIMESTAMPTZ DEFAULT now(),
            metadata      JSONB DEFAULT '{}'::jsonb
        )
    """))
    cn.execute(text(f"""
        CREATE TABLE chunks (
            chunk_id      BIGSERIAL PRIMARY KEY,
            doc_id        BIGINT NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
            chunk_idx     INT NOT NULL,
            text_content  TEXT NOT NULL,
            embedding     VECTOR({EMBED_DIM}),
            UNIQUE(doc_id, chunk_idx)
        )
    """))
    cn.execute(text("CREATE INDEX idx_documents_tenant ON documents (tenant_id)"))
    print("  ✅ schema created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D · Ingest some sample documents
# MAGIC
# MAGIC In a real app, source documents come from S3 / SharePoint / a Delta table via Reverse-ETL. Here we use a small inline corpus for demo speed.

# COMMAND ----------

DOCS = [
    ("Lakebase architecture", "Databricks Lakebase is a managed PostgreSQL service that "
     "decouples compute from storage. Storage lives in object storage; compute scales "
     "independently via CUs. Branches share storage via copy-on-write."),
    ("Customer-managed keys",  "Lakebase supports customer-managed keys via cloud KMS. "
     "Setup the KMS key, grant the workspace identity Encrypt/Decrypt, register at the "
     "account level, attach to the workspace. Validation is done via KMS audit logs."),
    ("pgvector usage",         "pgvector enables vector similarity search inside Postgres. "
     "Three index types: IVFFlat, HNSW, and exact. HNSW gives best recall/latency for "
     "production RAG workloads at moderate scale."),
    ("Connection pooling",     "Lakebase uses a built-in connection pooler. Each compute "
     "unit can handle ~100 concurrent connections. For higher fan-out, use PgBouncer "
     "in transaction mode in front of the application."),
    ("Branching for dev/test", "Lakebase branches are copy-on-write forks of a database. "
     "They share storage with the parent until you write. Branches are perfect for "
     "running destructive migrations in isolation, A/B testing schemas, or PITR-style "
     "data exploration."),
    ("Reverse ETL",            "Reverse ETL syncs Delta tables in Unity Catalog to "
     "Lakebase Postgres tables. Useful for serving ML model features, customer 360 "
     "tables, or pre-aggregated analytics into operational apps."),
    ("Federation queries",     "Lakehouse Federation allows querying Lakebase from a "
     "Databricks SQL Warehouse via foreign catalogs. Predicates push down to Postgres "
     "where possible. Use for ad-hoc analytics over OLTP data."),
]

with engine.begin() as cn:
    for title, body in DOCS:
        cn.execute(
            text("INSERT INTO documents (tenant_id, source_uri, title) "
                  "VALUES (:t, :u, :title)"),
            dict(t="acme", u=f"docs/{title.lower().replace(' ', '-')}.md",
                  title=title),
        )
    n = cn.execute(text("SELECT count(*) FROM documents")).scalar()
    print(f"  ✅ ingested {n} documents")

# COMMAND ----------

# MAGIC %md
# MAGIC ## E · Generate embeddings via a Databricks Foundation Model endpoint
# MAGIC
# MAGIC We use Databricks' built-in **`databricks-bge-large-en`** embedding model. Stays in your workspace; no external API cost.

# COMMAND ----------

import time

def embed_batch(texts: list[str]) -> list[list[float]]:
    """Embed a list of strings via the Databricks Foundation Model endpoint."""
    response = w.serving_endpoints.query(
        name="databricks-bge-large-en",
        input=texts,
    )
    return [item.embedding for item in response.data]


# Chunk + embed each doc. For demo speed we use the whole document as one chunk;
# in production, use a chunking library (langchain RecursiveCharacterTextSplitter,
# llama-index sentence splitter, etc.) with ~500 token chunks.
print("Generating embeddings...")
t0 = time.time()

with engine.begin() as cn:
    docs_to_embed = cn.execute(text(
        "SELECT d.doc_id, d.title || ' — ' || coalesce(d.metadata->>'body', '') AS body "
        "FROM documents d ORDER BY d.doc_id"
    )).all()

# For our seed data, fetch the doc body from our DOCS dict (this would normally come from doc storage)
title_to_body = {t: b for (t, b) in DOCS}

embeddings = embed_batch([f"{r.body}{title_to_body.get(r.body.split(' — ')[0], '')}"
                           for r in docs_to_embed])

with engine.begin() as cn:
    for (doc_id, _), emb in zip(docs_to_embed, embeddings):
        body = title_to_body.get(
            cn.execute(text("SELECT title FROM documents WHERE doc_id=:i"),
                        dict(i=doc_id)).scalar(),
            "",
        )
        cn.execute(
            text("INSERT INTO chunks (doc_id, chunk_idx, text_content, embedding) "
                  "VALUES (:d, :i, :t, :e)"),
            dict(d=doc_id, i=0, t=body, e=str(emb)),
        )

print(f"  ✅ embedded {len(embeddings)} chunks in {time.time()-t0:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## F · Build an HNSW index for fast approximate-nearest-neighbour search

# COMMAND ----------

# DBTITLE 1,Cell 15
with engine.begin() as cn:
    # HNSW is the sweet spot for production RAG. Parameters tuned for moderate scale.
    cn.execute(text("""
        CREATE INDEX IF NOT EXISTS chunks_hnsw_cosine_idx ON chunks
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
    """))

# VACUUM cannot run inside a transaction block — use AUTOCOMMIT isolation level
with engine.execution_options(isolation_level="AUTOCOMMIT").connect() as cn:
    cn.execute(text("VACUUM ANALYZE chunks"))
    print("  ✅ HNSW index built; statistics updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## G · Run a similarity search

# COMMAND ----------

QUERY = "How do I rotate the encryption key for my Lakebase database?"

q_emb = embed_batch([QUERY])[0]

with engine.begin() as cn:
    rows = cn.execute(
        text("""
        SELECT
            c.text_content,
            d.title,
            1 - (c.embedding <=> CAST(:q AS VECTOR)) AS similarity
        FROM chunks c
        JOIN documents d USING (doc_id)
        WHERE d.tenant_id = :tenant
        ORDER BY c.embedding <=> CAST(:q AS VECTOR)
        LIMIT 3
        """),
        dict(q=str(q_emb), tenant="acme"),
    ).all()

print(f"Query: {QUERY}\n")
for r in rows:
    print(f"  [{r.similarity:.3f}] {r.title}")
    print(f"    {r.text_content[:140]}...\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## H · End-to-end RAG: retrieve + generate
# MAGIC
# MAGIC Plug the retrieved chunks into a Databricks-served LLM for the answer.

# COMMAND ----------

# DBTITLE 1,Cell 19
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"  # or claude-3-7-sonnet etc.

def rag_answer(question: str, top_k: int = 3) -> str:
    q_emb = embed_batch([question])[0]
    with engine.begin() as cn:
        retrieved = cn.execute(
            text("""
            SELECT c.text_content, d.title
            FROM chunks c JOIN documents d USING (doc_id)
            WHERE d.tenant_id = :tenant
            ORDER BY c.embedding <=> CAST(:q AS VECTOR)
            LIMIT :k
            """),
            dict(q=str(q_emb), tenant="acme", k=top_k),
        ).all()

    context = "\n\n".join(
        f"[{r.title}]\n{r.text_content}" for r in retrieved
    )
    prompt = (f"You are a Databricks expert. Use ONLY the provided context to answer.\n\n"
              f"Context:\n{context}\n\nQuestion: {question}\n\nAnswer:")

    from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
    response = w.serving_endpoints.query(
        name=LLM_ENDPOINT,
        messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
        temperature=0.3,
        max_tokens=400,
    )
    return response.choices[0].message.content


answer = rag_answer("How do I rotate the encryption key for Lakebase?")
print("=" * 60)
print(answer)
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## I · Multi-tenancy + governance via Postgres
# MAGIC
# MAGIC One of pgvector's biggest wins is doing **secure multi-tenant RAG with row-level filtering** in pure SQL.

# COMMAND ----------

# Add a second tenant's docs
with engine.begin() as cn:
    cn.execute(text(
        "INSERT INTO documents (tenant_id, source_uri, title) VALUES "
        "('acme-eu', 'docs/eu-only-lakebase-cmk.md', 'EU Region CMK Notes')"
    ))
    eu_doc_id = cn.execute(text(
        "SELECT doc_id FROM documents WHERE tenant_id='acme-eu' LIMIT 1"
    )).scalar()
    body = ("EU customers can use AWS KMS keys created in eu-west-1 for "
            "Lakebase data residency. Cross-region replicas are not allowed "
            "for GDPR-compliant workloads.")
    emb = embed_batch([body])[0]
    cn.execute(
        text("INSERT INTO chunks (doc_id, chunk_idx, text_content, embedding) "
              "VALUES (:d, 0, :t, :e)"),
        dict(d=eu_doc_id, t=body, e=str(emb)),
    )

# Try the same query as 'acme' (should not see EU result) vs 'acme-eu':
q_emb = embed_batch(["EU compliance for KMS keys"])[0]
for tenant in ("acme", "acme-eu"):
    with engine.begin() as cn:
        rows = cn.execute(
            text("""
            SELECT d.title, 1 - (c.embedding <=> CAST(:q AS VECTOR)) AS sim
            FROM chunks c JOIN documents d USING (doc_id)
            WHERE d.tenant_id = :t
            ORDER BY c.embedding <=> CAST(:q AS VECTOR)
            LIMIT 2
            """),
            dict(q=str(q_emb), t=tenant),
        ).all()
    print(f"Tenant '{tenant}':")
    for r in rows:
        print(f"  [{r.sim:.3f}] {r.title}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## J · Hybrid search — vectors + full-text + structured filters
# MAGIC
# MAGIC Postgres lets you combine `pgvector` similarity, traditional `tsvector` full-text search, and `JSONB` metadata filters in one query — something pure vector DBs typically can't do.

# COMMAND ----------

with engine.begin() as cn:
    cn.execute(text(
        "CREATE INDEX IF NOT EXISTS chunks_fts_idx ON chunks "
        "USING gin (to_tsvector('english', text_content))"
    ))

    q_emb = embed_batch(["how do I rotate keys"])[0]
    rows = cn.execute(
        text("""
        SELECT
          d.title,
          1 - (c.embedding <=> CAST(:q AS VECTOR)) AS vector_sim,
          ts_rank(to_tsvector('english', c.text_content),
                  plainto_tsquery('english', :ftq)) AS text_rank,
          (1 - (c.embedding <=> CAST(:q AS VECTOR))) * 0.7
            + ts_rank(to_tsvector('english', c.text_content),
                      plainto_tsquery('english', :ftq)) * 0.3
            AS combined_score
        FROM chunks c JOIN documents d USING (doc_id)
        WHERE d.tenant_id = :tenant
        ORDER BY combined_score DESC
        LIMIT 5
        """),
        dict(q=str(q_emb), ftq="rotate key encryption", tenant="acme"),
    ).all()

print("Hybrid (vector 70% + full-text 30%):")
for r in rows:
    print(f"  combined={r.combined_score:.3f} vec={r.vector_sim:.3f} text={r.text_rank:.3f}  {r.title}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## K · Production checklist for pgvector on Lakebase
# MAGIC
# MAGIC - [ ] Use **HNSW** indexes; tune `m` (16-32) and `ef_construction` (64-200) per scale
# MAGIC - [ ] Set `hnsw.ef_search` per-query to trade recall vs latency
# MAGIC - [ ] **Re-VACUUM ANALYZE** after bulk loads; HNSW maintains itself but stats matter
# MAGIC - [ ] Embedding column is the single biggest size driver — for 10M chunks × 1024-dim float32, that's ~40 GB. Plan storage.
# MAGIC - [ ] Use `quantization` (e.g. `halfvec` for fp16) if dataset is huge — 50% storage savings, minor recall drop
# MAGIC - [ ] Always filter by tenant/ACL FIRST in a CTE, then do vector search (Postgres planner doesn't always pick the right index order)
# MAGIC - [ ] **Monitor query latency** via `pg_stat_statements` (module 09)

# COMMAND ----------

# MAGIC %md
# MAGIC ## L · Cleanup
# MAGIC
# MAGIC Keep the instance — module 12 (Capstone) reuses it. To free billing, run `Includes/Cleanup` at session end.

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
# MAGIC **Next:** **08 Demo - Branching for Dev-Test-Prod**.