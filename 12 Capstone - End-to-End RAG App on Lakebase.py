# Databricks notebook source
# MAGIC %md
# MAGIC # 12 · Capstone — End-to-End **Stateful** RAG Agent on Lakebase
# MAGIC
# MAGIC **Duration:** ~60 min · **Type:** Hands-on capstone · **Prerequisite:** ALL prior modules (especially 03, 07, 11, 11b)
# MAGIC
# MAGIC This capstone synthesizes everything into a **single deployable artifact**: a production-shaped customer-support agent that:
# MAGIC
# MAGIC - 🔐 Runs on **Lakebase encrypted with your CMK** (Module 03)
# MAGIC - 🧠 Uses **`pgvector` for RAG** over a knowledge base (Module 07)
# MAGIC - 💾 Uses **`PostgresSaver` for short-term memory** — recalls the last few turns (Module 11b)
# MAGIC - 📚 Uses **`DatabricksStore` for long-term memory** — remembers each customer's preferences across sessions (Module 11b)
# MAGIC - 🔄 Reads **synced operational tables** (customers, orders) from Delta (Module 06)
# MAGIC - 🔑 Authenticates via **OAuth IAM tokens** with auto-refresh (Module 04)
# MAGIC - 📡 Deploys as a **Databricks App** wired to Lakebase as a resource (Module 11)
# MAGIC
# MAGIC The result is the production reference architecture for an internal AI agent backed by an OLTP database + vector store, all in one place.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC                    ┌────────────────────────────────────────────────┐
# MAGIC                    │             Databricks workspace                │
# MAGIC                    │                                                │
# MAGIC  Customer ─HTTPS──▶│  ┌────────────────────────────────────────┐  │
# MAGIC  (browser)         │  │   Databricks App (FastAPI / Shiny)     │  │
# MAGIC                    │  │   - SP auth + auto-refresh OAuth       │  │
# MAGIC                    │  │   - Async LangGraph agent              │  │
# MAGIC                    │  │     • search_kb()         (RAG)        │  │
# MAGIC                    │  │     • lookup_customer()   (operational)│  │
# MAGIC                    │  │     • save_memory()       (LTM write)  │  │
# MAGIC                    │  │     • recall_memories()   (LTM read)   │  │
# MAGIC                    │  └─────┬────────────────┬─────────────────┘  │
# MAGIC                    │        │                │                    │
# MAGIC                    │        │ tool calls     │ checkpoint/state   │
# MAGIC                    │        ▼                ▼                    │
# MAGIC                    │  ┌──────────────────────────────────────┐   │
# MAGIC                    │  │       Lakebase (CMK-encrypted)        │   │
# MAGIC                    │  │                                       │   │
# MAGIC                    │  │  Knowledge Base       Memory          │   │
# MAGIC                    │  │  ┌──────────┐         ┌──────────┐   │   │
# MAGIC                    │  │  │chunks    │         │checkpoints│   │   │  ◀── short-term
# MAGIC                    │  │  │+pgvector │         │           │   │   │
# MAGIC                    │  │  └──────────┘         └──────────┘   │   │
# MAGIC                    │  │                       ┌──────────┐   │   │
# MAGIC                    │  │  Operational          │store     │   │   │  ◀── long-term
# MAGIC                    │  │  ┌──────────┐         │+vectors  │   │   │
# MAGIC                    │  │  │customers │ ◀sync   └──────────┘   │   │
# MAGIC                    │  │  │orders    │                        │   │
# MAGIC                    │  │  └──────────┘                        │   │
# MAGIC                    │  └──────────────────────────────────────┘   │
# MAGIC                    │                                              │
# MAGIC                    │  ┌──────────────────────────────────────┐   │
# MAGIC                    │  │  Mosaic AI Foundation Models         │   │
# MAGIC                    │  │  - databricks-bge-large-en           │   │
# MAGIC                    │  │  - databricks-meta-llama-3-1-405b    │   │
# MAGIC                    │  └──────────────────────────────────────┘   │
# MAGIC                    └────────────────────────────────────────────────┘
# MAGIC                                          │
# MAGIC                              ┌───────────▼────────────┐
# MAGIC                              │   AWS KMS / Azure KV   │  ◀── your CMK
# MAGIC                              └────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Every tier of the architecture is something you've stood up by hand in a previous module.** This capstone is the final assembly.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2" "sqlalchemy>=2" "pgvector>=0.3" \
# MAGIC                  langgraph langgraph-checkpoint-postgres databricks-langchain langchain-core "mlflow>=2.16"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — One Lakebase instance, four roles

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance
from Includes.lakebase_helpers import wait_for_instance, get_oauth_engine, dsn_for_instance
from sqlalchemy import text
import re, json

w = WorkspaceClient()
INSTANCE = f"{LAB_PREFIX}_capstone"

try:
    info = w.database.get_database_instance(name=INSTANCE)
    print(f"Re-using {INSTANCE}")
except Exception:
    w.database.create_database_instance(DatabaseInstance(name=INSTANCE, capacity="CU_2"))
    wait_for_instance(INSTANCE)
    print(f"  ✅ created {INSTANCE}")

engine = get_oauth_engine(INSTANCE)

# Conn string for psycopg / langgraph
PG_URL = dsn_for_instance(INSTANCE)  # postgresql://... ?sslmode=require

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Knowledge base (pgvector)

# COMMAND ----------

EMBED_DIM = 1024

with engine.begin() as cn:
    cn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
    cn.execute(text("DROP TABLE IF EXISTS chunks CASCADE"))
    cn.execute(text("DROP TABLE IF EXISTS documents CASCADE"))
    cn.execute(text("""
        CREATE TABLE documents (
            doc_id BIGSERIAL PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'support',
            title TEXT
        )
    """))
    cn.execute(text(f"""
        CREATE TABLE chunks (
            chunk_id BIGSERIAL PRIMARY KEY,
            doc_id BIGINT REFERENCES documents(doc_id) ON DELETE CASCADE,
            text_content TEXT,
            embedding VECTOR({EMBED_DIM})
        )
    """))

KB_DOCS = [
    ("Refund policy",           "Refunds within 30 days of purchase: full refund. After 30 days: support discretion. Look up the order_total field for amount."),
    ("Shipping delays",         "If status remains PLACED >5 days, escalate to fulfilment. Standard shipping is 3-5 business days; express is 1-2."),
    ("Account email update",    "Customers update email by verifying identity in a support ticket, then operations updates customers.email."),
    ("Cancelling an order",     "PLACED orders can be cancelled. Once SHIPPED, customer must initiate a return per refund policy."),
    ("Bulk discounts",          "Orders of 10+ identical units qualify for 15% discount. Apply to order_total at checkout. Retroactive requests denied."),
    ("VIP escalation",          "Customers with lifetime spend > AUD 10,000 are VIP — escalate any complaint same-day."),
    ("Data residency",          "EU customers' data stays in Frankfurt region. AU/NZ customers' data stays in Sydney region. No cross-region transfer."),
]

def embed(texts):
    return [item.embedding for item in
            w.serving_endpoints.query(name="databricks-bge-large-en", input=texts).data]

with engine.begin() as cn:
    for title, body in KB_DOCS:
        doc_id = cn.execute(text("""
            INSERT INTO documents (title) VALUES (:t) RETURNING doc_id
        """), dict(t=title)).scalar()
        emb = embed([f"{title}\n{body}"])[0]
        cn.execute(text("""
            INSERT INTO chunks (doc_id, text_content, embedding) VALUES (:d, :c, :e)
        """), dict(d=doc_id, c=body, e=str(emb)))

with engine.begin() as cn:
    cn.execute(text("CREATE INDEX chunks_hnsw ON chunks USING hnsw (embedding vector_cosine_ops)"))
    cn.execute(text("VACUUM ANALYZE chunks"))

print(f"  ✅ KB: {len(KB_DOCS)} documents embedded + indexed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Operational tables (synced/loaded from Delta)

# COMMAND ----------

# Bulk-load customers + orders from the seed Delta tables. In production, this
# would be a continuously-synced table (Module 06).
import psycopg, io

raw_dsn = PG_URL  # already the libpq form

with engine.begin() as cn:
    cn.execute(text("DROP TABLE IF EXISTS customers"))
    cn.execute(text("DROP TABLE IF EXISTS orders"))
    cn.execute(text("""
        CREATE TABLE customers (
            customer_id BIGINT PRIMARY KEY,
            email TEXT, full_name TEXT, country TEXT, created_at TIMESTAMPTZ,
            lifetime_spend NUMERIC(14,2) DEFAULT 0
        )
    """))
    cn.execute(text("""
        CREATE TABLE orders (
            order_id BIGINT PRIMARY KEY,
            customer_id BIGINT, product_id BIGINT, quantity INT,
            order_total NUMERIC(12,2), ordered_at TIMESTAMPTZ, status TEXT
        )
    """))

customers_pdf = spark.table(f"{LAB_CATALOG}.{LAB_SCHEMA}.customers").limit(500).toPandas()
orders_pdf    = spark.table(f"{LAB_CATALOG}.{LAB_SCHEMA}.orders").limit(2000).toPandas()

with psycopg.connect(raw_dsn) as cn:
    with cn.cursor() as cur:
        with cur.copy("COPY customers (customer_id, email, full_name, country, created_at) FROM STDIN") as cp:
            for _, r in customers_pdf.iterrows():
                cp.write_row((int(r.customer_id), r.email, r.full_name, r.country, r.created_at))
        with cur.copy("COPY orders (order_id, customer_id, product_id, quantity, order_total, ordered_at, status) FROM STDIN") as cp:
            for _, r in orders_pdf.iterrows():
                cp.write_row((int(r.order_id), int(r.customer_id), int(r.product_id),
                              int(r.quantity), float(r.order_total), r.ordered_at, r.status))
    cn.commit()

# Compute lifetime_spend
with engine.begin() as cn:
    cn.execute(text("""
        UPDATE customers c
        SET lifetime_spend = COALESCE((
            SELECT sum(order_total) FROM orders o WHERE o.customer_id = c.customer_id
        ), 0)
    """))
    n_c = cn.execute(text("SELECT count(*) FROM customers")).scalar()
    n_o = cn.execute(text("SELECT count(*) FROM orders")).scalar()
    print(f"  ✅ Operational data: {n_c} customers, {n_o} orders, lifetime_spend computed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Memory subsystems (short-term + long-term)

# COMMAND ----------

from langgraph.checkpoint.postgres import PostgresSaver
from databricks_langchain import DatabricksStore

# Set up memory tables (idempotent)
with PostgresSaver.from_conn_string(PG_URL) as cps:
    cps.setup()

store = DatabricksStore(conn=PG_URL, embedding_endpoint="databricks-bge-large-en")
store.setup()

# Persistent handles (graph compile picks these up)
checkpointer = PostgresSaver.from_conn_string(PG_URL).__enter__()

print("  ✅ checkpointer + DatabricksStore ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Define the agent's tools

# COMMAND ----------

from typing import Annotated
from langgraph.prebuilt import InjectedStore
from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig

def _embed_one(t: str) -> list[float]:
    return embed([t])[0]

@tool
def search_kb(question: str) -> str:
    """Search the support knowledge base for policies, procedures, and product info."""
    q_emb = _embed_one(question)
    with engine.begin() as cn:
        rows = cn.execute(text("""
            SELECT d.title, c.text_content,
                   1 - (c.embedding <=> CAST(:q AS VECTOR)) AS sim
            FROM chunks c JOIN documents d USING (doc_id)
            ORDER BY c.embedding <=> CAST(:q AS VECTOR)
            LIMIT 3
        """), dict(q=str(q_emb))).all()
    return "\n\n".join(f"[{r.title}] {r.text_content}" for r in rows)


@tool
def lookup_customer(email: str) -> str:
    """Look up a customer's profile (lifetime spend, country, signup date).
    Returns a JSON string."""
    with engine.begin() as cn:
        r = cn.execute(text("""
            SELECT customer_id, email, full_name, country, created_at, lifetime_spend
            FROM customers WHERE email = :e
        """), dict(e=email)).first()
    if not r:
        return json.dumps({"error": f"no customer with email {email}"})
    return json.dumps({
        "customer_id": r.customer_id, "email": r.email, "full_name": r.full_name,
        "country": r.country, "created_at": str(r.created_at),
        "lifetime_spend_aud": float(r.lifetime_spend),
        "is_vip": float(r.lifetime_spend) > 10_000,
    })


@tool
def lookup_recent_orders(email: str) -> str:
    """Get a customer's most recent 5 orders. Returns JSON."""
    with engine.begin() as cn:
        rows = cn.execute(text("""
            SELECT o.order_id, o.status, o.order_total, o.ordered_at
            FROM orders o JOIN customers c USING (customer_id)
            WHERE c.email = :e
            ORDER BY o.ordered_at DESC LIMIT 5
        """), dict(e=email)).all()
    return json.dumps([{"order_id": r.order_id, "status": r.status,
                        "total_aud": float(r.order_total), "ordered_at": str(r.ordered_at)}
                       for r in rows])


@tool
def save_memory(content: str,
                  config: RunnableConfig,
                  store: Annotated[any, InjectedStore]) -> str:
    """Save a fact about the customer to long-term memory.
    Use sparingly — only for stable preferences or important context."""
    import uuid
    user_id = config["configurable"].get("user_id", "anon")
    store.put(("memories", user_id), str(uuid.uuid4()), {"text": content})
    return f"Saved: {content}"


@tool
def recall_memories(query: str,
                     config: RunnableConfig,
                     store: Annotated[any, InjectedStore]) -> str:
    """Retrieve relevant facts about the customer from long-term memory."""
    user_id = config["configurable"].get("user_id", "anon")
    items = store.search(("memories", user_id), query=query, limit=3)
    if not items:
        return "(no relevant memories)"
    return "\n".join(f"- {i.value['text']}" for i in items)


tools = [search_kb, lookup_customer, lookup_recent_orders, save_memory, recall_memories]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Build the LangGraph agent

# COMMAND ----------

from langgraph.graph import StateGraph, START, END, MessagesState
from langgraph.prebuilt import ToolNode
from databricks_langchain import ChatDatabricks

SYSTEM = """You are a Databricks Customer Support agent. You have these tools:
- search_kb(question): semantic search over our support KB (policies, procedures)
- lookup_customer(email): customer profile incl. VIP status
- lookup_recent_orders(email): customer's 5 most recent orders
- save_memory(content): persist a customer fact across sessions (use sparingly)
- recall_memories(query): retrieve customer facts from prior sessions

Rules:
1. ALWAYS call recall_memories at the start of a new conversation if you have a customer email.
2. ALWAYS check VIP status on substantive requests; escalate VIP issues same-day.
3. Cite the KB policy by name when answering policy questions.
4. Save a memory ONLY when the customer expresses a stable preference or constraint.
5. Be concise — 3-4 sentences max unless asked for detail."""

llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-1-405b-instruct",
    temperature=0.2, max_tokens=600,
)
llm_with_tools = llm.bind_tools(tools)

def agent_node(state: MessagesState):
    messages = state["messages"]
    if not any(getattr(m, "type", None) == "system" for m in messages):
        from langchain_core.messages import SystemMessage
        messages = [SystemMessage(content=SYSTEM)] + messages
    return {"messages": [llm_with_tools.invoke(messages)]}

def should_continue(state: MessagesState):
    last = state["messages"][-1]
    return "tools" if getattr(last, "tool_calls", None) else END

builder = StateGraph(MessagesState)
builder.add_node("agent", agent_node)
builder.add_node("tools", ToolNode(tools))
builder.add_edge(START, "agent")
builder.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})
builder.add_edge("tools", "agent")

agent = builder.compile(checkpointer=checkpointer, store=store)
print("  ✅ Stateful agent compiled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Run end-to-end scenarios

# COMMAND ----------

# Pick a real customer email from our data
with engine.begin() as cn:
    sample_email = cn.execute(text("""
        SELECT email FROM customers WHERE lifetime_spend > 5000
        ORDER BY lifetime_spend DESC LIMIT 1
    """)).scalar() or "user1@example.com"
print(f"Demo customer: {sample_email}")

# COMMAND ----------

# Scenario 1 — Conversation, with the agent saving a preference
config_1 = {"configurable": {"thread_id": f"{sample_email}-mon",
                              "user_id": sample_email}}

print("=" * 70)
print("SCENARIO 1 — First conversation Monday")
print("=" * 70)

for q in [
    f"Hi, my email is {sample_email}. What's the refund policy?",
    "Also, please remember: I always want responses in plain English, no jargon.",
    "What about my recent orders?",
]:
    print(f"\n→ Customer: {q}")
    resp = agent.invoke({"messages": [{"role": "user", "content": q}]}, config=config_1)
    print(f"← Agent: {resp['messages'][-1].content}")

# COMMAND ----------

# Scenario 2 — NEW thread, NEW day, but long-term memory carries over
config_2 = {"configurable": {"thread_id": f"{sample_email}-tue",
                              "user_id": sample_email}}

print("=" * 70)
print("SCENARIO 2 — Tuesday: brand new thread, but LTM remembers")
print("=" * 70)

for q in [
    f"Hi again, this is {sample_email}. Recall my preferences and tell me about VIP escalation rules.",
]:
    print(f"\n→ Customer: {q}")
    resp = agent.invoke({"messages": [{"role": "user", "content": q}]}, config=config_2)
    print(f"← Agent: {resp['messages'][-1].content}")

# COMMAND ----------

# Scenario 3 — Different customer entirely; LTM is empty
config_3 = {"configurable": {"thread_id": "newuser-1",
                              "user_id": "newuser@example.com"}}

print("=" * 70)
print("SCENARIO 3 — Different customer, LTM empty")
print("=" * 70)

for q in [
    "Hi, I'm new. What's the refund policy?",
]:
    print(f"\n→ Customer: {q}")
    resp = agent.invoke({"messages": [{"role": "user", "content": q}]}, config=config_3)
    print(f"← Agent: {resp['messages'][-1].content}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Verify the four memory primitives in Lakebase

# COMMAND ----------

with engine.begin() as cn:
    print("Checkpoints (short-term memory):")
    n = cn.execute(text("SELECT count(DISTINCT thread_id) FROM checkpoints")).scalar()
    print(f"  {n} distinct threads stored\n")

    print("Long-term store entries:")
    rows = cn.execute(text("""
        SELECT prefix, key, value->>'text' AS fact
        FROM store ORDER BY created_at DESC LIMIT 5
    """)).all()
    for r in rows:
        print(f"  ns={r.prefix} key={r.key[:8]}... → {r.fact[:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Wrap as a Databricks App

# COMMAND ----------

# MAGIC %md
# MAGIC The complete app code below goes in a sibling repo (e.g. `lakebase-stateful-agent-app/`).
# MAGIC
# MAGIC ### `app.yaml`
# MAGIC ```yaml
# MAGIC command: ["python", "app.py"]
# MAGIC env:
# MAGIC   - name: LAKEBASE_INSTANCE_NAME
# MAGIC     valueFrom: capstone-lakebase
# MAGIC resources:
# MAGIC   - name: capstone-lakebase
# MAGIC     description: "OLTP + KB + memory store"
# MAGIC     database:
# MAGIC       instance_name: <YOUR_INSTANCE>
# MAGIC       database_name: postgres
# MAGIC       permission: CAN_USE
# MAGIC ```
# MAGIC
# MAGIC ### `app.py` (skeleton — adapt to your UI framework)
# MAGIC ```python
# MAGIC import os, contextlib
# MAGIC from fastapi import FastAPI
# MAGIC from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
# MAGIC from databricks_langchain import AsyncDatabricksStore
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC # ... build the same agent graph as above, but with Async variants ...
# MAGIC
# MAGIC INSTANCE = os.environ["LAKEBASE_INSTANCE_NAME"]
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC def get_dsn():
# MAGIC     info = w.database.get_database_instance(name=INSTANCE)
# MAGIC     user = w.current_user.me().user_name
# MAGIC     token = w.database.generate_database_credential(
# MAGIC         request_id="app", instance_names=[INSTANCE]).token
# MAGIC     return f"postgresql://{user}:{token}@{info.read_write_dns}:5432/postgres?sslmode=require"
# MAGIC
# MAGIC @contextlib.asynccontextmanager
# MAGIC async def lifespan(app):
# MAGIC     dsn = get_dsn()
# MAGIC     async with AsyncPostgresSaver.from_conn_string(dsn) as cp, \
# MAGIC                AsyncDatabricksStore(conn=dsn,
# MAGIC                                       embedding_endpoint="databricks-bge-large-en") as st:
# MAGIC         await cp.setup(); await st.setup()
# MAGIC         app.state.checkpointer = cp
# MAGIC         app.state.store = st
# MAGIC         app.state.agent = build_agent(cp, st)  # your build function
# MAGIC         yield
# MAGIC
# MAGIC app = FastAPI(lifespan=lifespan)
# MAGIC
# MAGIC @app.post("/chat")
# MAGIC async def chat(req: dict):
# MAGIC     resp = await app.state.agent.ainvoke(
# MAGIC         {"messages": [{"role": "user", "content": req["message"]}]},
# MAGIC         config={"configurable": {"thread_id": req["thread_id"],
# MAGIC                                    "user_id": req["user_id"]}},
# MAGIC     )
# MAGIC     return {"answer": resp["messages"][-1].content}
# MAGIC ```
# MAGIC
# MAGIC ### Deploy
# MAGIC ```bash
# MAGIC databricks bundle deploy --target dev --profile <your-profile>
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you've built (the full picture)
# MAGIC
# MAGIC | Module | Capability | What's in this Capstone |
# MAGIC |---|---|---|
# MAGIC | 02 | Provisioning | One Lakebase instance — could be either mode |
# MAGIC | **03** | **CMK** | Whatever you set on the workspace flows through automatically |
# MAGIC | 04 | OAuth + auto-refresh | `Includes/lakebase_helpers.get_oauth_engine` |
# MAGIC | 05 | Federation | Optional read-side ingress |
# MAGIC | 06 | Sync | Operational tables loaded from Delta |
# MAGIC | **07** | **pgvector** | Knowledge base + HNSW index |
# MAGIC | 08 | Branching | Use a branch for a "what if?" KB version |
# MAGIC | 09 | Observability | `pg_stat_statements` watches your tools |
# MAGIC | 10 | Backup | Continuous PITR across all four data shapes |
# MAGIC | 11 | Apps integration | The deployment shape above |
# MAGIC | **11b** | **LangGraph memory** | Checkpointer + DatabricksStore |
# MAGIC
# MAGIC The architecture is the **mature pattern** for an enterprise AI agent: one operational+vector+memory database, one governance plane, one identity, one CMK.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Run when done with the lab session:
# %run ./Includes/Cleanup
print("Run `%run ./Includes/Cleanup` when ready to tear down billable resources.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎓 Course complete
# MAGIC
# MAGIC You've covered every key Lakebase capability — including CMK, pgvector, branching, sync, observability, HA, Apps integration, **and stateful agent memory** — and assembled the production reference architecture for an internal AI agent.
# MAGIC
# MAGIC <details>
# MAGIC <summary>What to do next</summary>
# MAGIC
# MAGIC - **Migrate one real workload**: pick a team's existing OLTP + memory pattern and consolidate to Lakebase
# MAGIC - **Run the CMK setup once** in a dev workspace to learn your cloud's KMS quirks before doing it for real
# MAGIC - **Build the App**: take the skeleton above, plug into your existing chat UI, deploy
# MAGIC - **Share back**: if you find gaps in this lab, PR or DM the maintainer
# MAGIC
# MAGIC </details>
