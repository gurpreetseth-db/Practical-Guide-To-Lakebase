# Databricks notebook source
# MAGIC %md
# MAGIC # 11b · Demo — Agent Memory on Lakebase (LangGraph + DatabricksStore)
# MAGIC
# MAGIC **Duration:** ~45 min · **Type:** Hands-on · **Prerequisite:** Modules 01-07, 11
# MAGIC
# MAGIC The Capstone (Module 12) builds a stateful customer-support agent. To do that, you need **agent memory** — and Lakebase is the natural backing store for it. This module inserts the memory foundation: short-term (per-conversation) + long-term (per-user) memory, both backed by Lakebase.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · Why agents need memory — and why two kinds
# MAGIC
# MAGIC **Stateless agent (what you built in Module 07's RAG demo):**
# MAGIC ```
# MAGIC user → LLM (no context) → answer
# MAGIC user → LLM (no context) → answer    ← forgot the previous turn entirely
# MAGIC ```
# MAGIC
# MAGIC **Stateful agent (what you'll build here):**
# MAGIC ```
# MAGIC user → [thread_id checkpoint] ← short-term memory: previous turns
# MAGIC user → [user_id store] ← long-term memory: curated facts about THIS user
# MAGIC user → LLM (full context) → answer + maybe save a new fact
# MAGIC ```
# MAGIC
# MAGIC ### The two flavours
# MAGIC
# MAGIC | | **Short-term (checkpointer)** | **Long-term (store)** |
# MAGIC |---|---|---|
# MAGIC | What it remembers | The whole conversation state (messages, tool calls, scratchpad) | Curated facts the agent CHOSE to save |
# MAGIC | Keyed by | `thread_id` (one per conversation/session) | `user_id` (or namespace) — survives across sessions |
# MAGIC | Storage shape | A blob per checkpoint, replayed for state | Vector-searchable facts (pgvector) |
# MAGIC | When to write | Automatically after every node/turn | Explicitly via a `save_memory` tool the LLM calls |
# MAGIC | Retention | Often days — cleaned up via TTL | Indefinite (curate by adding/deleting via tool calls) |
# MAGIC | Lakebase tables it creates | `checkpoints`, `checkpoint_blobs`, `checkpoint_writes` | `store`, `store_vectors` |
# MAGIC
# MAGIC **Both live in your Lakebase database.** Same governance, same CMK, same backups. That's the win — no separate memory service to operate.

# COMMAND ----------

# MAGIC %pip install -q "langgraph" "langgraph-checkpoint-postgres" "databricks-langchain" "langchain-core" "psycopg[binary]>=3.2"
# MAGIC %pip install --upgrade "databricks-sdk>=0.40"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · Set up the Lakebase instance for memory

# COMMAND ----------

# DBTITLE 1,Cell 6
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance
from Includes.lakebase_helpers import wait_for_instance, get_oauth_engine, dsn_for_instance
from sqlalchemy import text

w = WorkspaceClient()
INSTANCE = f"{LAB_PREFIX}-memory".replace("_", "-")

try:
    info = w.database.get_database_instance(name=INSTANCE)
    print(f"Re-using {INSTANCE}")
except Exception:
    print(f"Creating {INSTANCE}...")
    w.database.create_database_instance(DatabaseInstance(name=INSTANCE, capacity="CU_2"))
    wait_for_instance(INSTANCE)

# Plain Postgres connection string (LangGraph's checkpointer wants a libpq DSN)
PG_DSN = dsn_for_instance(INSTANCE).replace("?sslmode=require", " sslmode=require")
# Convert URL form to keyword form for psycopg.AsyncConnection
import re
m = re.match(r"postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(\w+)", PG_DSN.split(" ")[0])
PG_KWARGS = dict(user=m.group(1), password=m.group(2), host=m.group(3),
                  port=int(m.group(4)), dbname=m.group(5), sslmode="require")

print(f"Memory backed by: {INSTANCE} → postgres")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · Short-term memory — a `PostgresSaver` checkpointer
# MAGIC
# MAGIC LangGraph's `PostgresSaver` (sync) and `AsyncPostgresSaver` (async — for Apps) persist conversation state per thread.

# COMMAND ----------

# DBTITLE 1,Cell 8
from langgraph.checkpoint.postgres import PostgresSaver

# `setup()` creates the checkpoint tables — idempotent, safe to call repeatedly.
# Use key=value DSN to avoid URL-parsing ambiguity with '@' in username
_dsn_kv = " ".join(f"{k}={v}" for k, v in {**PG_KWARGS, "dbname": "databricks_postgres"}.items())
with PostgresSaver.from_conn_string(_dsn_kv) as cps:
    cps.setup()
print("  ✅ checkpoint tables created (checkpoints, checkpoint_blobs, checkpoint_writes)")

with get_oauth_engine(INSTANCE).begin() as cn:
    rows = cn.execute(text(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name LIKE 'checkpoint%' "
        "ORDER BY table_name"
    )).all()
    print("Tables:", [r.table_name for r in rows])

# COMMAND ----------

# MAGIC %md
# MAGIC ## D · A trivial stateful chat (short-term memory only)

# COMMAND ----------

# DBTITLE 1,Cell 10
from langgraph.graph import StateGraph, START, END, MessagesState
from databricks_langchain import ChatDatabricks

llm = ChatDatabricks(endpoint="databricks-meta-llama-3-3-70b-instruct", temperature=0.3)

def chat_node(state: MessagesState):
    """One conversation turn — LLM sees full message history from the checkpoint."""
    response = llm.invoke(state["messages"])
    return {"messages": [response]}

graph_builder = StateGraph(MessagesState)
graph_builder.add_node("chat", chat_node)
graph_builder.add_edge(START, "chat")
graph_builder.add_edge("chat", END)

# Compile with checkpointer — this is what makes the graph stateful
_dsn_kv = " ".join(f"{k}={v}" for k, v in {**PG_KWARGS, "dbname": "databricks_postgres"}.items())
checkpointer = PostgresSaver.from_conn_string(_dsn_kv).__enter__()
graph = graph_builder.compile(checkpointer=checkpointer)

# COMMAND ----------

# DBTITLE 1,Cell 11
# Have a conversation in thread "alice-001"
config = {"configurable": {"thread_id": "alice-001"}}

# Use a fresh connection per-cell — manually entered context managers don't stay alive across cells
with PostgresSaver.from_conn_string(_dsn_kv) as cps:
    _graph = graph_builder.compile(checkpointer=cps)

    resp = _graph.invoke(
        {"messages": [{"role": "user", "content": "Hi! I'm Alice. I'm setting up a Lakebase database."}]},
        config=config,
    )
    print("Turn 1:", resp["messages"][-1].content[:200])

    resp = _graph.invoke(
        {"messages": [{"role": "user", "content": "What's my name and what was I working on?"}]},
        config=config,
    )
    print("\nTurn 2:", resp["messages"][-1].content[:200])
    # ⬑ The agent remembers because PostgresSaver loaded thread "alice-001"'s history

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resume a different thread — agent shouldn't have any context

# COMMAND ----------

# DBTITLE 1,Cell 13
config_bob = {"configurable": {"thread_id": "alice-004"}}

with PostgresSaver.from_conn_string(_dsn_kv) as cps:
    _graph = graph_builder.compile(checkpointer=cps)
    
    resp = _graph.invoke(
        {"messages": [{"role": "user", "content": "Do you know my name?"}]},
        config=config_bob,
    )
    print("Alice's turn 3:", resp["messages"][-1].content[:200])
# Should be "no, I don't know your name" — different thread, fresh state

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect what's in the checkpoint

# COMMAND ----------

# DBTITLE 1,Cell 15
from sqlalchemy import create_engine
from sqlalchemy.engine import URL as _SAURL
_mem_engine = create_engine(_SAURL.create(
    "postgresql+psycopg",
    username=PG_KWARGS["user"],
    password=PG_KWARGS["password"],
    host=PG_KWARGS["host"],
    port=PG_KWARGS["port"],
    database="databricks_postgres",
    query={"sslmode": "require"},
))
with _mem_engine.begin() as cn:
    rows = cn.execute(text("""
        SELECT thread_id, checkpoint_id, type
        FROM checkpoints
        ORDER BY checkpoint_id DESC
        LIMIT 6
    """)).all()
    for r in rows:
        print(f"  thread={r.thread_id}  ckpt={r.checkpoint_id[:18]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## E · Long-term memory — a `DatabricksStore` (pgvector-backed)
# MAGIC
# MAGIC Long-term memory persists curated facts across sessions. The store is keyed by an **arbitrary namespace** (commonly `user_id`) and supports semantic search over saved facts via pgvector.

# COMMAND ----------

# DBTITLE 1,Cell 17
# `databricks_langchain.DatabricksStore` is a LangGraph BaseStore implementation.
# It wraps pgvector for semantic memory recall.
from databricks_langchain import DatabricksStore

# Explicitly specify the database name to ensure connection targets the production project
store = DatabricksStore(
    instance_name=INSTANCE,
    embedding_endpoint="databricks-bge-large-en",
    embedding_dims=1024,
)
store.setup()  # creates `store` and `store_vectors` tables (idempotent)

# Inspect the tables it created:
with _mem_engine.begin() as cn:
    rows = cn.execute(text(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE 'store%' ORDER BY table_name"
    )).all()
    print("Tables:", [r.table_name for r in rows])
    print(f"instance: {INSTANCE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save and recall facts

# COMMAND ----------

# Each fact lives at (namespace, key, value). Namespace is a tuple of strings.
# Common pattern: ("memories", user_id)
NS = ("memories", "alice-001")

store.put(NS, "name",     {"text": "User's name is Alice."})
store.put(NS, "project",  {"text": "Alice is setting up a Lakebase database for fraud detection."})
store.put(NS, "timezone", {"text": "Alice is in Australia/Sydney timezone."})

# Semantic recall via pgvector under the hood
items = store.search(NS, query="What's the user working on?", limit=2)
for item in items:
    print(f"  [{item.score:.2f}]  {item.value['text']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## F · Combine — graph with BOTH memory types

# COMMAND ----------

from typing import Annotated
from langgraph.prebuilt import InjectedStore
from langchain_core.tools import tool
from langgraph.prebuilt import ToolNode
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict

# 1. Define tools that read/write long-term memory
@tool
def save_memory(content: str, store: Annotated[any, InjectedStore]) -> str:
    """Save a fact about the user that should be remembered across sessions."""
    import uuid
    user_id = "alice-001"  # In production: read from RunnableConfig.configurable.user_id
    ns = ("memories", user_id)
    store.put(ns, str(uuid.uuid4()), {"text": content})
    return f"Saved: {content}"


@tool
def recall_memories(query: str, store: Annotated[any, InjectedStore]) -> str:
    """Retrieve relevant facts about the user from long-term memory."""
    user_id = "alice-001"
    items = store.search(("memories", user_id), query=query, limit=3)
    if not items:
        return "(no relevant memories)"
    return "\n".join(f"- {i.value['text']}" for i in items)


tools = [save_memory, recall_memories]
llm_with_tools = llm.bind_tools(tools)

# 2. Define the graph
def chat_with_tools(state: MessagesState):
    return {"messages": [llm_with_tools.invoke(state["messages"])]}

def should_continue(state: MessagesState):
    last = state["messages"][-1]
    return "tools" if getattr(last, "tool_calls", None) else END

graph_builder = StateGraph(MessagesState)
graph_builder.add_node("chat", chat_with_tools)
graph_builder.add_node("tools", ToolNode(tools))
graph_builder.add_edge(START, "chat")
graph_builder.add_conditional_edges("chat", should_continue, {"tools": "tools", END: END})
graph_builder.add_edge("tools", "chat")

# Compile with BOTH checkpointer (short-term) and store (long-term)
graph = graph_builder.compile(checkpointer=checkpointer, store=store)

# COMMAND ----------

# DBTITLE 1,Cell 22
# Session 1 — Alice tells the agent something worth remembering
config = {"configurable": {"thread_id": "alice-session-1", "user_id": "alice-001"}}

with PostgresSaver.from_conn_string(_dsn_kv) as cps:
    _graph = graph_builder.compile(checkpointer=cps)

    resp = _graph.invoke(
        {"messages": [{"role": "user", "content": "Please remember: I prefer concise responses, no longer than 3 sentences."}]},
        config=config,
    )
    print("Session 1:", resp["messages"][-1].content[:300])

# COMMAND ----------

# Session 2 — different thread (so short-term memory is empty), but long-term store carries forward
config = {"configurable": {"thread_id": "alice-session-2", "user_id": "alice-001"}}

with PostgresSaver.from_conn_string(_dsn_kv) as cps:
    _graph = graph_builder.compile(checkpointer=cps)

    resp = _graph.invoke(
        {"messages": [{"role": "user",
                "content": "Tell me about Lakebase architecture. Recall any preferences I've expressed."}]},
        config=config,
    )
    print("Session 2 (new thread, but LTM remembers):")
    print(resp["messages"][-1].content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## G · Async patterns for Databricks Apps
# MAGIC
# MAGIC In a Databricks App with FastAPI / async I/O, swap to the async variants:
# MAGIC
# MAGIC ```python
# MAGIC from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
# MAGIC from databricks_langchain import AsyncDatabricksStore
# MAGIC
# MAGIC # Production app startup
# MAGIC async def lifespan(app):
# MAGIC     async with AsyncPostgresSaver.from_conn_string(PG_DSN) as cp, \\
# MAGIC                AsyncDatabricksStore(conn=PG_DSN, embedding_endpoint="databricks-bge-large-en") as st:
# MAGIC         await cp.setup()
# MAGIC         await st.setup()
# MAGIC         app.state.checkpointer = cp
# MAGIC         app.state.store = st
# MAGIC         yield
# MAGIC ```
# MAGIC
# MAGIC The async variants stream tokens correctly and don't block the event loop on each Postgres roundtrip.

# COMMAND ----------

# MAGIC %md
# MAGIC ## H · Operational notes
# MAGIC
# MAGIC | Concern | Recommendation |
# MAGIC |---|---|
# MAGIC | Checkpoint table growth | Add a TTL job: `DELETE FROM checkpoints WHERE created_at < now() - INTERVAL '30 days'` weekly |
# MAGIC | Long-term store growth | LLM-driven facts are tiny; growth is slow. Vacuum + analyze monthly. |
# MAGIC | PII in memory | The `store_vectors` table has embeddings of user content — treat with the same data classification as the source content |
# MAGIC | CMK | Both checkpoint and store tables live in your Lakebase database, so they inherit your CMK encryption (Module 03). No additional setup. |
# MAGIC | Multi-tenant isolation | Use the `user_id` namespace consistently; for hard isolation, use one Lakebase database per tenant |
# MAGIC | Token rotation | The PostgresSaver opens its own connection — wrap it in a context manager that re-creates with a fresh OAuth token at the start of each request, OR use `AsyncPostgresSaver` with `pool_pre_ping=True` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## I · What you've built
# MAGIC
# MAGIC | Capability | How |
# MAGIC |---|---|
# MAGIC | Per-conversation memory | `PostgresSaver` keyed by `thread_id` |
# MAGIC | Per-user memory | `DatabricksStore` keyed by `user_id` namespace |
# MAGIC | Semantic recall | pgvector under the hood (no extra index management) |
# MAGIC | Tools for the agent | `save_memory` + `recall_memories` injected via `InjectedStore` |
# MAGIC | Single-database story | Both memory tables live alongside your operational data |
# MAGIC | Same governance | UC + CMK apply to memory tables automatically |

# COMMAND ----------

# MAGIC %md
# MAGIC ### J.Cleanup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance
from Includes.lakebase_helpers import wait_for_instance

w = WorkspaceClient()

try:
    w.database.delete_database_instance(
        name=INSTANCE
    )
    wait_for_instance(INSTANCE, timeout_seconds=600)
    print(f"✅ {INSTANCE} Deleted Successfully ")
except Exception as e:
    if "not found" in str(e).lower():
        print(f"ℹ️  {INSTANCE} does not exist (already deleted).")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** **12 Capstone — End-to-End Stateful Agent on Lakebase** brings these memory primitives together with the RAG corpus and operational data lookup into a single deployable app.