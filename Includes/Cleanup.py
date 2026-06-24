# Databricks notebook source
# MAGIC %md
# MAGIC # Cleanup — tear down all lab artifacts
# MAGIC
# MAGIC Run this at the end of each session to stop billable resources. Safe to re-run; idempotent.

# COMMAND ----------

# MAGIC %run ./Setup

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# 1. Delete every Lakebase instance with our LAB_PREFIX
print("Deleting Lakebase instances...")
for inst in w.database.list_database_instances():
    if inst.name and inst.name.startswith(LAB_PREFIX):
        try:
            w.database.delete_database_instance(name=inst.name, force=True)
            print(f"  ✅ deleted {inst.name}")
        except Exception as e:
            print(f"  ⚠️  could not delete {inst.name}: {e}")

# 2. Drop the lab catalog (cascades to schema + tables, including any synced tables)
print(f"\nDropping lab catalog {LAB_CATALOG}...")
try:
    spark.sql(f"DROP CATALOG IF EXISTS {LAB_CATALOG} CASCADE")
    print(f"  ✅ dropped {LAB_CATALOG}")
except Exception as e:
    print(f"  ⚠️  could not drop {LAB_CATALOG}: {e}")

# 3. Note for CMK: KMS keys are NOT deleted from cleanup (intentionally —
# deleting a CMK is a security-sensitive action you should do manually,
# usually after the 30-day waiting period).
print("\nNote: CMK / KMS keys created in module 03 are NOT auto-deleted. "
      "Disable / delete from your cloud console when ready.")

print("\n✅ Cleanup complete.")
