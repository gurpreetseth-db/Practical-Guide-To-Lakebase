# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Demo — Customer-Managed Keys (CMK) for Lakebase
# MAGIC
# MAGIC **Duration:** ~60 minutes · **Type:** Hands-on demo · **Prerequisite:** Modules 01 & 02 · Cloud-side admin access required
# MAGIC
# MAGIC > **⚠️ READ FIRST:** This module is the only one that requires **cloud-side admin actions** (creating a KMS key, granting permissions). If you are not the workspace's cloud-account admin, partner with your security/cloud team **before** starting. Allow ~30 minutes of cross-team coordination on first run.
# MAGIC
# MAGIC By the end of this module you will have:
# MAGIC
# MAGIC 1. Created a KMS key (AWS / Azure / GCP) under your control
# MAGIC 2. Granted the workspace service the precise IAM permissions to use it
# MAGIC 3. Configured Databricks to use that key for **managed services** (which includes Lakebase storage)
# MAGIC 4. Stood up a Lakebase database under CMK encryption
# MAGIC 5. **Validated** that the encryption is using your key (not the default)
# MAGIC 6. **Rotated** the key and verified continuity
# MAGIC 7. **Run a revocation drill** — pulled the key, watched access break, restored access
# MAGIC 8. Walked through the **disaster-recovery procedure** if you ever lose key access permanently

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2" "sqlalchemy>=2"
# MAGIC %pip install --upgrade "databricks-sdk>=0.40"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · Concept refresher
# MAGIC
# MAGIC From Module 01, recall envelope encryption:
# MAGIC
# MAGIC ```
# MAGIC  Object storage page  ──encrypted with──▶  Data Encryption Key (DEK)
# MAGIC                                                       │
# MAGIC                                                       └─wrapped by─▶  Key Encryption Key (KEK) ◀── YOUR CMK
# MAGIC ```
# MAGIC
# MAGIC ### What CMK actually buys you
# MAGIC
# MAGIC | Capability | Without CMK | With CMK |
# MAGIC |---|---|---|
# MAGIC | Data encrypted at rest | ✅ (Databricks key) | ✅ (your key) |
# MAGIC | You can rotate the key | ❌ (Databricks does it) | ✅ on your schedule |
# MAGIC | You can audit key use | Limited | Full CloudTrail / KV / KMS logs |
# MAGIC | You can **revoke** to deny all access | ❌ | ✅ instant kill-switch |
# MAGIC | You can satisfy compliance "we hold the keys" controls | ❌ | ✅ (HIPAA, FedRAMP, FFIEC, etc.) |
# MAGIC | If you lose the key — you lose the data | n/a | ✅ — this is real, plan accordingly |
# MAGIC
# MAGIC The last row is critical: **CMK gives you sovereignty AND responsibility**. If your KMS key is deleted/revoked permanently, the data is unrecoverable. There is no Databricks back-door. This is a feature, not a bug. The recovery section at the end of this module is what every team needs to operationalize.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · CMK in Databricks — the architecture
# MAGIC
# MAGIC Databricks supports three CMK "use cases":
# MAGIC
# MAGIC | Use case | What it encrypts | Lakebase relevance |
# MAGIC |---|---|---|
# MAGIC | **Managed services** | Notebooks, secrets, query history, Lakebase storage | **Required for CMK on Lakebase** |
# MAGIC | **Workspace storage (DBFS root, cluster logs)** | DBFS root; cluster log buckets | Indirectly — same key can be used or a different one |
# MAGIC | **EBS volumes (for VMs in your VPC)** | Cluster local disk | Not Lakebase — different layer |
# MAGIC
# MAGIC Lakebase's data sits behind the **managed services** boundary. When you configure a managed-services CMK, Lakebase storage is automatically encrypted with that key.
# MAGIC
# MAGIC ### Where the configuration lives
# MAGIC
# MAGIC - **AWS / GCP**: configured at the **workspace** level via the **Account Console** (Account admin)
# MAGIC - **Azure**: configured at workspace creation via ARM template / Terraform / Azure portal (Tenant admin)
# MAGIC
# MAGIC The configuration is **immutable for some fields** post-creation depending on cloud (e.g. AWS allows updating the key ARN; Azure requires recreate for some scenarios). Validate with current docs before production.
# MAGIC
# MAGIC ### What data will be managed under Customer Managed Keys (CMK)?
# MAGIC - S3 (storage): Long‑term repository for write‑ahead logs (WAL), pages, and branches; all objects are encrypted under the customer’s CMK for compliant, auditable control.
# MAGIC - Page Servers (storage): The Postgres‑aware storage service that serves “hot” pages for a short period and syncs them to S3; all on‑disk content is CMK‑encrypted.
# MAGIC - Safekeepers (storage): The durable, consensus‑replicated WAL buffer that holds recent updates before they are processed and archived; all WAL persisted here is CMK‑encrypted.
# MAGIC - Compute/Postgres (compute): execution tier that runs queries and manages caches/temp files. All compute disk writes (OS/data disk, PostgreSQL temp files, unlogged tables, WAL artifacts, query cache) use in-memory, per-boot ephemeral DEKs generated locally. If the CMK is revoked, the compute control plane shuts down the instance, discarding the key and making disk data inaccessible.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · Pick your cloud and follow that path
# MAGIC
# MAGIC Sections C-AWS, C-Azure, C-GCP are mutually exclusive. Skip to your cloud.
# MAGIC
# MAGIC | If your workspace is on… | Go to section |
# MAGIC |---|---|
# MAGIC | AWS | C1 (immediately below) |
# MAGIC | Azure | C2 |
# MAGIC | GCP | C3 |
# MAGIC
# MAGIC When done with the cloud-specific setup, all three paths converge at **section D — Validate the encryption**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C1 · AWS — Create the KMS key + grant the workspace
# MAGIC
# MAGIC ### Step 1.1 — Create the KMS key
# MAGIC
# MAGIC In the AWS console (or via CLI as below), create a **symmetric, single-region** KMS key in the workspace's region:
# MAGIC
# MAGIC ```bash
# MAGIC # As your AWS account admin
# MAGIC aws kms create-key \
# MAGIC   --description "Databricks Lakebase CMK — workspace <name>" \
# MAGIC   --key-usage ENCRYPT_DECRYPT \
# MAGIC   --customer-master-key-spec SYMMETRIC_DEFAULT \
# MAGIC   --region us-west-2 \
# MAGIC   --tags TagKey=Purpose,TagValue=DatabricksLakebase \
# MAGIC          TagKey=Owner,TagValue=<your-team> \
# MAGIC          TagKey=Compliance,TagValue=HIPAA   # adjust per your governance
# MAGIC ```
# MAGIC
# MAGIC Note the **Key ARN** returned. You'll need it in step 1.3.
# MAGIC
# MAGIC Then create an alias for human readability:
# MAGIC
# MAGIC ```bash
# MAGIC aws kms create-alias \
# MAGIC   --alias-name alias/databricks-lakebase-cmk \
# MAGIC   --target-key-id <key-id-from-above> \
# MAGIC   --region us-west-2
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.2 — Set the key policy
# MAGIC
# MAGIC The key policy is the **most important** thing to get right. Databricks needs `Encrypt`, `Decrypt`, `ReEncryptFrom`, `ReEncryptTo`, `GenerateDataKey`, `DescribeKey`. **Do not** grant `kms:ScheduleKeyDeletion` or `kms:Delete*` — those should remain with your security team only.
# MAGIC
# MAGIC ```bash
# MAGIC cat > key-policy.json <<'EOF'
# MAGIC {
# MAGIC   "Version": "2012-10-17",
# MAGIC   "Id": "lakebase-cmk-policy",
# MAGIC   "Statement": [
# MAGIC     {
# MAGIC       "Sid": "Enable IAM User Permissions",
# MAGIC       "Effect": "Allow",
# MAGIC       "Principal": {"AWS": "arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:root"},
# MAGIC       "Action": "kms:*",
# MAGIC       "Resource": "*"
# MAGIC     },
# MAGIC     {
# MAGIC       "Sid": "Allow Databricks managed services to use the key",
# MAGIC       "Effect": "Allow",
# MAGIC       "Principal": {
# MAGIC         "AWS": "arn:aws:iam::414351767826:root"
# MAGIC       },
# MAGIC       "Action": [
# MAGIC         "kms:Encrypt",
# MAGIC         "kms:Decrypt",
# MAGIC         "kms:ReEncrypt*",
# MAGIC         "kms:GenerateDataKey*",
# MAGIC         "kms:DescribeKey"
# MAGIC       ],
# MAGIC       "Resource": "*",
# MAGIC       "Condition": {
# MAGIC         "StringEquals": {
# MAGIC           "aws:PrincipalTag/DatabricksAccountId": "<YOUR_DATABRICKS_ACCOUNT_ID>"
# MAGIC         }
# MAGIC       }
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC EOF
# MAGIC
# MAGIC aws kms put-key-policy \
# MAGIC   --key-id <key-id> \
# MAGIC   --policy-name default \
# MAGIC   --policy file://key-policy.json \
# MAGIC   --region us-west-2
# MAGIC ```
# MAGIC
# MAGIC > **💡 INSIGHT** — The principal `arn:aws:iam::414351767826:root` is the **Databricks production AWS account** that runs the managed services. The `Condition` clause scopes that broad access down to *your* Databricks account ID, so even though many customers' workspaces share that AWS account, only yours can use this key. Always include the condition.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.3 — Register the CMK with Databricks (Account Console)
# MAGIC
# MAGIC Log into [accounts.cloud.databricks.com](https://accounts.cloud.databricks.com) as Account admin:
# MAGIC
# MAGIC 1. **Security → Encryption keys → Add encryption key configuration**
# MAGIC 2. Use case: **Managed services**
# MAGIC 3. Key ARN: paste from step 1.1
# MAGIC 4. Key Alias: paste from step1.1
# MAGIC 5. Save → note the **Encryption key configuration ID** that comes back
# MAGIC
# MAGIC Then attach it to your workspace:
# MAGIC
# MAGIC 6. **Workspaces → \<your workspace> → Configuration → Encryption → Add → CNK for managed services**
# MAGIC 7. **Managed services encryption key**: select the configuration you just created
# MAGIC 8. Save. Workspace will redeploy the managed-services components — typically 2–10 minutes. Lakebase instances created **after** this point will use your CMK. **Existing instances** continue using whatever key they were created with.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.4 — Same configuration via Databricks CLI (preferred, scriptable)
# MAGIC
# MAGIC ```bash
# MAGIC # As Databricks Account admin (use a profile pointing at accounts.cloud.databricks.com)
# MAGIC databricks --profile ACCOUNTS account customer-managed-keys create \
# MAGIC   --use-cases MANAGED_SERVICES \
# MAGIC   --aws-key-info '{
# MAGIC     "key_arn": "arn:aws:kms:us-west-2:111111111111:key/abc-123",
# MAGIC     "key_alias": "alias/databricks-lakebase-cmk"
# MAGIC   }'
# MAGIC
# MAGIC # → returns customer_managed_key_id; capture it
# MAGIC
# MAGIC databricks --profile ACCOUNTS account workspaces update <workspace-id> \
# MAGIC   --managed-services-customer-managed-key-id <id>
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## C2 · Azure — Key Vault key + workspace identity
# MAGIC
# MAGIC ### Step 2.1 — Provision Key Vault + key
# MAGIC
# MAGIC Use a **Premium Key Vault with purge protection enabled and soft-delete enabled** (Databricks won't accept Standard or non-protected vaults).
# MAGIC
# MAGIC ```bash
# MAGIC # As Azure subscription owner (or with Key Vault Contributor + Key Vault Crypto Officer)
# MAGIC az keyvault create \
# MAGIC   --name databricks-lakebase-kv \
# MAGIC   --resource-group <rg> \
# MAGIC   --location australiaeast \
# MAGIC   --sku premium \
# MAGIC   --enable-purge-protection true \
# MAGIC   --enable-soft-delete true \
# MAGIC   --enable-rbac-authorization true   # we'll use RBAC, not access policies
# MAGIC
# MAGIC az keyvault key create \
# MAGIC   --vault-name databricks-lakebase-kv \
# MAGIC   --name lakebase-cmk \
# MAGIC   --kty RSA \
# MAGIC   --size 4096
# MAGIC ```
# MAGIC
# MAGIC ### Step 2.2 — Grant the Databricks workspace identity
# MAGIC
# MAGIC The Databricks workspace runs as a managed identity in Azure. Grant that identity the **Key Vault Crypto Service Encryption User** role on the key:
# MAGIC
# MAGIC ```bash
# MAGIC # The workspace's managed identity ID — find under Azure Portal:
# MAGIC #   Databricks workspace → Properties → Managed Resource Group → Find the SP
# MAGIC az role assignment create \
# MAGIC   --assignee <workspace-managed-identity-object-id> \
# MAGIC   --role "Key Vault Crypto Service Encryption User" \
# MAGIC   --scope /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/databricks-lakebase-kv/keys/lakebase-cmk
# MAGIC ```
# MAGIC
# MAGIC ### Step 2.3 — Configure the Databricks workspace
# MAGIC
# MAGIC In the Azure portal:
# MAGIC
# MAGIC 1. **Databricks workspace → Encryption → Edit**
# MAGIC 2. **Customer-managed key for managed services**: enable
# MAGIC 3. Key Vault URI: `https://databricks-lakebase-kv.vault.azure.net/`
# MAGIC 4. Key name: `lakebase-cmk`
# MAGIC 5. Key version: leave blank (auto-rotate to latest) OR pin a specific version
# MAGIC 6. Save → workspace will roll the configuration. ~5–10 min.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C3 · GCP — Cloud KMS key + workspace SA grant
# MAGIC
# MAGIC ### Step 3.1 — Create the KMS keyring + key
# MAGIC
# MAGIC ```bash
# MAGIC # As project owner
# MAGIC gcloud kms keyrings create databricks-lakebase \
# MAGIC   --location <region> \
# MAGIC   --project <project>
# MAGIC
# MAGIC gcloud kms keys create lakebase-cmk \
# MAGIC   --keyring databricks-lakebase \
# MAGIC   --location <region> \
# MAGIC   --purpose encryption \
# MAGIC   --rotation-period 90d \
# MAGIC   --next-rotation-time $(date -u -v+90d +'%Y-%m-%dT%H:%M:%SZ') \
# MAGIC   --project <project>
# MAGIC ```
# MAGIC
# MAGIC ### Step 3.2 — Grant the Databricks workspace SA
# MAGIC
# MAGIC ```bash
# MAGIC # Find the workspace SA (visible in the Account Console under your workspace)
# MAGIC export DBX_SA="db-...@databricks-prod-1.iam.gserviceaccount.com"
# MAGIC
# MAGIC gcloud kms keys add-iam-policy-binding lakebase-cmk \
# MAGIC   --keyring databricks-lakebase \
# MAGIC   --location <region> \
# MAGIC   --member "serviceAccount:$DBX_SA" \
# MAGIC   --role roles/cloudkms.cryptoKeyEncrypterDecrypter \
# MAGIC   --project <project>
# MAGIC ```
# MAGIC
# MAGIC ### Step 3.3 — Register with Databricks (Account Console)
# MAGIC
# MAGIC Same path as AWS — Account Console → Cloud resources → Encryption keys → Add. Use the GCP key resource name format:
# MAGIC
# MAGIC ```
# MAGIC projects/<project>/locations/<region>/keyRings/databricks-lakebase/cryptoKeys/lakebase-cmk
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## D · Verify the workspace is running on CMK
# MAGIC
# MAGIC Check the workspace metadata to confirm the CMK config landed:

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## E · Create a Lakebase database under CMK
# MAGIC
# MAGIC With the workspace configured, **every new Lakebase instance** automatically uses your CMK. Let's create one and verify.

# COMMAND ----------

# Lakebase Autoscaling uses the Postgres API (w.postgres) — not the legacy Database Instance API.
# Autoscaling is configured per-endpoint within a project.

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Project, ProjectSpec, Endpoint, EndpointSpec, EndpointType, Duration, FieldMask
)

w = WorkspaceClient()

auto_project_id = f"{LAB_PREFIX}-cmk-demo".replace("_", "-")
print(f"Creating AUTOSCALE project: {auto_project_id}")

# Step 1: Create the project
try:
    operation = w.postgres.create_project(
        project=Project(
            spec=ProjectSpec(
                display_name=f"{LAB_PREFIX} CMK Demo",
                pg_version=17,
            )
        ),
        project_id=auto_project_id,
    )
    result = operation.wait()
    print(f"  ✅ Project created: {result.name}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"  ℹ️  Project already exists, continuing...")
        result = w.postgres.get_project(name=f"projects/{auto_project_id}")
    else:
        raise e

# Step 2: Configure autoscaling on the endpoint (min 0.5 CU, max 1 CU, scale-to-zero)
# Discover the default branch dynamically
branch_name = result.status.default_branch if result.status and result.status.default_branch else f"projects/{auto_project_id}/branches/production"
branch_id = branch_name.split("/")[-1]

# List endpoints to find the default read-write endpoint
endpoints = list(w.postgres.list_endpoints(parent=f"projects/{auto_project_id}/branches/{branch_id}"))
print(f"  Found {len(endpoints)} endpoint(s)")

if endpoints:
    ep = endpoints[0]
    ep_name = ep.name  # full resource name
    print(f"  Updating endpoint '{ep_name}' with autoscale min=0.5 CU, max=1 CU, scale-to-zero...")

    # Update endpoint with autoscaling limits and suspend timeout (scale-to-zero)
    w.postgres.update_endpoint(
        name=ep_name,
        endpoint=Endpoint(
            spec=EndpointSpec(
                endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                autoscaling_limit_min_cu=0.5,
                autoscaling_limit_max_cu=1.0,
                # Scale-to-zero: suspend after 5 minutes of inactivity
                suspend_timeout_duration=Duration(seconds=300),
            )
        ),
        update_mask=FieldMask(field_mask=["spec.autoscaling_limit_min_cu", "spec.autoscaling_limit_max_cu", "spec.suspension"]),
    ).wait()
    print(f"  ✅ Autoscale configured: min 0.5 CU, max 1 CU, scale-to-zero enabled (5 min timeout)")
else:
    print("  ⚠️ No endpoints found — the project may still be provisioning. Retry shortly.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What's encrypted with your CMK now
# MAGIC
# MAGIC | Item | CMK-encrypted? |
# MAGIC |---|---|
# MAGIC | All Lakebase Postgres data pages on object storage | ✅ |
# MAGIC | Page server's snapshot deltas | ✅ |
# MAGIC | WAL records before they're synced to storage | ✅ |
# MAGIC | Backup snapshots | ✅ |
# MAGIC | Replication logs to the secondary | ✅ |
# MAGIC | In-flight TLS connections (separate concern, always-on) | TLS, not CMK |
# MAGIC | Postgres buffer cache (volatile RAM) | Not at rest — RAM only |

# COMMAND ----------

# MAGIC %md
# MAGIC ## F · Validate that data is *actually* encrypted with your key
# MAGIC
# MAGIC Two ways to validate:
# MAGIC
# MAGIC ### F.1 — KMS audit log (best evidence)
# MAGIC
# MAGIC Query CloudTrail (AWS) / Activity Log (Azure) / Cloud Audit Logs (GCP) for **`Decrypt`** events on your CMK with principal = Databricks managed-services SA:
# MAGIC
# MAGIC ```bash
# MAGIC # AWS — find recent decrypts on the CMK
# MAGIC aws cloudtrail lookup-events \
# MAGIC   --lookup-attributes AttributeKey=ResourceName,AttributeValue=<key-arn> \
# MAGIC   --max-results 50 \
# MAGIC   --region us-west-2 \
# MAGIC   | jq '.Events[] | {time: .EventTime, name: .EventName, principal: .Username, source: .EventSource}'
# MAGIC ```
# MAGIC
# MAGIC You should see `Decrypt` events from `databricks-managed-storage` or similar principals after the Lakebase instance creation. **Absence of this evidence means CMK is not actually being used.**
# MAGIC
# MAGIC ### F.2 — Revoke and observe break (quick smoke test)
# MAGIC
# MAGIC The most definitive validation is to **revoke key access and confirm Lakebase breaks**. We do this drill in section H — but if you want to sanity-check now, run a write to the new instance and see it succeed:

# COMMAND ----------

# DBTITLE 1,Untitled
import os
import time
import psycopg
from sqlalchemy import create_engine, text
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
# This is a Postgres API autoscaling project — use w.postgres, not the legacy database instances API
read_write_dns = ep.status.hosts.host

# Generate an OAuth token scoped to this endpoint
cred = w.postgres.generate_database_credential(endpoint=ep_name)
token = cred.token

dsn = (f"postgresql://{w.current_user.me().user_name}:{token}"
       f"@{read_write_dns}:5432/databricks_postgres?sslmode=require")

engine = create_engine(dsn)
with engine.begin() as cn:
    cn.execute(text("CREATE TABLE IF NOT EXISTS cmk_check (id int, payload text)"))
    cn.execute(text("INSERT INTO cmk_check VALUES (1, 'encrypted with CMK')"))
    rows = cn.execute(text("SELECT * FROM cmk_check")).all()
    print(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## G · Key rotation
# MAGIC
# MAGIC Rotation comes in two flavors:
# MAGIC
# MAGIC | Type | Frequency | What changes | Lakebase impact |
# MAGIC |---|---|---|---|
# MAGIC | **Automatic key material rotation** (KMS-managed) | AWS: 1y default. Azure: configurable. GCP: configurable. | New key version inside the same KMS key ID | Zero-downtime; transparent. Database keeps running. |
# MAGIC | **Manual key replacement** (point a workspace at a different key) | On-demand; rare | The KEK ARN/URI itself changes | Workspace re-keys its data encryption keys; brief reconfiguration. |
# MAGIC
# MAGIC ### Test: trigger a rotation (AWS example)
# MAGIC
# MAGIC ```bash
# MAGIC # Manually trigger a rotation
# MAGIC aws kms enable-key-rotation --key-id <key-id> --region us-west-2
# MAGIC
# MAGIC # Or rotate immediately (creates a new key version)
# MAGIC aws kms rotate-key-on-demand --key-id <key-id> --region us-west-2
# MAGIC ```
# MAGIC
# MAGIC Then check rotation history:
# MAGIC
# MAGIC ```bash
# MAGIC aws kms list-key-rotations --key-id <key-id> --region us-west-2
# MAGIC ```
# MAGIC
# MAGIC ### What to verify post-rotation
# MAGIC
# MAGIC 1. New `Decrypt` events in CloudTrail using the new key version
# MAGIC 2. Lakebase queries continue working (run the SELECT above)
# MAGIC 3. **No data re-encryption is required** — only the wrapped DEKs are re-wrapped under the new KEK version

# COMMAND ----------

# MAGIC %md
# MAGIC ## H · 🚨 Revocation drill (lab-safe)
# MAGIC
# MAGIC This is the most important exercise in the module. **In production, revocation is your kill-switch.** Practice it now in a controlled way.
# MAGIC
# MAGIC ### Goal
# MAGIC
# MAGIC Confirm that pulling key access immediately stops Lakebase data access — and that restoring the access brings it back.
# MAGIC
# MAGIC ### Procedure
# MAGIC
# MAGIC **Step 1 — Take a successful baseline read**

# COMMAND ----------

with engine.begin() as cn:
    rows = cn.execute(text("SELECT id, payload FROM cmk_check")).all()
    print("Baseline read OK:", rows)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2 — Disable the key (DO NOT delete!)**
# MAGIC
# MAGIC > **⚠️ WARNING:** Use *disable*, not *schedule for deletion*. Disable is reversible. Deletion has a 7–30 day waiting period that you cannot shortcut, and after that the key — and your data — are unrecoverable.
# MAGIC
# MAGIC ```bash
# MAGIC # AWS
# MAGIC aws kms disable-key --key-id <key-id> --region us-west-2
# MAGIC
# MAGIC # Azure
# MAGIC az keyvault key set-attributes --name lakebase-cmk \
# MAGIC   --vault-name databricks-lakebase-kv --enabled false
# MAGIC
# MAGIC # GCP — disable the primary version
# MAGIC gcloud kms keys versions disable <version> \
# MAGIC   --key lakebase-cmk \
# MAGIC   --keyring databricks-lakebase \
# MAGIC   --location <region>
# MAGIC ```
# MAGIC
# MAGIC **Step 3 — Wait ~5 minutes for caches to invalidate, then attempt a read.**
# MAGIC
# MAGIC The buffer cache may still hold pages briefly. Once it cycles, requests requiring decryption fail.

# COMMAND ----------

# After disabling the key in your cloud console, wait ~5 min, then run:
import time
time.sleep(300)  # 5 min — adjust as needed

try:
    with engine.begin() as cn:
        rows = cn.execute(text("SELECT id, payload FROM cmk_check")).all()
        print("UNEXPECTED success — try again in another minute:", rows)
except Exception as e:
    print(f"Expected error after key disabled: {type(e).__name__}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4 — Re-enable the key**
# MAGIC
# MAGIC ```bash
# MAGIC # AWS
# MAGIC aws kms enable-key --key-id <key-id> --region us-west-2
# MAGIC
# MAGIC # Azure
# MAGIC az keyvault key set-attributes --name lakebase-cmk \
# MAGIC   --vault-name databricks-lakebase-kv --enabled true
# MAGIC
# MAGIC # GCP
# MAGIC gcloud kms keys versions enable <version> \
# MAGIC   --key lakebase-cmk \
# MAGIC   --keyring databricks-lakebase \
# MAGIC   --location <region>
# MAGIC ```
# MAGIC
# MAGIC **Step 5 — Verify recovery (within ~2 min)**

# COMMAND ----------

import time
time.sleep(120)

with engine.begin() as cn:
    rows = cn.execute(text("SELECT id, payload FROM cmk_check")).all()
    print("Recovery confirmed:", rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What you've proven
# MAGIC
# MAGIC - **Revocation works** — pulling the KMS key immediately denies access
# MAGIC - **Recovery works** — restoring access resumes operation without data corruption
# MAGIC - **No Databricks back-door** — Databricks cannot decrypt your data without your KMS key
# MAGIC
# MAGIC ### Operational implications
# MAGIC
# MAGIC Document a **runbook** for your team that includes:
# MAGIC - Who can disable/re-enable the key (least-privilege; usually security team only)
# MAGIC - Notification path when revocation is required
# MAGIC - Communication plan to app owners (their queries will fail)
# MAGIC - Test the runbook quarterly via this same drill

# COMMAND ----------

# MAGIC %md
# MAGIC ## I · 💀 What if you actually lose key access?
# MAGIC
# MAGIC The disaster scenario: KMS key is deleted (not just disabled), or your AWS account is compromised and recovery is impossible.
# MAGIC
# MAGIC ### Prevention is everything
# MAGIC
# MAGIC | Control | Rationale |
# MAGIC |---|---|
# MAGIC | **Enable purge protection** on the KMS key (AWS: nothing to enable — schedule-deletion has 7-30 day waiting period; Azure: explicit; GCP: explicit) | 7-30 day window to undo accidental deletion |
# MAGIC | **Multi-region replica key** (AWS) | If the primary region is unavailable, replica key in another region keeps decryption possible |
# MAGIC | **Separate IAM roles for key admin vs key user** | The role that can encrypt/decrypt is *not* the role that can delete |
# MAGIC | **Restrict `kms:ScheduleKeyDeletion`** to a separate break-glass role with MFA | Reduces accidental human error |
# MAGIC | **Backup the data outside CMK** | Periodically export Lakebase data via Federation or Sync to a separate Delta Lake catalog with its own encryption — gives you a recovery option |
# MAGIC
# MAGIC ### If the worst happens
# MAGIC
# MAGIC | If the key is… | Recovery? |
# MAGIC |---|---|
# MAGIC | Disabled | Re-enable. Full recovery. |
# MAGIC | Scheduled for deletion (within waiting period) | Cancel the deletion. Full recovery. |
# MAGIC | Permanently deleted | **No recovery from CMK.** Restore from external backups (Sync to Delta) if you have them. |
# MAGIC | Deleted AND no external backups | **Data is permanently lost.** This is by design. |
# MAGIC
# MAGIC The single most important production checklist item: **also Sync your critical Lakebase tables to a Delta Lake catalog encrypted with a different key**. Module 06 covers that pattern.

# COMMAND ----------

# MAGIC %md
# MAGIC ## J · Production checklist
# MAGIC
# MAGIC Before going to production with CMK on Lakebase:
# MAGIC
# MAGIC - [ ] KMS key created with appropriate naming, tags, and labels for cost allocation
# MAGIC - [ ] Key policy follows least-privilege; **`kms:Delete*` excluded** from Databricks principal
# MAGIC - [ ] Soft-delete + purge protection enabled (Azure/GCP) or 30-day schedule-deletion window confirmed (AWS)
# MAGIC - [ ] Multi-region replica configured if you're doing cross-region DR (AWS)
# MAGIC - [ ] Automatic rotation enabled (annual minimum for HIPAA/PCI; quarterly for FedRAMP)
# MAGIC - [ ] CloudTrail / Activity Log / Audit Logs forwarded to your SIEM
# MAGIC - [ ] **Revocation drill scheduled** (quarterly) using the procedure in section H
# MAGIC - [ ] Runbook documented for: rotation, revocation, key admin handover
# MAGIC - [ ] **External backup pipeline** running (Sync critical tables to a separately-encrypted Delta catalog)
# MAGIC - [ ] Compliance evidence package: CloudTrail of `Decrypt` events on the CMK, key policy, IAM trail

# COMMAND ----------

# MAGIC %md
# MAGIC ## K · Cleanup
# MAGIC
# MAGIC Drop the demo instance. Note: this does NOT delete the KMS key — keep that for module 12 (Capstone).

# COMMAND ----------

# DBTITLE 1,Untitled
# auto_project_id is a Postgres API autoscaling project — use w.postgres, not the legacy database API
try:
    w.postgres.delete_project(name=f"projects/{auto_project_id}").wait()
    print(f"Deleted {auto_project_id}")
except Exception as e:
    if "not found" in str(e).lower():
        print(f"ℹ️  {auto_project_id} does not exist (already deleted).")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC **Next:** Proceed to **04 Demo - Connectivity & Security** for OAuth IAM tokens, PrivateLink, and IP allowlisting in depth.
# MAGIC
# MAGIC <details>
# MAGIC <summary>📚 Reference material</summary>
# MAGIC
# MAGIC - [Databricks docs — CMK for managed services](https://docs.databricks.com/security/customer-managed-keys/index.html)
# MAGIC - [Lakebase encryption notes](https://docs.databricks.com/lakebase/security.html)
# MAGIC - [AWS KMS best practices](https://docs.aws.amazon.com/kms/latest/developerguide/best-practices.html)
# MAGIC - [Azure Key Vault security](https://learn.microsoft.com/en-us/azure/key-vault/general/security-features)
# MAGIC - [GCP Cloud KMS rotation](https://cloud.google.com/kms/docs/rotating-keys)
# MAGIC
# MAGIC </details>