# Databricks notebook source

########################################################################################################################
# Knowledge Assistant Sync Task
# 
# This task triggers a sync of the Knowledge Assistant after vector index updates.
# 
# Widget Parameters:
# - knowledge_assistant_id: Knowledge Assistant ID to sync
#
# Purpose:
# - Ensures the Knowledge Assistant reflects the latest indexed data
# - Runs as final step in data ingestion pipeline
#
########################################################################################################################

# COMMAND ----------

# DBTITLE 1,Define parameters
dbutils.widgets.text("knowledge_assistant_id", "")

knowledge_assistant_id = dbutils.widgets.get("knowledge_assistant_id")

assert knowledge_assistant_id, "Knowledge Assistant ID is required"

print(f"Knowledge Assistant ID: {knowledge_assistant_id}")

# COMMAND ----------

# DBTITLE 1,Sync Knowledge Assistant
import requests

print("\n" + "="*70)
print("🔄 SYNCING KNOWLEDGE ASSISTANT")
print("="*70)

# Get workspace host and token
HOST = spark.conf.get("spark.databricks.workspaceUrl")
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Trigger Knowledge Assistant sync
url = f"https://{HOST}/api/2.0/knowledge-assistants/{knowledge_assistant_id}/sync-knowledge-sources"

try:
    print(f"\n📡 POST {url}")
    response = requests.post(
        url, 
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {TOKEN}"
        }
    )
    
    print(f"\n✅ Response Status: {response.status_code}")
    print(f"Response Body:\n{response.text}")
    
    if response.status_code == 200:
        print(f"\n🎉 Knowledge Assistant sync triggered successfully!")
    else:
        print(f"\n⚠️  Unexpected response code: {response.status_code}")
        raise RuntimeError(f"Knowledge Assistant sync failed with status {response.status_code}")
        
except Exception as e:
    print(f"\n❌ Error syncing Knowledge Assistant: {str(e)}")
    raise

print("="*70)

# COMMAND ----------


