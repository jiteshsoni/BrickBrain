# Databricks notebook source
# MAGIC %pip install -qqqq databricks-vectorsearch

# COMMAND ----------

# MAGIC dbutils.library.restartPython()

# COMMAND ----------

########################################################################################################################
# Vector Search ingestion pipeline
# 
# inputs: 
# - vector_search_endpoint: endpoint
# - vector_search_index: name of index
# - preprocessed_data_table: name of delta table to write all tables to
# - source_tables_str: comma sep list of tables to union together. Enforced schema. 
#
########################################################################################################################

# COMMAND ----------


# DBTITLE 1,Define variables from parameters
vector_search_endpoint = dbutils.widgets.get("vector_search_endpoint")
vector_search_index = dbutils.widgets.get("vector_search_index")
preprocessed_data_table = dbutils.widgets.get("preprocessed_data_table")
source_tables_str = dbutils.widgets.get("source_tables")
source_tables = [table.strip() for table in source_tables_str.split(",") if table.strip()]

assert vector_search_endpoint, "Vector Search Endpoint is required"
assert vector_search_index, "Vector Search Index is required"
assert preprocessed_data_table, "Preprocessed Data Table is required"
assert len(source_tables) > 0, "Source Tables are required"

print(f"Vector Search Endpoint: {vector_search_endpoint}")
print(f"Vector Search Index: {vector_search_index}")
print(f"Preprocessed Data Table: {preprocessed_data_table}")
print(f"Number of Source Tables: {len(source_tables)}")

# COMMAND ----------

# DBTITLE 1, Union source tables and add unique IDs
from pyspark.sql import functions as F

def union_source_tables(source_tables):
    """Union all source tables by name and add a unique ID column."""
    # Start with the first table
    combined_df = spark.table(source_tables[0])
    
    # Union with remaining tables by name (handles column ordering automatically)
    for table in source_tables[1:]:
        combined_df = combined_df.unionByName(spark.table(table), allowMissingColumns=True)
    
    # Add unique ID column using monotonically_increasing_id
    combined_df = combined_df.withColumn("id", F.monotonically_increasing_id())
    
    return combined_df

# COMMAND ----------

combined_df = union_source_tables(source_tables)

# Write to Delta table with Change Data Feed enabled
combined_df.write \
    .mode('overwrite') \
    .option('delta.enableChangeDataFeed', 'true') \
    .saveAsTable(preprocessed_data_table)

# COMMAND ----------

# DBTITLE 1,Initialize endpoint
from databricks.vector_search.client import VectorSearchClient
import time

vsc = VectorSearchClient(disable_notice=True)

try:
    endpoint = vsc.get_endpoint(vector_search_endpoint)
except Exception:
    vsc.create_endpoint(name=vector_search_endpoint, endpoint_type="STANDARD")

endpoint = vsc.get_endpoint(vector_search_endpoint)
while endpoint['endpoint_status']['state'] not in ["ONLINE", "PROVISIONING"]:
    time.sleep(30)
    endpoint = vsc.get_endpoint(vector_search_endpoint)

print(f"Endpoint named {vector_search_endpoint} is ready.")

# COMMAND ----------

# DBTITLE 1,Drop and Recreate Index
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

# Drop existing index if it exists (since we're doing full overwrite of source table)
try:
    print(f"🗑️  Checking for existing index {vector_search_index}...")
    existing_index = vsc.get_index(vector_search_endpoint, vector_search_index)
    print(f"📍 Found existing index, deleting it...")
    vsc.delete_index(vector_search_index)
    print(f"✅ Existing index deleted")
except Exception as e:
    if "does not exist" in str(e).lower() or "not found" in str(e).lower():
        print(f"ℹ️  No existing index to delete")
    else:
        print(f"⚠️  Error checking/deleting index: {str(e)[:200]}")

# Create fresh index
print(f"🆕 Creating fresh index {vector_search_index}...")
try:
    vsc.create_delta_sync_index(
        endpoint_name=vector_search_endpoint,
        index_name=vector_search_index,
        source_table_name=preprocessed_data_table,
        pipeline_type="TRIGGERED",
        primary_key="id",
        embedding_source_column="content", # The column containing our text
        embedding_model_endpoint_name="databricks-gte-large-en" # The embedding endpoint used to create the embeddings
    )
    print(f"✅ Index created successfully")
except Exception as e:
    # Rare race condition: index was created between delete and create
    if "RESOURCE_ALREADY_EXISTS" in str(e):
        print(f"⚠️  Index already exists (concurrent creation), will use existing index")
    else:
        raise

# COMMAND ----------

# DBTITLE 1,Test if Index Online
import databricks 
import time

vector_index = vsc.get_index(endpoint_name=vector_search_endpoint, index_name=vector_search_index)

try:
    # Try a simple similarity search to test if the index is working
    results = vector_index.similarity_search(
        query_text="recommendations spark structured streaming",
        columns=["id", "content", "primary_url", "content_type"],
        num_results=3
    )
    print("Sample search results:")
    for result in results['result']['data_array'][:2]:  # Show first 2 results
        print(f"  - {result[3]} ({result[2]}) -> {result[1]}")  # title and source_type
except Exception as e:
    print("Index created but may still be syncing. Try testing again in a few minutes.")
    
# COMMAND ----------

