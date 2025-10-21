# Databricks notebook source
# MAGIC %pip install -qqqq databricks-vectorsearch

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

########################################################################################################################
# Vector Search ingestion pipeline
# 
# Widget Parameters: 
# - vector_search_endpoint: Vector search endpoint name
# - vector_search_index: Full name of the vector search index (catalog.schema.index)
# - preprocessed_data_table: Unity Catalog table to write data with IDs to (for delta sync)
# - preprocessed_chunked_table: Unity Catalog table containing chunked content
#
# Features:
# - Always rebuilds: Drops and recreates index on every run for predictable results
# - Deterministic IDs: Uses hash of url + chunk_id to ensure consistent IDs across runs
# - Safety checks: Validates schema before index creation
#
########################################################################################################################

# COMMAND ----------

# DBTITLE 1,Define variables from parameters

# Create widgets with production defaults (can be overridden by job parameters)
dbutils.widgets.text("vector_search_endpoint", "brickbrain")
dbutils.widgets.text("vector_search_index", "brickbrain.default.brickbrain_index")
dbutils.widgets.text("preprocessed_data_table", "brickbrain.default.brickbrain_delta_table")
dbutils.widgets.text("preprocessed_chunked_table", "brickbrain.default.preprocessed_content_chunked")

vector_search_endpoint = dbutils.widgets.get("vector_search_endpoint")
vector_search_index = dbutils.widgets.get("vector_search_index")
preprocessed_data_table = dbutils.widgets.get("preprocessed_data_table")
preprocessed_chunked_table = dbutils.widgets.get("preprocessed_chunked_table")

assert vector_search_endpoint, "Vector Search Endpoint is required"
assert vector_search_index, "Vector Search Index is required"
assert preprocessed_data_table, "Preprocessed Data Table is required"
assert preprocessed_chunked_table, "Preprocessed Chunked Table is required"

print(f"Vector Search Endpoint: {vector_search_endpoint}")
print(f"Vector Search Index: {vector_search_index}")
print(f"Preprocessed Data Table (for delta sync): {preprocessed_data_table}")
print(f"Preprocessed Chunked Table (source): {preprocessed_chunked_table}")
print(f"Mode: ALWAYS REBUILD (drops and recreates index)")

# Set current catalog and database using Spark conf to avoid Hive Metastore errors
catalog_name = preprocessed_data_table.split('.')[0]
schema_name = preprocessed_data_table.split('.')[1]
print(f"\n🔧 Setting Spark configuration for Unity Catalog")
print(f"   Catalog: {catalog_name}, Schema: {schema_name}")

# Set via Spark SQL configuration (more reliable with Spark Connect)
spark.sql(f"USE CATALOG `{catalog_name}`")
spark.sql(f"USE SCHEMA `{schema_name}`")

# Verify settings
current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
current_schema = spark.sql("SELECT current_schema()").collect()[0][0]
print(f"   ✅ Current catalog: {current_catalog}")
print(f"   ✅ Current schema: {current_schema}")

# Always drop the preprocessed data table (will be recreated with fresh data)
print(f"\n🔄 Dropping preprocessed data table for fresh rebuild...")
spark.sql(f"DROP TABLE IF EXISTS {preprocessed_data_table}")
print(f"   ✅ Dropped {preprocessed_data_table}")

# COMMAND ----------

# DBTITLE 1, Read chunked data and add deterministic IDs
from pyspark.sql import functions as F

# Read chunked content (already unified and chunked)
combined_df = spark.table(preprocessed_chunked_table)

print(f"Read {combined_df.count()} chunks from {preprocessed_chunked_table}")

# Add deterministic ID based on url + chunk_id hash
# This ensures same content chunk gets same ID across runs (prevents duplicates in index)
# Each URL may have multiple chunks with different chunk_ids
combined_df = combined_df.withColumn(
    "id", 
    F.abs(F.hash(F.concat(F.col("url"), F.lit("_"), F.col("chunk_id")))).cast("bigint")
)

print(f"✅ Added deterministic IDs based on hash(url + chunk_id)")

# COMMAND ----------

# Count rows in source for later validation
source_row_count = combined_df.count()
print(f"📊 Total rows to index: {source_row_count:,}")

# Write to Delta table with Change Data Feed enabled and schema overwrite
combined_df.write \
    .mode('overwrite') \
    .option('delta.enableChangeDataFeed', 'true') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(preprocessed_data_table)

print(f"✅ Wrote {source_row_count:,} rows to {preprocessed_data_table}")

# COMMAND ----------

# DBTITLE 1,Initialize endpoint
from databricks.vector_search.client import VectorSearchClient
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import EndpointType
import time

vsc = VectorSearchClient(disable_notice=True)
w = WorkspaceClient()

try:
    endpoint = vsc.get_endpoint(vector_search_endpoint)
    print(f"✅ Found existing endpoint: {vector_search_endpoint}")
    print(f"   Region: {endpoint.get('endpoint_status', {}).get('region', 'N/A')}")
except Exception as e:
    if "does not exist" in str(e).lower() or "not found" in str(e).lower():
        print(f"📍 Creating new endpoint: {vector_search_endpoint}")
        print(f"   Target region: us-west-2 (AWS Oregon)")
        
        # Create endpoint with explicit region specification
        # Note: Region is determined by the workspace location, but we can use SDK for better control
        try:
            w.vector_search_endpoints.create_endpoint(
                name=vector_search_endpoint,
                endpoint_type=EndpointType.STANDARD
            )
            print(f"✅ Endpoint created in workspace region (us-west-2)")
        except Exception as create_error:
            # Fallback to VectorSearchClient if SDK method fails
            print(f"   Falling back to VectorSearchClient...")
            vsc.create_endpoint(name=vector_search_endpoint, endpoint_type="STANDARD")
            print(f"✅ Endpoint created")
    else:
        raise RuntimeError(f"❌ Failed to get/create endpoint: {str(e)}")

endpoint = vsc.get_endpoint(vector_search_endpoint)
# IMPORTANT: Wait for endpoint to be ready for index operations
# Valid ready states: ONLINE, ONLINE_NO_PENDING_UPDATE, PROVISIONING_INITIAL_SNAPSHOT
# PROVISIONING state (without suffix) means endpoint is still being created
ready_states = ["ONLINE", "ONLINE_NO_PENDING_UPDATE", "PROVISIONING_INITIAL_SNAPSHOT"]
while endpoint['endpoint_status']['state'] not in ready_states:
    current_state = endpoint['endpoint_status']['state']
    print(f"   ⏳ Waiting for endpoint to be ready... (current state: {current_state})")
    time.sleep(30)
    endpoint = vsc.get_endpoint(vector_search_endpoint)

endpoint_state = endpoint['endpoint_status']['state']
print(f"✅ Endpoint {vector_search_endpoint} is ready (state: {endpoint_state})!")
print(f"   Region: {endpoint.get('endpoint_status', {}).get('region', 'Inherited from workspace')}")

# COMMAND ----------

# DBTITLE 1,Create Fresh Index with Schema Validation
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

# SAFETY CHECK: Validate required columns exist
print(f"\n🔍 Validating schema before creating index...")
required_columns = ["id", "content", "url", "content_type", "domain", "chunk_id"]
table_df = spark.table(preprocessed_data_table)
table_columns = table_df.columns

missing_columns = [col for col in required_columns if col not in table_columns]
if missing_columns:
    raise RuntimeError(
        f"❌ SAFETY CHECK FAILED: Missing required columns!\n"
        f"   Missing: {missing_columns}\n"
        f"   Available: {table_columns}"
    )

print(f"✅ Schema validation passed - all required columns present: {required_columns}")

# COMMAND ----------

import time

# Delete the index if it exists
print(f"\n🗑️  Deleting index {vector_search_index} (if exists)...")
try:
    vsc.delete_index(
        endpoint_name=vector_search_endpoint,
        index_name=vector_search_index
    )
    print(f"✅ Index deleted successfully")
except Exception as e:
    if "does not exist" in str(e).lower() or "not found" in str(e).lower():
        print(f"ℹ️  Index didn't exist - proceeding to create a new one")
    else:
        raise RuntimeError(f"❌ Failed to delete index: {str(e)}")

# Add a delay to ensure the deletion completes
time.sleep(10)  # Adjust the delay time as needed

# Create a fresh index
print(f"\n🆕 Creating a new index for {source_row_count:,} rows...")
vsc.create_delta_sync_index(
    endpoint_name=vector_search_endpoint,
    index_name=vector_search_index,
    source_table_name=preprocessed_data_table,
    pipeline_type="TRIGGERED",
    primary_key="id",
    embedding_source_column="content",
    embedding_model_endpoint_name="databricks-gte-large-en",
    columns_to_sync=["url", "content_type", "domain", "chunk_id"]
)
print(f"✅ Index created successfully - data syncing will start automatically")

# COMMAND ----------

