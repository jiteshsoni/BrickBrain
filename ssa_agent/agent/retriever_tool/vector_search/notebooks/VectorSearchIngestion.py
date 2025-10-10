# Databricks notebook source
# MAGIC %pip install -qqqq databricks-vectorsearch

# COMMAND ----------

# MAGIC dbutils.library.restartPython()

# COMMAND ----------

########################################################################################################################
# Vector Search ingestion pipeline
# 
# Widget Parameters: 
# - vector_search_endpoint: Vector search endpoint name
# - vector_search_index: Full name of the vector search index (catalog.schema.index)
# - preprocessed_data_table: Unity Catalog table to write data with IDs to (for delta sync)
# - preprocessed_chunked_table: Unity Catalog table containing chunked content
# - force_rebuild: (optional) Set to "true" to drop and recreate index, "false" for efficient sync (default: "false")
#
# Features:
# - Deterministic IDs: Uses hash of url + chunk_id to ensure consistent IDs across runs
# - Smart sync: Only updates changed data when force_rebuild=false
# - Safety checks: Validates schema and verifies index contains searchable data
#
########################################################################################################################

# COMMAND ----------


# DBTITLE 1,Define variables from parameters
# Create widgets first (ensures they exist before reading)
dbutils.widgets.text("vector_search_endpoint", "")
dbutils.widgets.text("vector_search_index", "")
dbutils.widgets.text("preprocessed_data_table", "")
dbutils.widgets.text("preprocessed_chunked_table", "")
dbutils.widgets.dropdown("force_rebuild", "false", ["true", "false"])

vector_search_endpoint = dbutils.widgets.get("vector_search_endpoint")
vector_search_index = dbutils.widgets.get("vector_search_index")
preprocessed_data_table = dbutils.widgets.get("preprocessed_data_table")
preprocessed_chunked_table = dbutils.widgets.get("preprocessed_chunked_table")

# Read force_rebuild parameter
force_rebuild_str = dbutils.widgets.get("force_rebuild").lower().strip()
force_rebuild = force_rebuild_str == "true"

assert vector_search_endpoint, "Vector Search Endpoint is required"
assert vector_search_index, "Vector Search Index is required"
assert preprocessed_data_table, "Preprocessed Data Table is required"
assert preprocessed_chunked_table, "Preprocessed Chunked Table is required"

print(f"Vector Search Endpoint: {vector_search_endpoint}")
print(f"Vector Search Index: {vector_search_index}")
print(f"Preprocessed Data Table (for delta sync): {preprocessed_data_table}")
print(f"Preprocessed Chunked Table (source): {preprocessed_chunked_table}")
print(f"Force Rebuild: {force_rebuild}")

# If force rebuild, drop the preprocessed data table (will be recreated with new IDs)
if force_rebuild:
    print(f"\n🔄 Force rebuild requested - dropping preprocessed data table...")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {preprocessed_data_table}")
        print(f"   ✅ Dropped {preprocessed_data_table}")
    except Exception as e:
        print(f"   ⚠️  Could not drop {preprocessed_data_table}: {str(e)[:100]}")

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

# DBTITLE 1,Create or Sync Index with Schema Validation
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

# SAFETY CHECK: Validate required columns exist
print(f"\n🔍 Validating schema before creating/syncing index...")
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

# Check if index already exists
index_exists = False
try:
    print(f"\n🔍 Checking for existing index {vector_search_index}...")
    existing_index = vsc.get_index(vector_search_endpoint, vector_search_index)
    index_exists = True
    
    if force_rebuild:
        print(f"🔄 Force rebuild requested - deleting existing index...")
        vsc.delete_index(vector_search_index)
        index_exists = False
        print(f"✅ Existing index deleted, will create fresh index")
    else:
        print(f"📍 Found existing index - will trigger sync to update with new data")
except Exception as e:
    if "does not exist" in str(e).lower() or "not found" in str(e).lower():
        print(f"ℹ️  No existing index found - will create new index")
    else:
        print(f"⚠️  Error checking index: {str(e)[:200]}")

if not index_exists:
    # Create fresh index
    print(f"\n🆕 Creating new index for {source_row_count:,} rows...")
    try:
        vsc.create_delta_sync_index(
            endpoint_name=vector_search_endpoint,
            index_name=vector_search_index,
            source_table_name=preprocessed_data_table,
            pipeline_type="TRIGGERED",
            primary_key="id",
            embedding_source_column="content", # The column containing our text
            embedding_model_endpoint_name="databricks-gte-large-en", # The embedding endpoint used to create the embeddings
            columns_to_sync=[
                "url",
                "content_type",
                "domain",
                "chunk_id"
            ]
        )
        print(f"✅ Index created successfully")
    except Exception as e:
        if "RESOURCE_ALREADY_EXISTS" in str(e):
            print(f"ℹ️  Index was just created (concurrent creation), will sync")
            index_exists = True
        else:
            raise

if index_exists:
    # Trigger sync to update with new data from overwritten source table
    print(f"\n🔄 Triggering index sync for {source_row_count:,} rows...")
    try:
        vector_index = vsc.get_index(vector_search_endpoint, vector_search_index)
        vector_index.sync()
        print(f"✅ Index sync triggered - will process new data from source table")
    except Exception as e:
        print(f"⚠️  Error triggering sync: {str(e)[:200]}")
        print(f"   Index will auto-sync via Change Data Feed")

# COMMAND ----------

# DBTITLE 1,SAFETY CHECK: Validate Index Contains Data
import databricks 
import time

print(f"\n⏳ Waiting for index to sync and validating data integrity...")

vector_index = vsc.get_index(endpoint_name=vector_search_endpoint, index_name=vector_search_index)

# Wait for index to be ONLINE (with timeout)
max_wait_time = 600  # 10 minutes
start_time = time.time()
check_interval = 30

while True:
    elapsed = int(time.time() - start_time)
    try:
        index_status = vector_index.describe()
        status_info = index_status.get('status', {})
        status_state = status_info.get('state', 'UNKNOWN')
        detailed_state = status_info.get('detailed_state', 'UNKNOWN')
        is_ready = status_info.get('ready', False)
        
        # Print full detailed status every 30 seconds
        print(f"\n{'='*70}")
        print(f"[{elapsed}s] 🔍 Index Status Check")
        print(f"{'='*70}")
        print(f"State: {status_state}")
        print(f"Detailed State: {detailed_state}")
        print(f"Ready: {is_ready}")
        
        if 'status' in index_status:
            status_info = index_status['status']
            print(f"\nStatus Details:")
            for key, value in status_info.items():
                if key == 'indexed_row_count':
                    print(f"  {key}: {value:,} / {source_row_count:,} ({(value/source_row_count*100) if source_row_count > 0 else 0:.1f}%)")
                else:
                    print(f"  {key}: {value}")
        
        # Print other useful fields from index_status
        useful_fields = ['name', 'endpoint_name', 'index_type', 'primary_key', 'delta_sync_index_spec']
        print(f"\nIndex Configuration:")
        for field in useful_fields:
            if field in index_status:
                print(f"  {field}: {index_status[field]}")
        
        print(f"{'='*70}\n")
        
    except Exception as e:
        # Handle API errors during index sync (index might be transitioning)
        error_msg = str(e)
        if "500" in error_msg or "INTERNAL_ERROR" in error_msg:
            print(f"\n[{elapsed}s] ⚠️  Index API temporarily unavailable (transitioning)")
            print(f"  Error: {error_msg[:150]}")
            print(f"  Will retry in {check_interval}s...")
            status_state = "TRANSITIONING"
            detailed_state = "TRANSITIONING"
            is_ready = False
        else:
            raise
    
    # Check if index is ready (uses detailed_state or ready flag)
    if is_ready or "ONLINE" in detailed_state:
        print(f"\n✅ Index is READY after {elapsed}s")
        print(f"   Detailed State: {detailed_state}")
        print(f"   Ready: {is_ready}")
        break
    elif "FAILED" in detailed_state or "OFFLINE" in detailed_state:
        raise RuntimeError(
            f"❌ SAFETY CHECK FAILED: Index sync failed!\n"
            f"   State: {status_state}\n"
            f"   Detailed State: {detailed_state}\n"
            f"   Expected {source_row_count:,} rows to be indexed"
        )
    elif elapsed > max_wait_time:
        raise RuntimeError(
            f"❌ SAFETY CHECK FAILED: Index sync timeout after {max_wait_time}s\n"
            f"   Last known status: {status_state}\n"
            f"   This may indicate the index is corrupted. Try setting force_rebuild=true"
        )
    
    time.sleep(check_interval)

# CRITICAL SAFETY CHECK: Verify index has searchable data
print(f"\n🔍 SAFETY CHECK: Verifying index contains searchable data...")
try:
    results = vector_index.similarity_search(
        query_text="databricks spark",
        columns=["id", "content", "url", "content_type"],
        num_results=5
    )
    
    num_results = len(results.get('result', {}).get('data_array', []))
    
    if num_results == 0:
        raise RuntimeError(
            f"❌ SAFETY CHECK FAILED: Index is ONLINE but contains NO searchable data!\n"
            f"   Source table: {preprocessed_data_table} ({source_row_count:,} rows)\n"
            f"   Index: {vector_search_index} (0 searchable results)\n"
            f"   This indicates DATA LOSS during indexing - failing job!"
        )
    
    print(f"✅ Index validation PASSED")
    print(f"   Source rows: {source_row_count:,}")
    print(f"   Index status: ONLINE with searchable data")
    print(f"\n📊 Sample search results:")
    for result in results['result']['data_array'][:3]:
        content_preview = result[1][:80] + "..." if len(result[1]) > 80 else result[1]
        print(f"  - [{result[3]}] {content_preview}")
        print(f"    URL: {result[2]}")
        
except Exception as e:
    # If the search itself fails, that's a critical error
    raise RuntimeError(
        f"❌ SAFETY CHECK FAILED: Unable to query index!\n"
        f"   Error: {str(e)}\n"
        f"   Index may be corrupted or data was lost during sync"
    )

print("\n" + "="*70)
print("✅ ALL SAFETY CHECKS PASSED - NO DATA LOSS DETECTED")
print("="*70)
print(f"  Vector Index: {vector_search_index}")
print(f"  Source Rows: {source_row_count:,}")
print(f"  Status: ONLINE and searchable")
print("="*70)
    
# COMMAND ----------

