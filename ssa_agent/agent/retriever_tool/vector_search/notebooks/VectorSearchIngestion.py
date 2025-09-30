# Databricks notebook source
# MAGIC %pip install -qqqq databricks-vectorsearch
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

# DBTITLE 1, Enforce source tables are same schema 
from pyspark.sql.types import StructType, StructField, LongType, StringType

SCHEMA = StructType([
    StructField('id', LongType(), False),  # False = not nullable (identity column)
    StructField('chunk_id', LongType(), True),  
    StructField('primary_url', StringType(), True),
    StructField('content_type', StringType(), True),
    StructField('domain', StringType(), True),
    StructField('content', StringType(), True)
])

def enforce_schema(source_tables):
    for table in source_tables:
        df = spark.table(table)
        if df.schema != SCHEMA:
            raise ValueError(f"Table {table} schema does not match expected schema")

def union_source_tables(source_tables):
    df = spark.createDataFrame([], SCHEMA)
    for table in source_tables:
        df = df.unionAll(spark.table(table))
    return df

def create_delta_table(preprocessed_data_table):
    if not spark.catalog.tableExists(f"{preprocessed_data_table}") or spark.table(f"{preprocessed_data_table}").isEmpty():
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {preprocessed_data_table} (
            id BIGINT GENERATED ALWAYS AS IDENTITY,
            chunk_id BIGINT,
            primary_url STRING,
            content_type STRING, 
            domain STRING, 
            content STRING
        )
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)

# COMMAND ----------

enforce_schema(source_tables)
combined_df = union_source_tables(source_tables)
create_delta_table(preprocessed_data_table)
combined_df.write.mode('overwrite').saveAsTable(preprocessed_data_table)

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

# DBTITLE 1,Create Index
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

try:
    existing_index = vsc.get_index(vector_search_endpoint, vector_search_index)
    existing_index.sync()
except Exception:
    vsc.create_delta_sync_index(
        endpoint_name=vector_search_endpoint,
        index_name=vector_search_index,
        source_table_name=preprocessed_data_table,
        pipeline_type="TRIGGERED",
        primary_key="id",
        embedding_source_column="content", # The column containing our text
        embedding_model_endpoint_name="databricks-gte-large-en" # The embedding endpoint used to create the embeddings
    )

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

