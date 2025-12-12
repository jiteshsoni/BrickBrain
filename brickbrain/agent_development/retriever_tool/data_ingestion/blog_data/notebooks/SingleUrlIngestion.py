# Databricks notebook source
# MAGIC %pip install -qqqq requests beautifulsoup4 html2text lxml urllib3 langchain transformers
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

########################################################################################################################
# Single Blog URL Scraper and Ingestion Pipeline
# 
# This pipeline scrapes a single blog URL and chunks it immediately for vector search.
# The blog post is converted to markdown and then chunked by headers and token size.
#
# inputs: 
# - blog_url: URL of the blog post to scrape
# - raw_blog_content_table: name of the delta table to save the raw blog content
# - preprocessed_blogs_table: name of the delta table to save the chunked data
# - chunk_size: maximum tokens per chunk (default: 500)
# - chunk_overlap: overlapping tokens between chunks (default: 50)
#
########################################################################################################################

# COMMAND ----------

# Get parameters
blog_url = dbutils.widgets.get("blog_url")
raw_blog_content_table = dbutils.widgets.get("raw_blog_content_table")
preprocessed_blogs_table = dbutils.widgets.get("preprocessed_blogs_table")
chunk_size = int(dbutils.widgets.get("chunk_size") or "500")
chunk_overlap = int(dbutils.widgets.get("chunk_overlap") or "50")

print(f"Processing blog URL: {blog_url}")
print(f"Raw content table: {raw_blog_content_table}")
print(f"Preprocessed table: {preprocessed_blogs_table}")
print(f"Chunk size: {chunk_size}, Overlap: {chunk_overlap}")

# COMMAND ----------

import sys
import os
import logging
from typing import List, Dict, Any
import uuid
from datetime import datetime
from urllib.parse import urlparse

# Add the bundle root to the path
bundle_root = "/Workspace"
sys.path.append(bundle_root)

from ssa_agent.agent.retriever_tool.data_ingestion.blog_data.utils import BlogScraper, Chunker

from pyspark.sql.functions import col, explode, pandas_udf, lit
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from pyspark.sql import functions as F
import pandas as pd

logger = logging.getLogger(__name__)

# COMMAND ----------

# Validate the URL is Medium or Substack
def validate_blog_url(url):
    """Validate that the URL is from Medium or Substack"""
    parsed_url = urlparse(url)
    domain = parsed_url.netloc.lower()
    
    if 'medium.com' in domain or 'substack.com' in domain:
        return True
    else:
        raise ValueError(f"URL must be from Medium or Substack. Got: {domain}")

validate_blog_url(blog_url)
print(f"✅ Valid blog URL: {blog_url}")

# COMMAND ----------

# Initialize the blog scraper for single URL
scraper = BlogScraper([])  # Empty base_urls since we're processing a single URL

# Download and convert the single blog post
print(f"Scraping blog post: {blog_url}")
blog_data = scraper.download_and_convert_to_markdown(blog_url)

if not blog_data['success']:
    raise Exception(f"Failed to scrape blog post: {blog_data['metadata'].get('error', 'Unknown error')}")

print(f"✅ Successfully scraped: {blog_data['metadata']['title']}")

# Create DataFrame with the single blog post
blog_posts_data = [{
    "url": blog_data['metadata']['url'],
    "title": blog_data['metadata']['title'],
    "markdown_content": blog_data['content'],
    "domain": blog_data['metadata']['domain'],
    "scraped_at": blog_data['metadata']['scraped_at']
}]

blogs_df = spark.createDataFrame(blog_posts_data)

# Save to raw content table
blogs_df.write.mode('append').saveAsTable(raw_blog_content_table)
print(f"✅ Saved raw content to table: {raw_blog_content_table}")

# Display the raw data
print("Raw blog data:")
blogs_df.display()

# COMMAND ----------

# Initialize chunker and process the content
chunker = Chunker(max_chunk_size=chunk_size, chunk_overlap=chunk_overlap)

@pandas_udf("array<string>")
def parse_and_split(content: pd.Series) -> pd.Series: 
    """Chunk blog content with metadata."""
    return content.apply(lambda text: chunker.chunk_document(text))

# Process the blog content into chunks
preprocessed_blogs = (blogs_df.select(
    F.posexplode(parse_and_split('markdown_content')).alias('chunk_id', 'content'), 
    "domain", 
    "url",
    "title"
))

# Add additional metadata
preprocessed_blogs = preprocessed_blogs.select(
    col("chunk_id"),
    col("url").alias("primary_url"),
    lit("blog").alias("content_type"),
    col("domain"),
    col("content"),
    col("title")
)

# Save to preprocessed table
preprocessed_blogs.write.mode('append').saveAsTable(preprocessed_blogs_table)
print(f"✅ Saved {preprocessed_blogs.count()} chunks to table: {preprocessed_blogs_table}")

# Display the processed chunks
print("Processed chunks:")
preprocessed_blogs.display()

# COMMAND ----------

# DBTITLE 1,Trigger Vector Search Sync
from databricks.vector_search.client import VectorSearchClient

# Initialize Vector Search client
vsc = VectorSearchClient(disable_notice=True)

# Configuration - these should match your Vector Search setup
vector_search_endpoint = "brickbrain"
vector_search_index = "veena_ramesh_test.data.brickbrain_index"

try:
    # Get the vector search index
    vector_index = vsc.get_index(endpoint_name=vector_search_endpoint, index_name=vector_search_index)
    
    # Trigger sync to update the index with new data
    vector_index.sync()
    print(f"✅ Triggered Vector Search sync for index: {vector_search_index}")
    
except Exception as e:
    print(f"⚠️ Warning: Could not trigger Vector Search sync: {e}")
    print("The content has been added to the Delta table and will be picked up in the next scheduled sync.")

# COMMAND ----------

print(f"""
🎉 Blog scraping and ingestion completed successfully!

📄 Blog Post: {blog_data['metadata']['title']}
🔗 URL: {blog_url}
📊 Chunks Created: {preprocessed_blogs.count()}
💾 Raw Data Table: {raw_blog_content_table}
🔍 Processed Data Table: {preprocessed_blogs_table}
🔍 Vector Search Index: {vector_search_index}

The content is now available for search and retrieval in BrickBrain!
""")

# COMMAND ----------

