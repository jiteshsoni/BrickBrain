# Databricks notebook source
# MAGIC %pip install -qqqq requests beautifulsoup4 html2text lxml urllib3 feedparser langchain transformers tiktoken
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

########################################################################################################################
# Blog Scraper Data Ingestion Pipeline
# 
# This pipeline scrapes blog content and extracts it using LLM.
# Each blog post is converted to markdown and then cleaned by LLM.
#
# inputs: 
# - bundle_root: path to the bundle root
# - websites: comma-separated list of websites to scrape
# - raw_blog_content_table: name of the delta table to save the markdown data to
#
# Note: AI extraction and chunking are handled by the ChunkingTask notebook
########################################################################################################################

# COMMAND ----------

bundle_root = dbutils.widgets.get("bundle_root")
websites = dbutils.widgets.get("websites")
raw_blog_content_table = dbutils.widgets.get("raw_blog_content_table")

# Validate required parameters
assert bundle_root, "Bundle root is required"
assert websites, "Websites parameter is required"
assert raw_blog_content_table, "Raw blog content table is required"

print(f"Bundle root: {bundle_root}")
print(f"Websites: {websites}")
print(f"Raw table: {raw_blog_content_table}")

# Note: Catalog and schema should be created via setup_brickbrain_workspace.py before running this notebook

# COMMAND ----------

# Set default catalog to avoid Hive Metastore errors
catalog_name = raw_blog_content_table.split('.')[0]
schema_name = raw_blog_content_table.split('.')[1]
print(f"\n🔧 Setting default catalog: {catalog_name}")
print(f"🔧 Setting default schema: {schema_name}")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

import sys
import os
import logging
from typing import List, Dict, Any
import uuid
from datetime import datetime

sys.path.append(os.path.join(bundle_root, "agent"))

# Note: Chunker is not imported here as chunking is handled by separate ChunkingTask notebook
from retriever_tool.data_ingestion.blog_data.utils.blog_scraper import GetBlogs, BlogContentScraper

from pyspark.sql.functions import col, explode, pandas_udf, lit, expr
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from pyspark.sql import functions as F
import pandas as pd


logger = logging.getLogger(__name__)

# COMMAND ----------

# Discover all blog URLs from websites
discoverer = GetBlogs(websites)
all_urls = discoverer.discover_all_posts()

if not all_urls:
    logger.warning("No URLs discovered")
    dbutils.notebook.exit("No URLs discovered from websites")

print(f"Discovered {len(all_urls)} blog URLs from websites")

# Check for existing blogs to implement incremental ingestion
existing_urls = set()
try:
    print(f"Checking for existing blogs in {raw_blog_content_table}...")
    existing_df = spark.table(raw_blog_content_table)
    
    # Get all existing URLs
    url_rows = existing_df.select(col("url")).distinct().collect()
    existing_urls = set(row.url for row in url_rows if row.url)
    
    print(f"✅ Found {len(existing_urls)} unique blogs already in table.")
except Exception as e:
    print(f"Table {raw_blog_content_table} does not exist or is empty. Will create new table.")
    print(f"Details: {str(e)[:200]}")

# Filter out already scraped blogs
new_urls = []
skipped_urls = []

for url in all_urls:
    if url in existing_urls:
        skipped_urls.append(url)
    else:
        new_urls.append(url)

print(f"\n{'='*80}")
print(f"📊 Blog Ingestion Summary")
print(f"{'='*80}")
print(f"Total blogs discovered from websites: {len(all_urls)}")
print(f"  ✅ Already parsed (will skip):      {len(skipped_urls)}")
print(f"  🆕 New blogs (will scrape):         {len(new_urls)}")
print(f"{'='*80}\n")

# Show skipped blogs in detail
if skipped_urls:
    print(f"⏭️  Already Parsed Blogs ({len(skipped_urls)} total) - SKIPPING:")
    print(f"{'-'*80}")
    for i, url in enumerate(skipped_urls, 1):
        print(f"  {i}. {url}")
    print(f"{'-'*80}\n")

# Show new blogs to fetch in detail
if new_urls:
    print(f"🆕 New Blogs to Scrape ({len(new_urls)} total):")
    print(f"{'-'*80}")
    for i, url in enumerate(new_urls, 1):
        print(f"  {i}. {url}")
    print(f"{'-'*80}\n")

if len(new_urls) == 0:
    print("\n✅ No new blogs to scrape. All discovered blogs are up to date.")
    # Don't exit - we'll check if existing blogs need preprocessing later

# Only scrape new URLs if we have any
if len(new_urls) > 0:
    print(f"🔄 Starting to scrape {len(new_urls)} new blogs...\n")
    scraper = BlogContentScraper()
    results = scraper.scrape_multiple(new_urls)
    print(f"\n✅ Scraping completed!")

    # Create DataFrame from results
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([
        StructField("url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("markdown_content", StringType(), True),
        StructField("domain", StringType(), True)
    ])

    blog_posts_data = []
    failed_scrapes = []

    for result in results:
        if result['success']:
            row = {
                "url": result['metadata']['url'],
                "title": result['metadata']['title'],
                "markdown_content": result['content'],
                "domain": result['metadata']['domain'],
            }
            blog_posts_data.append(row)
        else:
            failed_scrapes.append({
                'url': result['metadata']['url'],
                'error': result['metadata'].get('error', 'Unknown error')
            })

    # Log scraping results
    print(f"\n{'='*80}")
    print(f"📊 Scraping Results")
    print(f"{'='*80}")
    print(f"  ✅ Successfully scraped: {len(blog_posts_data)}/{len(new_urls)}")
    print(f"  ❌ Failed to scrape:     {len(failed_scrapes)}/{len(new_urls)}")
    print(f"{'='*80}\n")

    if failed_scrapes:
        print(f"⚠️  Failed Scrapes ({len(failed_scrapes)} total):")
        print(f"{'-'*80}")
        for i, fail in enumerate(failed_scrapes, 1):
            print(f"  {i}. {fail['url']}")
            print(f"     Error: {fail['error'][:100]}")
        print(f"{'-'*80}\n")

    logger.info(f"Successfully scraped {len(blog_posts_data)} out of {len(new_urls)} new posts")

    if not blog_posts_data:
        print("⚠️  All scraping attempts failed for new blogs")
        blog_posts_data = []  # Empty list, will skip to reading raw table
    else:
        df = spark.createDataFrame(blog_posts_data, schema)

        # Always use append mode for raw data ingestion
        # If table doesn't exist, Spark will create it automatically
        # Manual intervention required for schema changes or table recreation
        print(f"Writing {len(blog_posts_data)} new blogs in append mode")

        df.write.mode('append').saveAsTable(raw_blog_content_table)

        # Display current table stats
        current_df = spark.table(raw_blog_content_table)
        print(f"✅ Total blogs in table: {current_df.count()}")
else:
    print("⏭️  Skipping scraping - no new blogs to fetch")
    blog_posts_data = []

print(f"\n{'='*80}")
print(f"✅ Blog Ingestion Complete")
print(f"{'='*80}")
print(f"Blogs stored in: {raw_blog_content_table}")
print(f"Total blogs in raw table: {spark.table(raw_blog_content_table).count()}")
print(f"\nℹ️  Note: AI extraction and chunking will be performed by ChunkingTask")
print(f"{'='*80}\n")
