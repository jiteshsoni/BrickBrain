# Databricks notebook source
# MAGIC %pip install requests beautifulsoup4 html2text lxml urllib3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

########################################################################################################################
# Blog Scraper Data Ingestion Pipeline
# 
# inputs: 
# - bundle_root: path to the bundle root
# - websites: comma-separated list of websites to scrape
# - delta_table_name: name of the delta table to save the data to
# - urls_file: optional file with hardcoded URLs (relative to bundle_root)
#
########################################################################################################################

# COMMAND ----------

bundle_root = dbutils.widgets.get("bundle_root")
websites = dbutils.widgets.get("websites") # "https://www.databricksters.com/,https://www.canadiandataguy.com/"
delta_table_name = dbutils.widgets.get("delta_table_name") # "main.default.blog_content"
urls_file = dbutils.widgets.get("urls_file") if dbutils.widgets.get("urls_file") else "urls.txt" # "urls.txt"

# COMMAND ----------

import sys
import os
import logging

sys.path.append(bundle_root)

from data_prep.blog_data.utils.blog_scraper import BlogScraper

logger = logging.getLogger(__name__)

# COMMAND ----------

# Get configuration from pipeline parameters
websites = websites.split(",")

logger.info(f"Websites to scrape: {websites}")
logger.info(f"Delta table: {delta_table_name}")

# COMMAND ----------

try:
    # Initialize and run the blog scraper
    scraper = BlogScraper(websites, delta_table_path=delta_table_name)
    
    # Read hardcoded URLs from file if it exists
    hardcoded_urls = []
    urls_file_path = os.path.join(bundle_root, urls_file)
    try:
        with open(urls_file_path, 'r') as f:
            hardcoded_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        logger.info(f"Loaded {len(hardcoded_urls)} hardcoded URLs from {urls_file_path}")
    except FileNotFoundError:
        logger.info(f"No {urls_file_path} file found, using only discovered URLs")
    
    # Run the scraper
    scraper.scrape_all_blogs(hardcoded_urls=hardcoded_urls)
    
    logger.info("Blog scraping completed successfully!")
    
except Exception as e:
    logger.error(f"Error during blog scraping: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Results

# COMMAND ----------

# Display summary of scraped data
df = spark.table(delta_table_name)
total_records = df.count()

print(f"Blog posts scraped: {total_records}")

# Show sample of recent blog posts
print("\nRecent blog posts:")
df.select("url", "title", "domain", "scraped_at") \
  .orderBy("scraped_at", ascending=False) \
  .show(10, truncate=False)

# Show metadata from separate table
metadata_table_name = delta_table_name + "_metadata"
try:
    metadata_df = spark.table(metadata_table_name)
    metadata_count = metadata_df.count()
    print(f"\nMetadata records: {metadata_count}")
    print("\nLatest metadata:")
    metadata_df.select("metadata_type", "created_at") \
      .orderBy("created_at", ascending=False) \
      .show(5, truncate=False)
except Exception as e:
    print(f"No metadata table found: {e}")