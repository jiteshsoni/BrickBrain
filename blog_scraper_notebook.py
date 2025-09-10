# Databricks notebook source
# MAGIC %md
# MAGIC # Blog Scraper Pipeline
# MAGIC 
# MAGIC This notebook runs the blog scraper as a Databricks pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import and Setup

# COMMAND ----------

# Install required packages
%pip install requests beautifulsoup4 html2text lxml urllib3

# COMMAND ----------

from blog_scraper import BlogScraper
import logging

# Get logger without configuring basicConfig (Databricks handles this)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Set up widgets with defaults
dbutils.widgets.text("websites", "https://www.databricksters.com/,https://www.canadiandataguy.com/", "Websites to scrape")
dbutils.widgets.text("delta_table_name", "main.default.blog_content", "Delta table name")

# Get configuration from pipeline parameters
websites = dbutils.widgets.get("websites").split(",")
delta_table_name = dbutils.widgets.get("delta_table_name")

logger.info(f"Websites to scrape: {websites}")
logger.info(f"Delta table: {delta_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Blog Scraper

# COMMAND ----------

try:
    # Initialize and run the blog scraper
    scraper = BlogScraper(websites, delta_table_path=delta_table_name)
    
    # Read hardcoded URLs from file if it exists
    hardcoded_urls = []
    try:
        with open('urls.txt', 'r') as f:
            hardcoded_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        logger.info(f"Loaded {len(hardcoded_urls)} hardcoded URLs")
    except FileNotFoundError:
        logger.info("No urls.txt file found, using only discovered URLs")
    
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
blog_posts = df.filter(~df.url.isin("URL_LIST_METADATA", "SCRAPING_SUMMARY")).count()

print(f"Total records in table: {total_records}")
print(f"Blog posts scraped: {blog_posts}")
print(f"Metadata records: {total_records - blog_posts}")

# Show sample of recent blog posts
print("\nRecent blog posts:")
df.filter(~df.url.isin("URL_LIST_METADATA", "SCRAPING_SUMMARY")) \
  .select("url", "title", "domain", "scraped_at") \
  .orderBy("scraped_at", ascending=False) \
  .show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete
# MAGIC 
# MAGIC The blog scraper has successfully completed. Check the Delta table for the latest blog content.