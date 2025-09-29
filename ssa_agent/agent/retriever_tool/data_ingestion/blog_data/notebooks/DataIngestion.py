# Databricks notebook source
# MAGIC %pip install -qqqq requests beautifulsoup4 html2text lxml urllib3 langchain transformers
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

########################################################################################################################
# Blog Scraper Data Ingestion Pipeline with Chunking
# 
# This pipeline scrapes blog content and chunks it immediately for vector search.
# Each blog post is converted to markdown and then chunked by headers and token size.
#
# inputs: 
# - bundle_root: path to the bundle root
# - websites: comma-separated list of websites to scrape
# - delta_table_name: name of the delta table to save the chunked data to
# - urls_file: optional file with hardcoded URLs (relative to bundle_root)
# - chunk_size: maximum tokens per chunk (default: 500)
# - chunk_overlap: overlapping tokens between chunks (default: 50)
#
########################################################################################################################

# COMMAND ----------

bundle_root = dbutils.widgets.get("bundle_root")
websites = dbutils.widgets.get("websites") # "https://www.databricksters.com/,https://www.canadiandataguy.com/"
raw_blog_content_table = dbutils.widgets.get("raw_blog_content_table") # "main.default.blog_chunks"
preprocessed_blogs_table = dbutils.widgets.get("preprocessed_blogs_table") # "veena_ramesh_test.data.preprocessed_blogs"
chunk_size = int(dbutils.widgets.get("chunk_size") or "500")
chunk_overlap = int(dbutils.widgets.get("chunk_overlap") or "50")

# COMMAND ----------

import sys
import os
import logging
from typing import List, Dict, Any
import uuid
from datetime import datetime

sys.path.append(bundle_root)

from retriever_tool.data_ingestion.blog_data.utils import BlogScraper, Chunker

from pyspark.sql.functions import col, explode, pandas_udf, lit
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from pyspark.sql import functions as F
import pandas as pd

logger = logging.getLogger(__name__)

# COMMAND ----------
_websites = websites.split(",")
scraper = BlogScraper(_websites)

blogs = scraper.scrape_all_blogs()
blogs.write.mode('overwrite').saveAsTable(raw_blog_content_table)
blogs.display()

# COMMAND ----------

chunker = Chunker(max_chunk_size=chunk_size, chunk_overlap=chunk_overlap)
blogs = spark.table(raw_blog_content_table)

@pandas_udf("array<string>")
def parse_and_split(content: pd.Series) -> pd.Series: 
    """Chunk blog content with metadata."""
    
    return content.apply(lambda text: chunker.chunk_document(text))


preprocessed_blogs = (blogs.select(F.posexplode(parse_and_split('markdown_content')).alias('chunk_id', 'content'), 
"domain", "url"))

preprocessed_blogs.write.mode('overwrite').saveAsTable(preprocessed_blogs_table)
preprocessed_blogs.display()