# Databricks notebook source
# MAGIC %pip install -qqqq requests beautifulsoup4 html2text lxml urllib3 feedparser langchain transformers tiktoken
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

########################################################################################################################
# Blog Scraper Data Ingestion Pipeline
# 
# This pipeline scrapes blog content and chunks it for vector search.
# Each blog post is converted to markdown.
#
# inputs: 
# - bundle_root: path to the bundle root
# - websites: comma-separated list of websites to scrape
# - raw_blog_content_table: name of the delta table to save the markdown data to
# - preprocessed_blogs_table: name of the delta table to save the chunked data to
# - chunk_size: maximum tokens per chunk (default: 500)
# - chunk_overlap: overlapping tokens between chunks (default: 50)
#
########################################################################################################################

# COMMAND ----------

bundle_root = dbutils.widgets.get("bundle_root")
websites = dbutils.widgets.get("websites")
raw_blog_content_table = dbutils.widgets.get("raw_blog_content_table")
preprocessed_blogs_table = dbutils.widgets.get("preprocessed_blogs_table")
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

from retriever_tool.data_ingestion.blog_data.utils import scrape_blogs, Chunker

from pyspark.sql.functions import col, explode, pandas_udf, lit, expr
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from pyspark.sql import functions as F
import pandas as pd


logger = logging.getLogger(__name__)

# COMMAND ----------

df = scrape_blogs(websites, spark)
df.write.mode('overwrite').saveAsTable(raw_blog_content_table)

# COMMAND ----------

blogs = spark.sql(f"select * from {raw_blog_content_table}")

prompt = """
Please analyze the following blog post and extract all of the EXACT phrases used for the technical content. Keep all of the technical information written exactly, and ignore all filler content (like jokes, introductions, and advertisements). 

Instructions:
1. Extract technical content exactly as is. 
2. Preserve EXISTING examples and technical procedures. Put code quotes around code. 
3. Remove all filtered or unnecessary content that is not useable for search. 
4. Do not insert any content. 

Output format in Markdown. 

Blog post:
"""

blogs_extracted = (
    blogs
    .withColumn("extracted_content", expr(f"ai_query('databricks-meta-llama-3-3-70b-instruct', concat('{prompt}', markdown_content))")
    ) 
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
import pandas as pd

chunker = Chunker(max_chunk_size=800, chunk_overlap=50)

@pandas_udf(ArrayType(StringType()))
def parse_and_split(content: pd.Series) -> pd.Series:
    """Chunk blog content and add summaries."""
    return content.apply(lambda text: chunker.chunk_document(text)) 

final_df = blogs_extracted.select(
    F.posexplode(parse_and_split(
        F.col('extracted_content'), 
    )).alias('chunk_id', 'content'), 
    "domain", 
    "url"
).withColumn(
    "content_type", 
    F.lit("blog")
).withColumnRenamed(
    "url", 
    "primary_url"
)

final_df.write.mode('overwrite').saveAsTable(preprocessed_blogs_table)
final_df.display()
