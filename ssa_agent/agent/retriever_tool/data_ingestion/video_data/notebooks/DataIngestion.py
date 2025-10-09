# Databricks notebook source
# MAGIC %pip install -qqqq youtube-transcript-api nltk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

########################################################################################################################
# Video Transcription Data Ingestion Pipeline
# 
# inputs: 
# - bundle_root: path to the bundle root
# - channel_name: name of the channel to fetch videos from
# - max_videos: maximum number of videos to fetch
# - max_workers: maximum number of workers to use for transcription
# - delay: delay between requests
# - raw_data_table: name of the table to save the data to
#
########################################################################################################################

# COMMAND ----------

bundle_root = dbutils.widgets.get("bundle_root")
channel_name = dbutils.widgets.get("channel_name")
max_videos = int(dbutils.widgets.get("max_videos")) 
max_workers = 5 
webshare_proxy_username = dbutils.secrets.get(scope="brickbrain_ssa_agent_scope", key="webshare_proxy_username")
webshare_proxy_password = dbutils.secrets.get(scope="brickbrain_ssa_agent_scope", key="webshare_proxy_password") 
youtube_api_key = dbutils.secrets.get(scope="brickbrain_ssa_agent_scope", key="youtube_api_key")
raw_youtube_content_table = dbutils.widgets.get("raw_youtube_content_table")
preprocessed_youtube_content_table = dbutils.widgets.get("preprocessed_youtube_content_table")

assert bundle_root, "Bundle root is required"
assert channel_name, "Channel name is required"
assert max_videos, "Max videos is required"
assert raw_youtube_content_table, "Raw youtube content table is required"
assert preprocessed_youtube_content_table, "Preprocessed youtube content table is required"

print(f"Bundle root: {bundle_root}")
print(f"Channel name: {channel_name}")
print(f"Max videos: {max_videos}")
print(f"Raw table: {raw_youtube_content_table}")
print(f"Preprocessed table: {preprocessed_youtube_content_table}")

# Extract catalog and schema from table name and create both if not exists
table_parts = raw_youtube_content_table.split('.')
if len(table_parts) >= 2:
    catalog = table_parts[0]
    schema = table_parts[1]
    
    # Create catalog first
    try:
        print(f"Creating catalog if not exists: {catalog}")
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        print(f"✅ Catalog {catalog} is ready")
    except Exception as e:
        print(f"⚠️  Could not create catalog (may already exist): {str(e)[:200]}")
    
    # Then create schema
    try:
        print(f"Creating schema if not exists: {catalog}.{schema}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        print(f"✅ Schema {catalog}.{schema} is ready")
    except Exception as e:
        print(f"⚠️  Could not create schema (may already exist): {str(e)[:200]}")
        print(f"   Proceeding...")
else:
    print("Warning: Could not extract catalog.schema from table name")

# COMMAND ----------

import sys
import os
import logging
from typing import List, Dict

sys.path.append(os.path.join(bundle_root, "agent"))

from retriever_tool.data_ingestion.video_data.utils import YouTubeClient, TranscriptClient, TranscriptCleaner

from pyspark.sql.functions import pandas_udf, lit, col, expr, concat
from pyspark.sql.types import StringType
import pandas as pd 

# COMMAND ----------

youtube_client = YouTubeClient(youtube_api_key)

transcription_service = TranscriptClient(
    proxy_username=webshare_proxy_username,
    proxy_password=webshare_proxy_password
)

# COMMAND ----------

channel_id = youtube_client.get_channel_id_from_username(channel_name)
print(f"Channel ID for '{channel_name}': {channel_id}. ")

videos = youtube_client.get_channel_videos(channel_id, max_results=max_videos)
print(f"Fetched {len(videos)} videos. ")

# COMMAND ----------

transcribed_videos = transcription_service.get_transcriptions_parallel(
    all_videos=videos,
    max_workers=max_workers,
    delay=0.5
)

successful_transcriptions = sum(1 for video in transcribed_videos if video['transcription_status'] == 'Success')

print(f"Transcription Results:")
print(f"- Total videos: {len(transcribed_videos)}")
print(f"- Successful: {successful_transcriptions}")
print(f"- Failed: {len(transcribed_videos) - successful_transcriptions}")

# COMMAND ----------

df = spark.createDataFrame(transcribed_videos)
df.write.mode('overwrite').saveAsTable(f"{raw_youtube_content_table}")
df.display()

# COMMAND ----------

transcript_cleaner = TranscriptCleaner(
    remove_stopwords=False, 
    fix_spelling=True        
)

# COMMAND ----------

prompt = """
Please analyze the following video transcript and extract the key technical content, insights, and actionable information. Remove all unnecessary filler content and keep all technical information as precise as possible.

Instructions:
1. Focus on technical concepts, best practices, and specific recommendations
2. Remove casual conversation, filler content, and repetitive information
3. Preserve code examples, specific tool names, and technical procedures
4. Correct terms and acronynms, assuming the downstream users are Databricks Data Engineers (e.g. DBT, ETL)
5. Keep the most valuable insights that would help data engineers and solutions architects

Output format in Markdown. 
1. Key concepts <key concepts> 2. Best practices <best practices> 3. Actionable insights <actionable insights> 4. Technical recommendations <recommendations> 

Transcript:
"""

@pandas_udf(returnType=StringType())
def clean_text_batch(texts: pd.Series) -> pd.Series:
    """Vectorized cleaning function for pandas UDF."""
    return texts.apply(transcript_cleaner.clean)

df_cleaned = (
    df.withColumn("transcription_cleaned", clean_text_batch(col("transcription")))
      .withColumn("videoId", col("id")['videoId'])
      .withColumn("url", concat(lit("https://youtube.com/watch?v="), col("videoId")))
      .withColumn("domain", lit("youtube.com"))
      .withColumn("transcription_ai_cleaned", 
                  expr(f"ai_query('databricks-meta-llama-3-3-70b-instruct', concat('{prompt}', transcription_cleaned))"))
      .withColumn("content_type", lit("video"))
      .select("url", "domain", "transcription_ai_cleaned")
) 

df_cleaned.write.mode('overwrite').saveAsTable(f"{preprocessed_youtube_content_table}")
df_cleaned.display()