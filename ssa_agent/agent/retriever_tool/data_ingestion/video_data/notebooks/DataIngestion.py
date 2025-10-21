# Databricks notebook source
# MAGIC %pip install -qqqq youtube-transcript-api nltk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

########################################################################################################################
# Video Transcription Data Ingestion Pipeline
#
# Widget Parameters:
# - bundle_root: path to the bundle root
# - channel_name: name of the YouTube channel to fetch videos from
# - max_videos: maximum number of videos to fetch from the channel
# - published_after: (optional) fetch only videos published after this date (YYYY-MM-DD format, empty for all)
# - raw_youtube_content_table: Unity Catalog table name for raw video data with transcriptions
#
# Databricks Secrets (required):
# - youtube_api_key: YouTube Data API v3 key
# - webshare_proxy_username: Proxy username for transcript API
# - webshare_proxy_password: Proxy password for transcript API
#
# Features:
# - Incremental ingestion: Only scrapes new videos, skips existing ones
# - Parallel transcription fetching (5 workers)
#
# Note: AI extraction and chunking are handled by the ChunkingTask notebook
########################################################################################################################

# COMMAND ----------

bundle_root = dbutils.widgets.get("bundle_root")
channel_name = dbutils.widgets.get("channel_name")
max_videos = int(dbutils.widgets.get("max_videos")) 
max_workers = 5  # Number of parallel workers for transcription
published_after = dbutils.widgets.get("published_after") or None  # Optional date filter
webshare_proxy_username = dbutils.secrets.get(scope="brickbrain_ssa_agent_scope", key="webshare_proxy_username")
webshare_proxy_password = dbutils.secrets.get(scope="brickbrain_ssa_agent_scope", key="webshare_proxy_password") 
youtube_api_key = dbutils.secrets.get(scope="brickbrain_ssa_agent_scope", key="youtube_api_key")
raw_youtube_content_table = dbutils.widgets.get("raw_youtube_content_table")

assert bundle_root, "Bundle root is required"
assert channel_name, "Channel name is required"
assert max_videos, "Max videos is required"
assert raw_youtube_content_table, "Raw youtube content table is required"

print(f"Bundle root: {bundle_root}")
print(f"Channel name: {channel_name}")
print(f"Max videos: {max_videos}")
print(f"Published after: {published_after if published_after else 'All videos'}")
print(f"Raw table: {raw_youtube_content_table}")

# Note: Catalog and schema should be created via setup_brickbrain_workspace.py before running this notebook

# COMMAND ----------

# Set default catalog to avoid Hive Metastore errors
catalog_name = raw_youtube_content_table.split('.')[0]
schema_name = raw_youtube_content_table.split('.')[1]
print(f"\n🔧 Setting default catalog: {catalog_name}")
print(f"🔧 Setting default schema: {schema_name}")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

import sys
import os
import logging
import warnings
from typing import List, Dict

# Suppress threadpoolctl noise/warnings
warnings.filterwarnings('ignore', message='.*threadpoolctl.*')
warnings.filterwarnings('ignore', category=RuntimeWarning, module='threadpoolctl')

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

videos = youtube_client.get_channel_videos(
    channel_id=channel_id, 
    max_results=max_videos,
    published_after=published_after
)
print(f"Fetched {len(videos)} videos from YouTube API. ")

# Check for existing videos to implement incremental ingestion
existing_video_ids = set()
try:
    print(f"Checking for existing videos in {raw_youtube_content_table}...")
    existing_df = spark.table(raw_youtube_content_table)
    
    # Spark Connect compatible way to get video IDs
    video_ids = existing_df.select(col("id.videoId").alias("videoId")).distinct().collect()
    existing_video_ids = set(row.videoId for row in video_ids if row.videoId)
    
    print(f"✅ Found {len(existing_video_ids)} unique videos already in table.")
except Exception as e:
    print(f"Table {raw_youtube_content_table} does not exist or is empty. Will create new table.")
    print(f"Details: {str(e)[:200]}")

# Filter out already scraped videos and separate them for logging
new_videos = []
skipped_videos = []

for video in videos:
    video_id = video['id']['videoId']
    if video_id in existing_video_ids:
        skipped_videos.append(video)
    else:
        new_videos.append(video)

print(f"\nVideos from API fetch: {len(videos)} total")
print(f"  - Already scraped (skipping): {len(skipped_videos)}")
print(f"  - New videos to fetch: {len(new_videos)}")

# Show skipped videos (only those from current API fetch that already exist)
if skipped_videos:
    print(f"\n⏭️  Skipping already scraped videos:")
    for video in skipped_videos:
        video_id = video['id']['videoId']
        title = video['snippet'].get('title', 'Unknown Title')
        print(f"   - https://youtube.com/watch?v={video_id}")
        print(f"     Title: {title}")

# Show new videos to fetch
if new_videos:
    print(f"\n🆕 New videos to fetch:")
    for video in new_videos:
        video_id = video['id']['videoId']
        title = video['snippet'].get('title', 'Unknown Title')
        print(f"   - https://youtube.com/watch?v={video_id}")
        print(f"     Title: {title}")

if len(new_videos) == 0:
    print("\n✅ No new videos to scrape. All fetched videos are up to date.")
    # Don't exit - we'll check if existing videos need preprocessing later

videos = new_videos

# COMMAND ----------

# Only run transcription if we have new videos to fetch
if len(videos) > 0:
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
else:
    print("⏭️  Skipping transcription - no new videos to fetch from YouTube")
    transcribed_videos = []

# COMMAND ----------

# If we transcribed new videos, save them to raw table
if len(transcribed_videos) > 0:
    # Define explicit schema for YouTube video data to avoid schema inference issues
    from pyspark.sql.types import StructType, StructField, StringType, MapType

    youtube_schema = StructType([
        StructField("id", MapType(StringType(), StringType()), True),
        StructField("snippet", MapType(StringType(), StringType()), True),
        StructField("transcription", StringType(), True),
        StructField("transcription_status", StringType(), True)
    ])

    df = spark.createDataFrame(transcribed_videos, schema=youtube_schema)

    # Always use append mode for raw data ingestion
    print(f"Writing {len(transcribed_videos)} new videos in append mode")
    df.write.mode('append').saveAsTable(f"{raw_youtube_content_table}")

    # Display current table stats
    current_df = spark.table(raw_youtube_content_table)
    print(f"✅ Total videos in table: {current_df.count()}")
    df.display()
else:
    print("⏭️  No new videos to write to raw table")

# Now read ALL videos from raw table for preprocessing
# This ensures we process videos that may have been scraped but never preprocessed
print(f"\n📖 Reading all videos from raw table for preprocessing...")
try:
    df = spark.table(raw_youtube_content_table)
    raw_video_count = df.count()
    print(f"   Found {raw_video_count} total videos in raw table")
    
    if raw_video_count == 0:
        print("   No videos to process")
        dbutils.notebook.exit("No videos in raw table")
except Exception as e:
    print(f"   ❌ Error reading raw table: {str(e)[:200]}")
    dbutils.notebook.exit("Raw table doesn't exist")

print(f"\n{'='*80}")
print(f"✅ Video Ingestion Complete")
print(f"{'='*80}")
print(f"Videos stored in: {raw_youtube_content_table}")
print(f"Total videos in raw table: {spark.table(raw_youtube_content_table).count()}")
print(f"\nℹ️  Note: AI extraction and chunking will be performed by ChunkingTask")
print(f"{'='*80}\n")