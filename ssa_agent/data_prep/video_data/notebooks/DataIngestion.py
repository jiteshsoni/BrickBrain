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

KEY = dbutils.secrets.get(scope="brickbrain_ssa_agent_scope", key="youtube_api_key")
channel_name = dbutils.widgets.get("channel_name")
max_videos = dbutils.widgets.get("max_videos") #1
max_workers = 5 
webshare_proxy_username = dbutils.secrets.get(scope="brickbrain_ssa_agent_scope", key="webshare_proxy_username")
webshare_proxy_password = dbutils.secrets.get(scope="brickbrain_ssa_agent_scope", key="webshare_proxy_password") 
raw_data_table = dbutils.widgets.get("raw_data_table") #users.veena_ramesh.raw_video_transcriptions"

# COMMAND ----------

import sys
import os
import logging
from typing import List, Dict

sys.path.append(bundle_root)

from data_prep.data_ingestion.utils import Config, YouTubeClient, TranscriptClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

youtube_client = YouTubeClient(KEY)

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
df.write.mode('overwrite').saveAsTable(f"{raw_data_table}")
df.display()