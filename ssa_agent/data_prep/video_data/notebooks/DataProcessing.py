# Databricks notebook source
# MAGIC %pip install -qqqq youtube-transcript-api nltk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

########################################################################################################################
# Video Transcription Data Processing Pipeline
# 
# inputs: 
# - bundle_root: path to the bundle root
# - raw_data_table: name of the table to get transcriptions from
# - cleaned_data_table: name of the table to save the cleaned transcriptions to
#
########################################################################################################################

# COMMAND ----------

raw_data_table = dbutils.widgets.get("raw_data_table") #users.veena_ramesh.raw_video_transcriptions"
cleaned_data_table = dbutils.widgets.get("cleaned_data_table") #users.veena_ramesh.cleaned_video_transcriptions"
bundle_root = dbutils.widgets.get("bundle_root")

# COMMAND ----------

import sys
import os
import logging
from typing import List, Dict

sys.path.append(bundle_root)

from data_prep.data_ingestion.utils import TranscriptCleaner

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

transcript_cleaner = TranscriptCleaner(
    remove_stopwords=False, 
    fix_spelling=True        
)


df = spark.table(raw_data_table)

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, col, regexp_replace, trim, when, length
from pyspark.sql.types import StringType
import pandas as pd 

@pandas_udf(returnType=StringType())
def clean_text_batch(texts: pd.Series) -> pd.Series:
    """Vectorized cleaning function for pandas UDF."""
    return texts.apply(transcript_cleaner.clean)

df_cleaned = df.withColumn("transcription_cleaned", clean_text_batch(col("transcription")))
df_cleaned.write.mode('overwrite').saveAsTable(f"{cleaned_data_table}")
df_cleaned.display()
