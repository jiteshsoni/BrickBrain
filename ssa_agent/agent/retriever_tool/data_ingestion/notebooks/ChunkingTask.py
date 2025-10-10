# Databricks notebook source
# MAGIC %pip install -qqqq langchain langchain-text-splitters transformers tiktoken
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

########################################################################################################################
# Unified Content Processing & Chunking Task
# 
# This task reads from RAW tables, applies AI extraction, and chunks content.
# All preprocessing tables are OVERWRITTEN to ensure deterministic output.
#
# Widget Parameters:
# - bundle_root: path to the bundle root
# - raw_blog_content_table: Unity Catalog table with raw blog markdown
# - raw_youtube_content_table: Unity Catalog table with raw video transcriptions
# - preprocessed_content_table: Unity Catalog table to write AI-extracted content (OVERWRITE)
# - preprocessed_chunked_table: Unity Catalog table to write chunked content (OVERWRITE)
# - chunk_size: maximum tokens per chunk (default: 800)
# - chunk_overlap: overlapping tokens between chunks (default: 50)
# - llm_model: LLM model for content extraction
#
# Key Design:
# - Reads ALL data from raw tables (incremental happens at raw level)
# - Applies AI Query to extract technical content
# - Chunks the extracted content
# - OVERWRITES both preprocessed tables (ensures no duplicates)
#
########################################################################################################################

# COMMAND ----------

bundle_root = dbutils.widgets.get("bundle_root")
raw_blog_content_table = dbutils.widgets.get("raw_blog_content_table")
raw_youtube_content_table = dbutils.widgets.get("raw_youtube_content_table")
preprocessed_content_table = dbutils.widgets.get("preprocessed_content_table")
preprocessed_chunked_table = dbutils.widgets.get("preprocessed_chunked_table")
chunk_size = int(dbutils.widgets.get("chunk_size") or "800")
chunk_overlap = int(dbutils.widgets.get("chunk_overlap") or "50")
llm_model = dbutils.widgets.get("llm_model") or "databricks-meta-llama-3-3-70b-instruct"

# Validate required parameters
assert bundle_root, "Bundle root is required"
assert raw_blog_content_table, "Raw blog content table is required"
assert raw_youtube_content_table, "Raw youtube content table is required"
assert preprocessed_content_table, "Preprocessed content table is required"
assert preprocessed_chunked_table, "Preprocessed chunked table is required"

print(f"Bundle root: {bundle_root}")
print(f"Raw blog table: {raw_blog_content_table}")
print(f"Raw youtube table: {raw_youtube_content_table}")
print(f"Preprocessed table: {preprocessed_content_table}")
print(f"Chunked table: {preprocessed_chunked_table}")
print(f"Chunk size: {chunk_size}")
print(f"Chunk overlap: {chunk_overlap}")
print(f"LLM model: {llm_model}")

# COMMAND ----------

# Inline Chunker class
import re
from typing import List, Dict, Any
from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter
from transformers import AutoTokenizer

# COMMAND ----------

class Chunker:    
    def __init__(self, max_chunk_size: int = 800, chunk_overlap: int = 100, min_chunk_size: int = 200):
        self.max_chunk_size = max_chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_chunk_size = min_chunk_size
        
        self.tokenizer = AutoTokenizer.from_pretrained("openai-community/openai-gpt")
        
        self.headers_to_split_on = [
            ("####", "Header 4"),
        ]
        
        self.markdown_splitter = MarkdownHeaderTextSplitter(
            headers_to_split_on=self.headers_to_split_on
        )
        
        self.text_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(
            self.tokenizer, 
            chunk_size=self.max_chunk_size, 
            chunk_overlap=self.chunk_overlap,
            separators=["\n\n", "\n", ". ", "! ", "? "]
        )
    
    def estimate_tokens(self, text: str) -> int:
        return len(self.tokenizer.encode(text))

    def chunk_document(self, content: str, document_title: str = "") -> List[str]:
        if not content or len(content.strip()) < 100:
            return []

        if not document_title:
            title_match = re.search(r'^#\s+(.+)$', content, re.MULTILINE)
            if title_match:
                document_title = title_match.group(1).strip()
        
        header_chunks = self.markdown_splitter.split_text(content)
        final_chunks = []        
        combined_chunks = self._combine_small_sections(header_chunks)
        
        for chunk_data in combined_chunks:
            chunk_text = chunk_data['text']
            chunk_title = chunk_data.get('title', '')
            
            if not chunk_title and document_title:
                chunk_title = document_title
            
            if self.estimate_tokens(chunk_text) > self.max_chunk_size:
                sub_chunks = self.text_splitter.split_text(chunk_text)
                for sub_chunk in sub_chunks:
                    if len(sub_chunk.strip()) >= self.min_chunk_size:
                        final_chunks.append(sub_chunk.strip())
            else:    
                if len(chunk_text.strip()) >= self.min_chunk_size:
                    final_chunks.append(chunk_text.strip())
        
        return final_chunks

    def _combine_small_sections(self, header_chunks: List[Any]) -> List[Dict[str, str]]:
        if not header_chunks:
            return []
        
        combined = []
        current_text = ""
        current_title = ""
        
        for chunk in header_chunks:
            chunk_text = chunk.page_content
            chunk_metadata = chunk.metadata
            
            chunk_title = chunk_metadata.get("Header 4", "")
            
            potential_text = current_text + "\n\n" + chunk_text if current_text else chunk_text
            potential_tokens = self.estimate_tokens(potential_text)
            
            if current_text and potential_tokens > self.max_chunk_size:
                combined.append({
                    'text': current_text.strip(),
                    'title': current_title
                })
                current_text = chunk_text
                current_title = chunk_title
            else:
                if current_text:
                    current_text += "\n\n" + chunk_text
                    if not current_title:
                        current_title = chunk_title
                else:
                    current_text = chunk_text
                    current_title = chunk_title
        
        if current_text:
            combined.append({
                'text': current_text.strip(),
                'title': current_title
            })
        
        return combined

# COMMAND ----------

from pyspark.sql.functions import col, posexplode, pandas_udf, lit, expr, concat
from pyspark.sql.types import ArrayType, StringType
import pandas as pd  # Still needed for chunking UDF

# COMMAND ----------

print("="*80)
print("STAGE 1: READ RAW DATA")
print("="*80)

# Read blogs
try:
    blogs_df = spark.table(raw_blog_content_table)
    blog_count = blogs_df.count()
    print(f"\n📄 Blogs: {blog_count} items from {raw_blog_content_table}")
except Exception as e:
    print(f"⚠️  No blog data found: {str(e)[:200]}")
    blogs_df = None
    blog_count = 0

# Read videos
try:
    videos_df = spark.table(raw_youtube_content_table)
    video_count = videos_df.count()
    print(f"🎥 Videos: {video_count} items from {raw_youtube_content_table}")
except Exception as e:
    print(f"⚠️  No video data found: {str(e)[:200]}")
    videos_df = None
    video_count = 0

total_raw = blog_count + video_count
print(f"\n📊 Total raw items to process: {total_raw}")

if total_raw == 0:
    print("❌ No data to process!")
    dbutils.notebook.exit("No data in raw tables")

# COMMAND ----------

print("\n" + "="*80)
print("STAGE 2: AI EXTRACTION")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Process Blogs with AI Extraction
if blogs_df and blog_count > 0:
    blog_prompt = """
Please analyze the following blog post and extract all of the EXACT phrases used for the technical content. Keep all of the technical information written exactly, and ignore all filler content (like jokes, introductions, and advertisements). 

Instructions:
1. Extract technical content exactly as is. 
2. Preserve EXISTING examples and technical procedures. Put code quotes around code. 
3. Remove all filtered or unnecessary content that is not useable for search. 
4. Do not insert any content. 

Output format in Markdown. 

Blog post:
"""
    
    print(f"\n🤖 Starting AI extraction for {blog_count} blogs...")
    print(f"   Model: {llm_model}")
    print(f"   This may take a few minutes...\n")
    
    blogs_extracted = (
        blogs_df
        .withColumn("content", expr(f"ai_query('{llm_model}', concat('{blog_prompt}', markdown_content))"))
        .withColumn("content_type", lit("blog"))
        .select("url", "domain", "content", "content_type")
    )
    
    # Trigger computation and count
    extracted_blog_count = blogs_extracted.count()
    
    print(f"✅ Blog extraction complete: {extracted_blog_count}/{blog_count} blogs processed")
else:
    blogs_extracted = None
    print("⏭️  No blogs to process")

# COMMAND ----------

# DBTITLE 1,Process Videos with Double AI Query (Clean + Extract)
if videos_df and video_count > 0:
    print(f"\n🤖 Processing {video_count} videos with double AI query approach...")
    print(f"   Model: {llm_model}")
    print(f"   Step 1: Clean transcripts (fix spelling, grammar, remove filler)")
    print(f"   Step 2: Extract technical content")
    print(f"   This runs fully distributed on Spark (no driver bottleneck)\n")
    
    # STEP 1: AI Query for Transcript Cleaning
    cleaning_prompt = """Clean and normalize this video transcript for technical documentation:

1. Fix spelling errors (especially technical terms: databricks, kubernetes, mlflow, spark, delta, etc.)
2. Correct grammar and punctuation
3. Remove filler words (um, uh, like, you know, basically, actually, etc.)
4. Remove repetitive phrases
5. Normalize capitalization
6. Keep ALL technical content intact

Output clean, readable text maintaining the original meaning and technical accuracy.

Transcript:
"""
    
    print(f"   🧹 Step 1: Cleaning transcripts with AI...")
    videos_with_cleaned = (
        videos_df
        .withColumn("videoId", col("id")['videoId'])
        .withColumn("url", concat(lit("https://youtube.com/watch?v="), col("videoId")))
        .withColumn("domain", lit("youtube.com"))
        .withColumn("transcription_cleaned", 
                   expr(f"ai_query('{llm_model}', concat('{cleaning_prompt}', transcription))"))
    )
    
    # Count to trigger computation
    cleaned_count = videos_with_cleaned.count()
    print(f"   ✅ Cleaned {cleaned_count} video transcripts\n")
    
    # STEP 2: AI Query for Content Extraction
    extraction_prompt = """Extract key technical content, insights, and actionable information from this cleaned video transcript.

Instructions:
1. Focus on technical concepts, best practices, and specific recommendations
2. Preserve code examples, specific tool names, and technical procedures
3. Correct technical terms and acronyms for Databricks Data Engineers (e.g., DBT, ETL, Delta Lake)
4. Keep the most valuable insights for data engineers and solutions architects

Output format in Markdown:
1. Key concepts <key concepts>
2. Best practices <best practices>
3. Actionable insights <actionable insights>
4. Technical recommendations <recommendations>

Cleaned Transcript:
"""
    
    print(f"   🎯 Step 2: Extracting technical content with AI...")
    videos_extracted = (
        videos_with_cleaned
        .withColumn("content", 
                   expr(f"ai_query('{llm_model}', concat('{extraction_prompt}', transcription_cleaned))"))
        .withColumn("content_type", lit("video"))
        .select("url", "domain", "content", "content_type")
    )
    
    # Trigger computation and count
    extracted_video_count = videos_extracted.count()
    
    print(f"   ✅ Extracted content from {extracted_video_count} videos")
    print(f"\n✅ Video processing complete: {extracted_video_count}/{video_count} videos processed\n")
else:
    videos_extracted = None
    print("⏭️  No videos to process")

# COMMAND ----------

# DBTITLE 1,Combine All Extracted Content
print("\n" + "="*80)
print("COMBINING EXTRACTED CONTENT")
print("="*80)

if blogs_extracted and videos_extracted:
    print(f"   Combining {blog_count} blogs + {video_count} videos")
    preprocessed_df = blogs_extracted.union(videos_extracted)
elif blogs_extracted:
    print(f"   Using {blog_count} blogs only")
    preprocessed_df = blogs_extracted
elif videos_extracted:
    print(f"   Using {video_count} videos only")
    preprocessed_df = videos_extracted
else:
    print("❌ No data after extraction")
    dbutils.notebook.exit("No data after AI extraction")

preprocessed_count = preprocessed_df.count()
print(f"\n✅ Total items after AI extraction: {preprocessed_count}")
print(f"{'='*80}\n")

# COMMAND ----------

print("\n" + "="*80)
print("STAGE 3: WRITE PREPROCESSED CONTENT (OVERWRITE)")
print("="*80)

print(f"\n💾 Writing {preprocessed_count} items to {preprocessed_content_table}")
print(f"   Mode: OVERWRITE (ensures no duplicates)")

preprocessed_df.write.mode('overwrite').saveAsTable(preprocessed_content_table)

# Verify
final_preprocessed_count = spark.table(preprocessed_content_table).count()
print(f"\n✅ Preprocessed table written")
print(f"   Total rows: {final_preprocessed_count}")
print(f"\n   Breakdown by content type:")
spark.table(preprocessed_content_table).groupBy("content_type").count().show()

# COMMAND ----------

print("\n" + "="*80)
print("STAGE 4: CHUNKING")
print("="*80)

chunker = Chunker(max_chunk_size=chunk_size, chunk_overlap=chunk_overlap)

@pandas_udf(ArrayType(StringType()))
def chunk_content(texts: pd.Series) -> pd.Series:
    """Chunk content into embedding-sized pieces."""
    return texts.apply(lambda text: chunker.chunk_document(text) if text else [])

print(f"\n🔪 Chunking {preprocessed_count} items...")
print(f"   Chunk size: {chunk_size} tokens")
print(f"   Overlap: {chunk_overlap} tokens")

# Apply chunking and explode into multiple rows
chunked_df = preprocessed_df.select(
    posexplode(chunk_content(col("content"))).alias("chunk_id", "content"),
    "url",
    "domain",
    "content_type"
).select(
    "url",
    "domain",
    "content",
    "content_type",
    col("chunk_id").cast("bigint")
)

chunk_count = chunked_df.count()
expansion_ratio = chunk_count / preprocessed_count if preprocessed_count > 0 else 0

print(f"\n✅ Chunking complete:")
print(f"   Original items: {preprocessed_count}")
print(f"   Total chunks: {chunk_count}")
print(f"   Expansion ratio: {expansion_ratio:.2f}x")

# Breakdown by content type
print(f"\n   Chunks by content type:")
chunked_df.groupBy("content_type").count().show()

# COMMAND ----------

print("\n" + "="*80)
print("STAGE 5: WRITE CHUNKED CONTENT (OVERWRITE)")
print("="*80)

print(f"\n💾 Writing {chunk_count} chunks to {preprocessed_chunked_table}")
print(f"   Mode: OVERWRITE (ensures deterministic output)")

chunked_df.write.mode('overwrite').saveAsTable(preprocessed_chunked_table)

# Verify
final_chunk_count = spark.table(preprocessed_chunked_table).count()
print(f"\n✅ Chunked table written")
print(f"   Total chunks: {final_chunk_count}")

# COMMAND ----------

print("\n" + "="*80)
print("✅ CONTENT PROCESSING COMPLETE")
print("="*80)

print(f"""
Summary:
  Raw Data:          {total_raw} items ({blog_count} blogs + {video_count} videos)
  AI Extracted:      {preprocessed_count} items
  Chunked:           {chunk_count} chunks ({expansion_ratio:.2f}x expansion)
  
Tables Updated (OVERWRITE):
  - {preprocessed_content_table} ({final_preprocessed_count} rows)
  - {preprocessed_chunked_table} ({final_chunk_count} rows)
  
✨ All data is now ready for vector search indexing!
""")

# Display sample
print("Sample of chunked data:")
spark.table(preprocessed_chunked_table).display()
