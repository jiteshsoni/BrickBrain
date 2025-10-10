# BrickBrain Data Ingestion

This document covers all data ingestion processes for the BrickBrain knowledge base, including blog content and YouTube video transcriptions.

## 📊 Table Dependency Graph

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           EXTERNAL SOURCES                                   │
│  • Blog Websites (databricksters.com, canadiandataguy.com, medium.com)     │
│  • YouTube Channels (DustinVannoy, CanadianDataGuy)                        │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
│ BlogDataIngestion   │  │VideoDataIngestion_1 │  │VideoDataIngestion_2 │
│     [Task]          │  │     [Task]          │  │     [Task]          │
└──────────┬──────────┘  └──────────┬──────────┘  └──────────┬──────────┘
           │                        │                        │
           ▼                        ▼                        ▼
┌─────────────────────┐  ┌─────────────────────────────────────────────┐
│ raw_blog_content    │  │      raw_youtube_content                    │
│ MODE: APPEND        │  │      MODE: APPEND                           │
│ (Incremental)       │  │      (Incremental)                          │
├─────────────────────┤  ├─────────────────────────────────────────────┤
│ • url               │  │ • id (Map - contains videoId)               │
│ • title             │  │ • snippet (Map - title, description, date)  │
│ • markdown_content  │  │ • transcription (raw text)                  │
│ • domain            │  │ • transcription_status                      │
└──────────┬──────────┘  └──────────┬──────────────────────────────────┘
           │                        │
           └────────────┬───────────┘
                        │
                        ▼
           ┌────────────────────────┐
           │   ChunkingTask         │
           │      [Task]            │
           │ • AI Content Extract   │
           │ • Text Chunking        │
           └────────────┬───────────┘
                        │
                        ▼
           ┌──────────────────────┐
           │ preprocessed_content │
           │ MODE: OVERWRITE      │
           │ (Full Refresh)       │
           ├──────────────────────┤
           │ • url                │
           │ • domain             │
           │ • content (AI clean) │
           │ • content_type       │
           └──────────┬───────────┘
                      │ (in-memory processing)
                      ▼
           ┌──────────────────────────────┐
           │ preprocessed_content_chunked │
           │ MODE: OVERWRITE              │
           │ (Full Refresh)               │
           ├──────────────────────────────┤
           │ • url                        │
           │ • domain                     │
           │ • content (chunk text)       │
           │ • content_type               │
           │ • chunk_id (bigint)          │
           └──────────────┬───────────────┘
                          │
                          ▼
           ┌──────────────────────────────┐
           │ VectorSearchIngestion        │
           │      [Task]                  │
           │ • Add deterministic IDs      │
           │ • Create/sync vector index   │
           └──────────────┬───────────────┘
                          │
                          ▼
           ┌──────────────────────────┐
           │ brickbrain_delta_table   │
           │ MODE: OVERWRITE          │
           │ (Full Refresh + CDF)     │
           ├──────────────────────────┤
           │ • id (hash deterministic)│
           │ • url                    │
           │ • domain                 │
           │ • content                │
           │ • content_type           │
           │ • chunk_id               │
           └──────────┬───────────────┘
                      │ (source table for index)
                      ▼
           ┌──────────────────────────┐
           │   brickbrain_index       │
           │   MODE: DELTA SYNC       │
           │   (Triggered)            │
           ├──────────────────────────┤
           │ • Vector embeddings      │
           │ • Similarity search      │
           │ • Model: gte-large-en    │
           └────────────┬─────────────┘
                                                      │
                                                      ▼
                                         ┌──────────────────────────┐
                                         │ KnowledgeAssistantSync   │
                                         │      [Task]              │
                                         │ • Sync KA with index     │
                                         └────────────┬─────────────┘
                                                      │
                                                      ▼
                                         ┌──────────────────────────┐
                                         │  Knowledge Assistant     │
                                         │  (BrickBrain Agent)      │
                                         └──────────────────────────┘
```

### 🔄 Ingestion Modes Explained

| Mode | Tables | Behavior | Purpose |
|------|--------|----------|---------|
| **APPEND** | `raw_blog_content`, `raw_youtube_content` | Incremental - only new URLs/videoIds added | Minimize API calls, avoid re-scraping |
| **OVERWRITE** | `preprocessed_content`, `preprocessed_content_chunked`, `brickbrain_delta_table` | Full refresh - deterministic output | Ensure no duplicates, consistent processing |
| **DELTA SYNC** | `brickbrain_index` | Incremental via CDF - only changed chunks | Efficient embedding updates |

### 📋 Key Design Decisions

1. **Incremental at Source (APPEND)**
   - Raw tables use APPEND mode with deduplication checks
   - Only new blogs/videos are scraped (saves time and API quota)
   - URL/videoId used as natural deduplication key

2. **Deterministic Downstream (OVERWRITE)**
   - All preprocessed tables use OVERWRITE mode
   - Ensures no duplicates from AI extraction or chunking
   - Deterministic IDs (`hash(url + chunk_id)`) prevent embedding duplicates

3. **Efficient Vector Updates (DELTA SYNC)**
   - Change Data Feed (CDF) enabled on `brickbrain_delta_table`
   - Vector index only re-embeds changed chunks
   - Significantly reduces compute costs after initial load

### 🎯 Task Execution Order

```
┌─ PARALLEL ────────────────────────────────────────┐
│  1. BlogDataIngestion                             │
│  2. VideoDataIngestion_DustinVannoy               │
│  3. VideoDataIngestion_CanadianDataGuy            │
└─────────────────┬─────────────────────────────────┘
                  │ (wait for all to complete)
                  ▼
         ┌────────────────────┐
         │ 4. ChunkingTask    │ ← Reads ALL raw data
         └─────────┬──────────┘
                   ▼
      ┌───────────────────────────┐
      │ 5. VectorSearchIngestion  │ ← Processes ALL chunks
      └─────────┬─────────────────┘
                ▼
   ┌──────────────────────────────┐
   │ 6. KnowledgeAssistantSync    │ ← Updates KA
   └──────────────────────────────┘
```

---

## Overview

BrickBrain ingests data from multiple sources:
- **Blog Content**: Technical blogs from Databricks and related sites
- **Video Content**: YouTube video transcriptions from Databricks channels

All ingested data flows into a vector search index for the BrickBrain agent.

---

## Blog Data Ingestion

### Features
- Scrapes blog content from configured websites
- Converts HTML to clean markdown
- Uses LLM to extract key technical content
- Chunks content for vector search

### Configuration
Blog ingestion is configured in the DAB bundle (`databricks.yml` and `data-ingestion.yml`):
- Website URLs to scrape
- Chunking parameters (chunk size, overlap)
- LLM model for content extraction
- Target Delta tables

### Output Tables
- **Raw Blogs**: `{catalog}.{schema}.raw_blogs` - Original scraped content
- **Preprocessed Blogs**: `{catalog}.{schema}.preprocessed_blogs` - Cleaned and chunked content

### Notebook
`agent/retriever_tool/data_ingestion/blog_data/notebooks/DataIngestion.py`

---

## Video Data Ingestion

### Features
- Fetches videos from specified YouTube channels via YouTube Data API v3
- Downloads transcriptions using YouTube Transcript API (with proxy support)
- Cleans and preprocesses transcriptions (spell correction, filler word removal)
- Uses LLM to extract key technical content
- **Incremental ingestion**: Only scrapes new videos, skips existing ones
- Supports date filtering to scrape only recent videos

### Setup

#### 1. Configure Databricks Secrets

All credentials are stored in Databricks secrets. Run the workspace setup script:

```bash
cd /Users/jitesh.soni/Documents/Cursor_base/BrickBrain/ssa_agent
python setup_brickbrain_workspace.py
```

This will prompt you for:
- YouTube API Key
- Webshare Proxy Username
- Webshare Proxy Password

And create the secret scope `brickbrain_ssa_agent_scope` with these secrets.

#### 2. Configure Channels

Video ingestion is configured in `databricks.yml` and `data-ingestion.yml`:

```yaml
# In databricks.yml
variables:
  youtube_channel: "DustinVannoy"
  max_videos: "1000"
  youtube_channel_2: "CanadianDataGuy"
  max_videos_2: "1000"

# In data-ingestion.yml
- task_key: VideoDataIngestion_DustinVannoy
  base_parameters:
    channel_name: ${var.youtube_channel}
    max_videos: ${var.max_videos}
    published_after: "2024-01-01"  # Only videos from 2024 onwards
    
- task_key: VideoDataIngestion_CanadianDataGuy
  base_parameters:
    channel_name: ${var.youtube_channel_2}
    max_videos: ${var.max_videos_2}
    published_after: ""  # All videos (no date filter)
```

### Date Filtering

The `published_after` parameter filters videos by publication date:
- Format: `YYYY-MM-DD` (e.g., `"2024-01-01"`)
- Set to empty string `""` to fetch all videos regardless of date
- Uses YouTube API's `publishedAfter` parameter

### Output Tables (Consolidated)

All YouTube channels write to the same tables:
- **Raw YouTube Content**: `{catalog}.{schema}.raw_youtube_content`
  - `id`: Video metadata including videoId
  - `snippet`: Video title, description, publish time
  - `transcription`: Raw transcription text
  - `transcription_status`: "Success" or "Fail"

- **Preprocessed YouTube Content**: `{catalog}.{schema}.preprocessed_youtube_content`
  - `url`: Video URL (https://youtube.com/watch?v={videoId})
  - `domain`: "youtube.com"
  - `transcription_ai_cleaned`: LLM-extracted technical content
  - `content_type`: "video"

### Notebook
`agent/retriever_tool/data_ingestion/video_data/notebooks/DataIngestion.py`

### Architecture

```
YouTube API → Fetch Videos (with date filter) → Transcript API → Clean Transcription → LLM Extraction → Delta Table
```

1. **Fetch Videos**: `YouTubeClient.get_channel_videos()` fetches video metadata
2. **Check Existing**: Compare videoIds with existing table to find new videos
3. **Get Transcriptions**: `TranscriptClient.get_transcriptions_parallel()` downloads transcripts in parallel (only for new videos)
4. **Clean Text**: `TranscriptCleaner.clean()` removes filler words and corrects spelling
5. **Extract Content**: LLM (`databricks-meta-llama-3-3-70b-instruct`) extracts technical content
6. **Save**: Append to shared Delta tables

### Proxy Configuration

The transcript API uses Webshare proxy to avoid rate limiting. Free tier provides:
- 10 proxy IPs
- 250MB bandwidth per month
- Sufficient for moderate video ingestion workloads

---

## Incremental Ingestion

### How It Works

1. **Fetch videos from YouTube API** (respects max_videos and published_after filters)
2. **Check existing videos** in `raw_youtube_content` table by extracting `id.videoId`
3. **Filter new videos** by comparing fetched videoIds vs existing videoIds
4. **Scrape only new videos** (fetch transcriptions, which is expensive)
5. **Append to table** using `mode='append'` with `mergeSchema=true`

### Implementation Details

#### Video ID Extraction
```python
# Spark Connect compatible
video_ids = existing_df.select(col("id.videoId").alias("videoId")).distinct().collect()
existing_video_ids = set(row.videoId for row in video_ids if row.videoId)
```

#### Deduplication Check
```python
new_videos = [v for v in videos if v['id']['videoId'] not in existing_video_ids]
```

### API Quota Efficiency

**Without Incremental Ingestion:**
- Every run: Fetch 1000 videos → Transcribe 1000 videos
- Transcription API calls: 1000 (expensive!)

**With Incremental Ingestion:**
- First run: Fetch 1000 videos → Transcribe 1000 videos
- Second run: Fetch 1000 videos → Transcribe 0 videos (all exist)
- Third run (daily): Fetch 1000 videos → Transcribe ~5-10 new videos

**Savings:** ~99% reduction in transcription API calls after initial load!

### Testing Incremental Ingestion

#### Test 1: First Run (No existing data)
**Expected behavior:**
```
Fetched X videos from YouTube API
Checking for existing videos in brickbrain.brickbrain_dev.raw_youtube_content...
Table does not exist or is empty. Will create new table.
New videos to scrape: X out of X
```
**Result:** All videos should be scraped and written to table.

#### Test 2: Second Run (Same parameters)
**Expected behavior:**
```
Fetched X videos from YouTube API
Checking for existing videos in brickbrain.brickbrain_dev.raw_youtube_content...
✅ Found X unique videos already scraped.
New videos to scrape: 0 out of X
✅ No new videos to scrape. All videos are up to date.
```
**Result:** Job exits early, no transcriptions fetched (saves time and API quota).

#### Test 3: Run with Higher max_videos
**Expected behavior:**
```
Fetched Y videos from YouTube API (where Y > X)
Checking for existing videos in brickbrain.brickbrain_dev.raw_youtube_content...
✅ Found X unique videos already scraped.
New videos to scrape: (Y-X) out of Y
```
**Result:** Only NEW videos are transcribed and appended to table.

#### Test 4: Cross-Channel Deduplication
**Expected behavior:**
- Run DustinVannoy channel → scrapes N videos
- Run CanadianDataGuy channel → if any videos overlap (same videoId), they should be skipped

**Result:** Same table, no duplicate videos even across channels.

---

## Vector Search Ingestion

After blog and video ingestion completes, the `VectorSearchIngestion` task:
1. Unions all preprocessed tables (`preprocessed_blogs`, `preprocessed_youtube_content`)
2. Adds unique IDs and metadata
3. Creates or updates the Databricks Vector Search index

### Configuration
Configured in `data-ingestion.yml`:
```yaml
- task_key: VectorSearchIngestion
  depends_on:
    - task_key: BlogDataIngestion
    - task_key: VideoDataIngestion_DustinVannoy
    - task_key: VideoDataIngestion_CanadianDataGuy
  base_parameters:
    source_tables: "{catalog}.{schema}.preprocessed_blogs,{catalog}.{schema}.preprocessed_youtube_content"
    preprocessed_data_table: "{catalog}.{schema}.brickbrain_delta_table"
    vector_search_endpoint: "brickbrain_endpoint"
    vector_search_index: "{catalog}.{schema}.brickbrain_index"
```

### Notebook
`agent/retriever_tool/vector_search/notebooks/VectorSearchIngestion.py`

---

## Troubleshooting

### Blog Ingestion Issues
- Verify website URLs are accessible
- Check LLM endpoint is available
- Review scraping logic in `blog_scraper.py`

### Video Ingestion Issues

#### No videos fetched
- Check channel name is correct
- Verify YouTube API key is valid (via `setup_brickbrain_workspace.py`)
- Check date filter isn't too restrictive

#### Transcription failures
- Verify proxy credentials are correct (via `setup_brickbrain_workspace.py`)
- Some videos may not have transcriptions available
- Check proxy bandwidth hasn't been exceeded

#### LLM extraction issues
- Ensure the LLM endpoint is accessible
- Check for rate limits on the model endpoint
- Verify the prompt is appropriate for the content

#### Schema errors
- Ensure Unity Catalog and schema exist (run `setup_brickbrain_workspace.py`)
- Check table permissions in Unity Catalog

### Pagination Verification

The `get_channel_videos()` function properly handles pagination:
```python
while len(videos) < max_results:
    params['maxResults'] = min(50, max_results - len(videos))
    if next_page_token:
        params['pageToken'] = next_page_token
    # Fetch page...
    next_page_token = data.get('nextPageToken')
    if not next_page_token:
        break  # No more pages
```

**Tested scenarios:**
- max_videos=2: Fetches 2 videos (single page)
- max_videos=100: Fetches 100 videos across 2 pages (50+50)
- max_videos=1000: Fetches up to 1000 videos across 20 pages

---

## Running the Ingestion Pipeline

### Full Pipeline (all sources)
```bash
cd ssa_agent
databricks bundle deploy
databricks bundle run data_ingestion_job
```

### Individual Tasks
```bash
# Blog ingestion only
databricks jobs run-now <job_id> --task BlogDataIngestion

# YouTube ingestion only (specific channel)
databricks jobs run-now <job_id> --task VideoDataIngestion_DustinVannoy

# Vector search update only
databricks jobs run-now <job_id> --task VectorSearchIngestion
```

---

## File Structure

```
retriever_tool/
├── DATA_INGESTION.md          # This file
├── data_ingestion/
│   ├── blog_data/
│   │   ├── notebooks/
│   │   │   └── DataIngestion.py
│   │   └── utils/
│   │       ├── __init__.py
│   │       ├── blog_scraper.py
│   │       └── chunking.py
│   └── video_data/
│       ├── notebooks/
│       │   └── DataIngestion.py
│       └── utils/
│           ├── __init__.py
│           ├── cleaner.py
│           └── fetch_data.py
└── vector_search/
    └── notebooks/
        └── VectorSearchIngestion.py
```

