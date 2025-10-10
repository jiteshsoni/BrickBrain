# BrickBrain Table Lineage & Dependency Graph

## Overview

This document maps the complete table lineage for the BrickBrain data ingestion pipeline, showing how data flows from external sources through multiple processing stages to the final vector search index.

---

## Table Dependency Graph

```
External Sources
    │
    ├─── Blog Websites (databricksters.com, canadiandataguy.com, etc.)
    │       │
    │       ▼
    │    [Task: BlogDataIngestion]
    │       │
    │       ▼
    │    raw_blog_content (APPEND)
    │
    └─── YouTube Channels (DustinVannoy, CanadianDataGuy)
            │
            ├─── [Task: VideoDataIngestion_DustinVannoy]
            │       │
            └─── [Task: VideoDataIngestion_CanadianDataGuy]
                    │
                    ▼
                 raw_youtube_content (APPEND)

                    │
                    ▼
    ┌───────────────┴───────────────┐
    │                               │
    │         [Task: ChunkingTask]  │
    │     (reads both raw tables)   │
    │                               │
    └───────────────┬───────────────┘
                    │
                    ▼
            preprocessed_content (OVERWRITE)
                    │
                    │ (in-memory processing)
                    ▼
            preprocessed_content_chunked (OVERWRITE)
                    │
                    ▼
            [Task: VectorSearchIngestion]
                    │
                    ▼
            brickbrain_delta_table (OVERWRITE, with CDF enabled)
                    │
                    │ (source table for index)
                    ▼
            brickbrain_index (Vector Search Index - Delta Sync)
                    │
                    ▼
            [Task: KnowledgeAssistantSync]
                    │
                    ▼
            Knowledge Assistant (synced)
```

---

## Table Catalog

### 1. Raw Tables (Incremental - APPEND mode)

#### `{catalog}.{schema}.raw_blog_content`
- **Written by**: `BlogDataIngestion` task
- **Mode**: APPEND (incremental ingestion, only new URLs scraped)
- **Source**: External blog websites
- **Schema**:
  - `url` (String) - Blog post URL
  - `title` (String) - Blog post title  
  - `content` (String) - Raw markdown content
  - `published_date` (String) - Publication date
  - `domain` (String) - Source website domain
  - `scrape_timestamp` (Timestamp) - When scraped
- **Purpose**: Stores original scraped blog content in markdown format

#### `{catalog}.{schema}.raw_youtube_content`
- **Written by**: 
  - `VideoDataIngestion_DustinVannoy` task
  - `VideoDataIngestion_CanadianDataGuy` task
- **Mode**: APPEND (incremental ingestion, only new video IDs scraped)
- **Source**: YouTube Data API v3 + YouTube Transcript API
- **Schema**:
  - `id` (Map<String,String>) - YouTube video ID and kind
  - `snippet` (Map<String,String>) - Video metadata (title, description, publishedAt, etc.)
  - `transcription` (String) - Cleaned video transcript
  - `transcription_status` (String) - "Success" or error message
- **Purpose**: Stores raw video transcriptions and metadata

---

### 2. Preprocessed Tables (Full Refresh - OVERWRITE mode)

#### `{catalog}.{schema}.preprocessed_content`
- **Written by**: `ChunkingTask` task
- **Mode**: OVERWRITE (deterministic full refresh)
- **Reads from**: 
  - `raw_blog_content` (all rows)
  - `raw_youtube_content` (all rows)
- **Schema**:
  - `url` (String) - Content URL
  - `domain` (String) - Source domain
  - `content` (String) - AI-extracted technical content
  - `content_type` (String) - "blog" or "video"
- **Processing**:
  - Applies LLM (Llama 3.3 70B) to extract key technical content
  - Filters out navigation, ads, boilerplate
  - Unifies blog and video data into single schema
- **Purpose**: Contains clean, AI-extracted content ready for chunking

#### `{catalog}.{schema}.preprocessed_content_chunked`
- **Written by**: `ChunkingTask` task
- **Mode**: OVERWRITE (deterministic full refresh)
- **Reads from**: `preprocessed_content` (in-memory, same task)
- **Schema**:
  - `url` (String) - Content URL
  - `domain` (String) - Source domain
  - `content` (String) - Individual chunk text
  - `content_type` (String) - "blog" or "video"
  - `chunk_id` (BigInt) - Sequential chunk number within document (0, 1, 2, ...)
- **Processing**:
  - Chunks content using LangChain MarkdownHeaderTextSplitter
  - Max chunk size: 800 tokens (configurable)
  - Chunk overlap: 50 tokens (configurable)
  - Each document explodes into multiple rows (1 row per chunk)
- **Purpose**: Contains chunked content ready for vector embeddings

---

### 3. Vector Search Tables (Full Refresh - OVERWRITE mode)

#### `{catalog}.{schema}.brickbrain_delta_table`
- **Written by**: `VectorSearchIngestion` task
- **Mode**: OVERWRITE (deterministic full refresh)
- **Reads from**: `preprocessed_content_chunked` (all rows)
- **Schema**:
  - `id` (BigInt) - Deterministic hash of (url + chunk_id) for deduplication
  - `url` (String) - Content URL
  - `domain` (String) - Source domain
  - `content` (String) - Chunk text
  - `content_type` (String) - "blog" or "video"
  - `chunk_id` (BigInt) - Sequential chunk number
- **Table Properties**:
  - `delta.enableChangeDataFeed = true` - Enables efficient delta sync
  - `overwriteSchema = true` - Allows schema evolution
- **Processing**:
  - Adds deterministic `id` = hash(url + "_" + chunk_id)
  - Same content chunk gets same ID across runs (prevents duplicates)
- **Purpose**: Delta table source for vector search index with CDF enabled for efficient updates

#### `{catalog}.{schema}.brickbrain_index` (Vector Search Index)
- **Created by**: `VectorSearchIngestion` task
- **Type**: Delta Sync Index (TRIGGERED pipeline)
- **Source Table**: `brickbrain_delta_table`
- **Embedding Model**: `databricks-gte-large-en` (Foundation Model API)
- **Configuration**:
  - `primary_key`: `id`
  - `embedding_source_column`: `content`
  - `columns_to_sync`: `url`, `content_type`, `domain`, `chunk_id`
  - `pipeline_type`: `TRIGGERED` (manual sync, not continuous)
- **Sync Behavior**:
  - **Initial creation**: Indexes all rows from source table
  - **Subsequent syncs**: Uses Change Data Feed to only process changes
  - **Force rebuild**: Drops and recreates index (when `force_rebuild=true`)
- **Purpose**: Vector search index for semantic similarity search

---

### 4. Knowledge Assistant

#### Knowledge Assistant (External Resource)
- **Synced by**: `KnowledgeAssistantSync` task
- **Source**: `brickbrain_index` (Vector Search Index)
- **Purpose**: Databricks Knowledge Assistant that uses the vector index for RAG

---

## Data Flow Summary

### Stage 1: Raw Data Ingestion (Incremental)
```
External Sources → [BlogDataIngestion] → raw_blog_content (APPEND)
External Sources → [VideoDataIngestion_*] → raw_youtube_content (APPEND)
```
- **Idempotency**: URL-based deduplication (only new URLs scraped)
- **Write Mode**: APPEND
- **When**: Each data ingestion job run

### Stage 2: Preprocessing & Chunking (Full Refresh)
```
raw_blog_content + raw_youtube_content → [ChunkingTask] → 
    preprocessed_content (OVERWRITE)
        ↓ (in-memory processing)
    preprocessed_content_chunked (OVERWRITE)
```
- **Idempotency**: Full deterministic reprocessing of all raw data
- **Write Mode**: OVERWRITE (ensures no duplicates)
- **When**: After any new data is added to raw tables
- **Processing**: AI extraction + chunking (sequential stages)
- **Key**: preprocessed_content is written first, then immediately used in-memory for chunking

### Stage 3: Vector Search Indexing (Full Refresh + Delta Sync)
```
preprocessed_content_chunked → [VectorSearchIngestion] → 
    brickbrain_delta_table (OVERWRITE with CDF)
        ↓ (source table for index)
    brickbrain_index (Delta Sync from CDF)
```
- **Idempotency**: 
  - Table overwrite ensures deterministic output
  - Deterministic IDs prevent duplicate embeddings
  - Delta sync only processes changes
- **Write Mode**: OVERWRITE (table), SYNC (index)
- **When**: After chunking completes
- **Processing**: ID generation → Delta table write → Vector index sync
- **Key**: brickbrain_delta_table is the source table for brickbrain_index

### Stage 4: Knowledge Assistant Sync
```
brickbrain_index → [KnowledgeAssistantSync] → Knowledge Assistant
```
- **Purpose**: Updates Knowledge Assistant to use latest vector index
- **When**: After vector index sync completes

---

## Key Design Decisions

### 1. **Hybrid Incremental/Full-Refresh Architecture**
- **Raw tables**: APPEND mode for efficiency (only new data fetched)
- **Downstream tables**: OVERWRITE mode for determinism (no duplicate concerns)
- **Rationale**: 
  - Incremental at source minimizes API calls and scraping time
  - Full refresh downstream ensures data quality and consistency
  - Deterministic IDs prevent vector index duplicates despite OVERWRITE mode

### 2. **Deterministic ID Generation**
- Formula: `id = hash(url + "_" + chunk_id)`
- **Benefits**:
  - Same content chunk gets same ID across runs
  - Prevents duplicate embeddings in vector index
  - Enables efficient delta sync (only changed chunks re-embedded)

### 3. **Change Data Feed (CDF) for Vector Search**
- Enabled on `brickbrain_delta_table`
- **Benefits**:
  - Efficient delta sync (only process changed rows)
  - Faster vector index updates
  - Lower compute costs for embeddings

### 4. **Separation of Concerns**
- **BlogDataIngestion**: Blog scraping only
- **VideoDataIngestion**: Video transcription only
- **ChunkingTask**: AI extraction + chunking (unified for both types)
- **VectorSearchIngestion**: Vector embedding + indexing

---

## Table Name Conventions

All tables follow the pattern: `{catalog}.{schema}.{table_name}`

### Environment-Specific Catalogs & Schemas

| Environment | Catalog | Schema | Example Full Table Name |
|-------------|---------|--------|------------------------|
| **dev** | `brickbrain` | `brickbrain_dev` | `brickbrain.brickbrain_dev.raw_blog_content` |
| **stage** | `brickbrain` | `brickbrain_stg` | `brickbrain.brickbrain_stg.raw_blog_content` |
| **prod** | `brickbrain` | `brickbrain_prod` | `brickbrain.brickbrain_prod.raw_blog_content` |

---

## Task Dependencies

### Job: `data_ingestion_job`

```
BlogDataIngestion ──┐
                    │
VideoDataIngestion_DustinVannoy ──┤
                                  ├─→ ChunkingTask → VectorSearchIngestion → KnowledgeAssistantSync
VideoDataIngestion_CanadianDataGuy ──┘
```

**Task Execution Order**:
1. **Parallel**: `BlogDataIngestion`, `VideoDataIngestion_DustinVannoy`, `VideoDataIngestion_CanadianDataGuy`
2. **Sequential** (after all ingestion tasks complete): `ChunkingTask`
3. **Sequential** (after chunking): `VectorSearchIngestion`
4. **Sequential** (after vector indexing): `KnowledgeAssistantSync`

---

## Configuration

All table names are parameterized in `databricks.yml` and `data-ingestion.yml`:

```yaml
variables:
  uc_catalog: "brickbrain"
  schema: "brickbrain_dev"  # or brickbrain_stg, brickbrain_prod
  
  # Chunking parameters
  chunk_size: "800"         # max tokens per chunk
  chunk_overlap: "50"       # overlapping tokens
  
  # Vector search
  vector_search_endpoint: "brickbrain"
  knowledge_assistant_id: "1ff550b7-4fc9-4692-aa7c-e57e22e40076"
```

---

## Monitoring & Validation

### Data Quality Checks

1. **Raw Tables**: Count checks after ingestion
2. **Chunking**: Expansion ratio validation (chunks/documents)
3. **Vector Index**: Row count validation (source vs index)
4. **Schema**: Required column validation before indexing

### Key Metrics to Monitor

- **Raw table growth**: New blogs/videos per run
- **Chunk expansion ratio**: Average chunks per document
- **Vector index size**: Total embedded chunks
- **Sync duration**: Time for vector index delta sync

---

## Troubleshooting

### "Schema mismatch" errors
- **Cause**: Schema changes in source data
- **Fix**: May require manual table recreation for raw tables (APPEND mode limitation)

### "Duplicate chunks" in vector index
- **Cause**: Hash collision (very rare) or missing deterministic ID
- **Fix**: Check ID generation logic in `VectorSearchIngestion`

### Vector index not updating
- **Cause**: Change Data Feed not enabled or sync not triggered
- **Fix**: Verify CDF on `brickbrain_delta_table`, check sync logs

### Missing data in Knowledge Assistant
- **Cause**: Sync task didn't run or failed
- **Fix**: Run `KnowledgeAssistantSync` task manually

---

## Future Enhancements

1. **Incremental Preprocessing**: Change chunking to also be incremental (requires tracking processed URLs)
2. **Partition Strategy**: Partition raw tables by date for better query performance
3. **Data Retention**: Implement retention policy for old/stale content
4. **Quality Metrics**: Track LLM extraction quality and chunk quality scores

---

*Last Updated: October 10, 2025*
*Maintained by: BrickBrain Team*
