# BrickBrain Data Ingestion - Quick Reference

## 🎯 Job Information

**Job Name:** `dev-BrickBrain-data-ingestion`  
**Workspace:** `https://dbc-a657af2e-14d9.cloud.databricks.com`

---

## 📊 Current Data Sources

### Blogs (3 sources)
| Source | Type | URL |
|--------|------|-----|
| Databricksters | Substack | https://www.databricksters.com/ |
| CanadianDataGuy | Substack | https://www.canadiandataguy.com/ |
| AI on Databricks | Medium | https://medium.com/@AI-on-Databricks |

---

## 🗂️ Output Tables

### Development Environment (`brickbrain.brickbrain_dev`)
| Table | Purpose |
|-------|---------|
| `raw_blog_content` | Raw scraped blog posts |
| `preprocessed_blogs` | Chunked blog content with embeddings |
| `brickbrain_delta_table` | Final unified table for vector search |
| `brickbrain_index` | Vector Search index |

### Stage Environment (`brickbrain.brickbrain_stg`)
Same table structure as dev

### Production Environment (`brickbrain.brickbrain_prod`)
Same table structure as dev

---

## ⚡ Quick Commands

### Setup Workspace (First Time or After Workspace Reset)
```bash
cd /Users/jitesh.soni/Documents/Cursor_base/BrickBrain/ssa_agent
python setup_brickbrain_workspace.py
```

### Deploy and Run Job
```bash
cd /Users/jitesh.soni/Documents/Cursor_base/BrickBrain/ssa_agent

# Deploy to dev
databricks bundle deploy -t dev

# Run the job
databricks bundle run data_ingestion_job -t dev
```

### Deploy to Different Environments
```bash
# Stage
databricks bundle deploy -t stage
databricks bundle run data_ingestion_job -t stage

# Prod
databricks bundle deploy -t prod
databricks bundle run data_ingestion_job -t prod
```

### Validate Configuration
```bash
databricks bundle validate -t dev
```

---

## 📊 Query Output Data

### Check Blog Content
```sql
-- Count raw blogs
SELECT domain, COUNT(*) as post_count
FROM brickbrain.brickbrain_dev.raw_blog_content
GROUP BY domain;

-- Count blog chunks
SELECT COUNT(*) as total_chunks
FROM brickbrain.brickbrain_dev.preprocessed_blogs;

-- View sample chunks
SELECT domain, primary_url, content
FROM brickbrain.brickbrain_dev.preprocessed_blogs
LIMIT 5;
```

### Check Combined Data
```sql
-- View unified table
SELECT content_type, domain, COUNT(*) as chunks
FROM brickbrain.brickbrain_dev.brickbrain_delta_table
GROUP BY content_type, domain;

-- Check total records
SELECT COUNT(*) FROM brickbrain.brickbrain_dev.brickbrain_delta_table;
```

### Check Vector Search Index
```sql
-- Check index status via Python
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()
index = vsc.get_index("brickbrain", "brickbrain.brickbrain_dev.brickbrain_index")

# Test similarity search
results = index.similarity_search(
    query_text="How do I optimize Delta tables?",
    num_results=5
)
```

---

## 🔧 Configuration Files

### Main Configuration
- **Bundle Config:** `ssa_agent/databricks.yml`
- **Job Definition:** `ssa_agent/_resources/data-ingestion.yml`
- **Blog Sources:** `ssa_agent/agent/retriever_tool/data_ingestion/blog_data/config.yaml`

### Current Parameters
- **Chunk Size:** 800 tokens
- **Chunk Overlap:** 50 tokens
- **LLM Model:** databricks-meta-llama-3-3-70b-instruct
- **Embedding Model:** databricks-gte-large-en
- **Vector Search Endpoint:** brickbrain

---

## 📝 Task Flow

Current pipeline (YouTube tasks temporarily disabled):

```
1. BlogDataIngestion
   ↓
2. VectorSearchIngestion (drop & recreate index)
```

When YouTube is re-enabled, it will be:
```
1. BlogDataIngestion ────┐
2. VideoDataIngestion_1 ─┼→ 4. VectorSearchIngestion
3. VideoDataIngestion_2 ─┘
```

---

## 🚨 Common Issues & Solutions

### "SCHEMA_NOT_FOUND" Error
**Solution:** The notebooks now auto-create catalog and schema if they don't exist.
```sql
-- If needed, manually create:
CREATE CATALOG IF NOT EXISTS brickbrain;
CREATE SCHEMA IF NOT EXISTS brickbrain.brickbrain_dev;
```

### "RESOURCE_ALREADY_EXISTS" for Vector Index
**Solution:** Fixed! The VectorSearchIngestion now drops and recreates the index on each run.

### Import Errors
**Solution:** Check that `sys.path.append(os.path.join(bundle_root, "agent"))` is set correctly in notebooks.

### Secrets Not Found
**Solution:** Run the workspace setup script:
```bash
python setup_brickbrain_workspace.py
```

---

## 🔍 Monitoring

### Check Job Status via CLI
```bash
# Get latest run
databricks jobs runs list --job-id <JOB_ID> --limit 1

# Get run details
databricks jobs runs get --run-id <RUN_ID>
```

### Check Tables in Databricks SQL Editor
```sql
SHOW TABLES IN brickbrain.brickbrain_dev;
```

### View Task Logs
Visit the job run URL in the Databricks UI to see detailed task logs.

---

## 🎯 Catalog & Schema Structure

### Environments
| Environment | Catalog | Schema |
|-------------|---------|--------|
| Dev | `brickbrain` | `brickbrain_dev` |
| Stage | `brickbrain` | `brickbrain_stg` |
| Prod | `brickbrain` | `brickbrain_prod` |

**Note:** All environments use the same `brickbrain` catalog with different schemas.

---

## 📦 Bundle Structure

```
ssa_agent/
├── databricks.yml              # Main bundle config
├── _resources/
│   ├── data-ingestion.yml      # Job definitions
│   └── artifacts.yml           # App definitions
├── agent/
│   └── retriever_tool/
│       └── data_ingestion/
│           ├── blog_data/      # Blog scraping
│           └── video_data/     # YouTube (currently disabled)
└── setup_brickbrain_workspace.py  # Workspace setup script
```

---

## 🏆 Idempotency Features

✅ **Catalog/Schema Creation:** Auto-creates if not exists  
✅ **Vector Index:** Drops and recreates on each run  
✅ **Delta Tables:** Uses overwrite mode  
✅ **Secrets:** Create only if not exists  

**Result:** You can run the job multiple times without errors!

---

**Last Updated:** October 8, 2025  
**Status:** ✅ Production Ready (Blog ingestion only)
