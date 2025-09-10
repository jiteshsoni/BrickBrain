# BrickBrain - Blog Scraper

A Python-based blog scraper that automatically discovers and downloads blog posts from specified websites, converting them to markdown and storing them in Delta Lake tables.

## Features

- **Automatic Blog Discovery**: Discovers blog URLs from sitemaps and RSS feeds
- **Content Conversion**: Converts HTML content to clean markdown format
- **Delta Lake Storage**: Stores blog content in Delta Lake tables with ACID properties
- **Metadata Separation**: Maintains separate tables for blog content and scraping metadata
- **Databricks Integration**: Designed to run on Databricks with serverless compute
- **Scheduled Execution**: Can be run as scheduled Databricks jobs
- **Incremental Processing**: Uses merge (upsert) operations to handle updates

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Websites      │    │   Blog Scraper   │    │  Delta Tables   │
│                 │    │                  │    │                 │
│ • Databricksters│───▶│ • Sitemap Parser │───▶│ • Blog Content  │
│ • CanadianDataGuy│    │ • Content Fetch  │    │ • Metadata      │
│ • Custom URLs   │    │ • HTML→Markdown  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Project Structure

```
BrickBrain/
├── blog_scraper.py              # Core scraper logic
├── blog_scraper_notebook.py     # Databricks notebook entry point
├── test_scraper.py              # Local testing script
├── urls.txt                     # Static list of URLs to scrape
├── job_config.json              # Databricks job configuration
├── requirements.txt             # Python dependencies
├── DEVELOPMENT_RULES.md         # Development guidelines
└── README.md                    # This file
```

## Quick Start

### Local Testing with Databricks Connect

1. **Setup Environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure Databricks Connect**:
   ```bash
   databricks-connect test
   ```

3. **Run Local Test**:
   ```bash
   python test_scraper.py
   ```

### Deploy to Databricks

1. **Push to GitHub**:
   ```bash
   git add -A
   git commit -m "Your changes"
   git push origin scrape_blogs
   ```

2. **Create Databricks Job**:
   ```bash
   databricks jobs create --json @job_config.json
   ```

3. **Run Job**:
   ```bash
   databricks jobs run-now <job-id>
   ```

## Configuration

### Websites to Scrape

The scraper targets these websites by default:
- `https://www.databricksters.com/`
- `https://www.canadiandataguy.com/`

Additional URLs can be added to `urls.txt` for static scraping.

### Delta Tables

- **Blog Content**: `main.default.blog_content`
  - `url` (string): Blog post URL (primary key)
  - `title` (string): Blog post title
  - `markdown_content` (string): Full content in markdown format
  - `domain` (string): Source domain
  - `scraped_at` (timestamp): When the content was scraped

- **Metadata**: `main.default.blog_content_metadata`
  - `metadata_type` (string): Type of metadata (URL_LIST, SCRAPING_SUMMARY)
  - `metadata_content` (string): JSON metadata content
  - `created_at` (timestamp): When metadata was created

## Dependencies

- `requests`: HTTP client for fetching web content
- `beautifulsoup4`: HTML parsing and content extraction
- `html2text`: HTML to markdown conversion
- `lxml`: XML/HTML parser
- `urllib3`: HTTP client utilities
- `databricks-connect`: Local development with Databricks

## Development Guidelines

See [DEVELOPMENT_RULES.md](DEVELOPMENT_RULES.md) for detailed development guidelines including:
- Files never to commit
- DRY principles
- Databricks-specific rules

## How It Works

1. **Discovery**: The scraper reads sitemaps and RSS feeds to discover blog URLs
2. **Filtering**: URLs are filtered using regex patterns to identify blog posts
3. **Fetching**: Content is downloaded and parsed using BeautifulSoup
4. **Conversion**: HTML content is converted to clean markdown
5. **Storage**: Content is stored in Delta Lake using merge operations
6. **Metadata**: Scraping statistics and URL lists are stored in a separate metadata table

## Monitoring

The scraper provides detailed logging and stores metadata including:
- Total URLs discovered vs. hardcoded
- Successful vs. failed downloads
- Scraping timestamps and duration
- Source website information

## License

This project is for educational and research purposes.