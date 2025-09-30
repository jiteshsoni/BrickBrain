# BrickBrain

## Overview

BrickBrain is an **SSA Agent** designed to revolutionize how Solutions Architects access and utilize domain-specific expertise. This intelligent agent addresses a critical gap in current tooling where generic LLMs like ChatGPT fail to deliver reliable, context-specific guidance for Databricks and Spark Streaming technologies:

- **Generic ChatGPT/LLMs**: Provide generic answers with poor understanding of Databricks/Spark best practices
- **Static docs and blogs**: Valuable but fragmented and difficult to query at scale  
- **SSA expertise**: Strong but memory-limited, unable to recall extensive content on demand
- **Slack threads**: Useful but ephemeral and hard to synthesize

### Our Solution

BrickBrain is a complete knowledge agent that collects, processes, and intelligently serves curated, high-quality content from hand-picked expert sources including **Databricksters (blog)**, **Canadian Data Guy (blog)**, **Dustin Vannoy (Youtube channel)**, and other domain authorities. The agent stores this curated content in Delta tables optimized for Vector Search and has already demonstrated promising results during QBRs by generating in-depth, high-quality answers that outperform general-purpose LLMs.

#### Products Used

- **Agent Bricks**: to create the intelligent agent
- **Vector Search**: to create the searchable content index  
- **Lakebase**: to store conversation history and enable persistence
- **Databricks Apps**: to run the slackbot and the chatbot UI
- **Lakeflow Jobs**: to orchestrate the data ingestion workflows
- **MLflow**: for experiment tracking and feedback integration

### Feedback Loop

Using Agent Bricks automatic steering, we sync feedback and rationale from Slack to Unity Catalog via MLflow Experiments. These traces are transformed and merged back to Agent Bricks to improve quality over time.

Lakebase conversations and MLflow experiments can also be analyzed to identify knowledge gaps, enabling Specialists to create targeted content on relevant topics.

The ultimate goal is to create a **self-reinforcing feedback loop** that:

- Reduces ASQs (5-10% potential reduction)
- Integrates with daily tools (Cursor IDE, Slack)
- Improves through user feedback and new content
- Establishes continuous knowledge enhancement

```text
BrickBrain/
├── ssa_agent/                           # Main application directory
│   ├── agent/                           # Agent configuration and tools
│   │   ├── agent_brick.yaml             # Agent Brick configuration
│   │   └── retriever_tool/              # Data ingestion and search tools
│   │       ├── data_ingestion/          # Data preparation modules
│   │       │   ├── blog_data/           # Blog data ingestion
│   │       │   │   ├── notebooks/
│   │       │   │   │   ├── DataIngestion.py      # Blog scraping notebook
│   │       │   │   │   └── SingleUrlIngestion.py # Single URL processor
│   │       │   │   ├── utils/
│   │       │   │   │   ├── blog_scraper.py       # Core blog scraper logic
│   │       │   │   │   └── chunking.py           # Text chunking utilities
│   │       │   │   └── requirements.txt          # Blog-specific dependencies
│   │       │   └── video_data/          # Video data ingestion
│   │       │       ├── notebooks/
│   │       │       │   └── DataIngestion.py      # Video ingestion notebook
│   │       │       └── utils/
│   │       │           ├── __init__.py           # Package initialization
│   │       │           ├── fetch_data.py         # YouTube API clients
│   │       │           └── cleaner.py            # Text cleaning utilities
│   │       └── vector_search/           # Vector search implementation
│   │           ├── notebooks/
│   │           │   └── VectorSearchIngestion.py  # Vector index creation
│   │           └── utils/
│   │               └── data_helper.py            # Search utilities
│   ├── ask_brickbrain/                  # Claude MCP Server integration
│   │   ├── brickbrain_mcp.py            # MCP server implementation
│   │   ├── app.yaml                     # App configuration
│   │   ├── setup.sh                     # Setup script
│   │   └── requirements.txt             # MCP dependencies
│   ├── chatapp/                         # Web chat interface
│   │   └── app/
│   │       ├── app.py                   # Streamlit chat application
│   │       ├── conversation.py          # Conversation management
│   │       ├── document_processor.py    # Document processing
│   │       └── model_serving_utils.py   # Model serving utilities
│   ├── slackbot/                        # Slack integration
│   │   ├── app/
│   │   │   ├── app.py                   # Slack bot application
│   │   │   ├── conversation.py          # Conversation handling
│   │   │   └── mlflow_client.py         # MLflow integration
│   │   └── notebooks/
│   │       └── LaunchApp.py             # Deployment notebook
│   ├── _resources/                      # Configuration files
│   │   ├── artifacts.yml                # Artifact configuration
│   │   ├── data-ingestion.yml           # Pipeline configuration
│   │   └── ...                         
│   └── databricks.yml                   # Databricks bundle config
├── DEVELOPMENT_RULES.md                 # Development guidelines
├── problem_statement.md                 # Project overview and problem definition
└── README.md                            # This file
```

## Data Ingestion

### Blog Data Pipeline

The blog data pipeline begins with automated discovery, where the scraper reads sitemaps and RSS feeds to identify all available blog URLs from target websites. These discovered URLs are then filtered using carefully crafted regex patterns to distinguish actual blog posts from other types of pages such as category listings, author pages, or navigation elements. Once relevant URLs are identified, the system fetches the content by downloading and parsing the HTML using BeautifulSoup. The raw HTML content undergoes conversion to clean markdown format, preserving the structure and readability while removing unnecessary formatting elements. Finally, the processed content is stored in Delta Lake using merge (upsert) operations to ensure data consistency and prevent duplicates, while scraping statistics and comprehensive URL lists are maintained in a separate metadata table for monitoring and auditing purposes.

### Video Data Pipeline

The video data pipeline starts with channel discovery, utilizing the YouTube Data API to fetch comprehensive channel information from either usernames or handles provided by the user. The system then enumerates all videos from the identified channel, with configurable limits to control the scope of data collection. Video transcripts are retrieved through parallel transcription processes, employing multi-threaded fetching via the YouTube Transcript API to maximize efficiency and reduce processing time. To handle potential rate limiting and IP blocking issues, the pipeline incorporates proxy rotation using Webshare proxy services, ensuring reliable access to YouTube's APIs. Raw transcriptions along with their associated metadata are initially saved to Delta Lake in their unprocessed form. The pipeline then applies comprehensive text processing through advanced NLP cleaning techniques, including the removal of common filler words such as "um," "uh," and "like," spell correction using the NLTK word corpus, integration of custom vocabularies for technical terms specific to the domain, and thorough text normalization and formatting. Once the cleaning process is complete, the refined transcriptions are saved to a separate Delta table, providing both raw and processed versions of the video content for different analytical needs.

```text
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│   Data Sources      │    │   Processing Layer  │    │    Delta Tables     │
│                     │    │                     │    │                     │
│ ┌─────────────────┐ │    │ ┌─────────────────┐ │    │ ┌─────────────────┐ │
│ │   Blog Sites    │ │───▶│ │  Blog Scraper   │ │───▶│ │  Blog Content   │ │
│ │• Databricksters │ │    │ │• Sitemap Parser │ │    │ │• Raw Content    │ │
│ │• CanadianDataGuy│ │    │ │• Content Fetch  │ │    │ │• Metadata       │ │
│ │• Custom URLs    │ │    │ │• HTML→Markdown  │ │    │ │                 │ │
│ └─────────────────┘ │    │ └─────────────────┘ │    │ └─────────────────┘ │
│                     │    │                     │    │                     │
│ ┌─────────────────┐ │    │ ┌─────────────────┐ │    │ ┌─────────────────┐ │
│ │  YouTube APIs   │ │───▶│ │ Video Processor │ │───▶│ │ Video Content   │ │
│ │• YouTube Data   │ │    │ │• Channel Videos │ │    │ │• Raw Transcripts│ │
│ │• Transcript API │ │    │ │• Transcriptions │ │    │ │• Cleaned Text   │ │
│ │• Proxy Support  │ │    │ │• Text Cleaning  │ │    │ │• Metadata       │ │
│ └─────────────────┘ │    │ └─────────────────┘ │    │ └─────────────────┘ │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
```

## Quick Start

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Access to MLflow, Agent Bricks, Lakebase, and Vector Search
- Python 3.8+ environment

### Configure Databricks Asset Bundle

Use `databricks.yml` as a starting point and update the variables and configuration to your needs. 

The command `databricks bundle deploy` will create all necessary assets defined in this DAB. Note that deployment might not work as the Agent Brick KA is not defined here. We will update this as DABs updates. 

## Configuration

### Websites to Scrape

The scraper targets these websites by default:

- `https://www.databricksters.com/`
- `https://www.canadiandataguy.com/`

In the future, we are adding capabilities to the Slackbot to accept requests for different blogs and youtube videos. 

## Credits

We used the following resources as guides for development:

- **Slack Bot**: [Databricks Apps Collection Slack Bot](https://github.com/alex-lopes-databricks/databricks_apps_collection/blob/main/slack-bot/manifest.yml) as a guide for generating the Slackbot application
- **Lakebase Integration**: Robert Mosely's guide to using Lakebase for conversation checkpointing
- **Medium Article**: [Let's Create a Slackbot, Cause Why Not?](https://python.plainenglish.io/lets-create-a-slackbot-cause-why-not-2972474bf5c1) for additional Slack bot development insights

## Future Work

- [Adding interactivity to the MCP server](https://glama.ai/mcp/servers/@noopstudios/interactive-feedback-mcp)
- Sending new blogs via Slack to be ingested and used by BrickBrain
- Expanding data sources to include additional domain expert content
- Enhanced feedback loop integration with Agent Bricks for continuous improvement

## License

This project is for educational and research purposes.
