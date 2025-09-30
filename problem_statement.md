# BrickBrain: Specialized Knowledge Agent for Databricks SSAs

## Overview

BrickBrain is a complete knowledge agent system that ingests, processes, and intelligently serves curated Databricks expertise from domain authorities including Databricksters, Canadian Data Guy, Dustin Vannoy's YouTube channel, and other hand-picked sources. The system combines automated data pipelines, vector search capabilities, and multiple deployment interfaces to deliver context-specific Databricks guidance.

## Problem Statement

Current tools (ChatGPT, static docs, Slack threads) fail to deliver reliable, context-specific guidance for Databricks and Spark Streaming. SSAs struggle to recall and synthesize the growing body of content (79+ blogs, docs, and videos). There's an opportunity to:

- **Codify expertise** into a knowledge agent
- **Reduce ASQs** (5–10% potential reduction) by enabling the agent to answer directly to SAs in Slack
- **Boost SSA productivity** by providing comprehensive starting points for ASQs
- **Establish a self-reinforcing feedback loop** that continuously improves as new content is created

### Current Solutions Are Inadequate

- **Generic ChatGPT/LLMs** → Generic answers, poor understanding of Spark/Databricks best practices
- **Static docs, blogs, YouTube** → Fragmented and conflicting expertise can lead to confusion
- **SSA expertise** → Limited by memory and bandwidth

No existing solution combines memory + adaptiveness + integration into daily tools.

## Our Solution
#### Products used
- **Agent Bricks**: to create the agent
- **Vector Search**: to create the index
- **Lakebase**: to store conversation history
- **Databricks Apps**: to run the slackbot and the chatbot UI
- **Lakeflow Jobs**: to orchestrate the workflows

Created a Vector Search index using data sources from blogs, like Databricksters, Canadian Data Guy Substack, and the AI on Databricks Medium, and Youtube videos, like Dustin Vannoy. These pipelines are easily expandable. 

### Feedback Loop
Using Agent Bricks automatic steering, we sync feedback and rationale from Slack to a Unity Catalog via the MLflow Experiment. These traces are then transformed simply and then pushed back and merged to Agent Bricks to improve the quality. 

Lakebase conversations and the MLflow experiment can also be used to analyze any knowledge gaps-- so Specialists can create more blogs on relevant topics. 

# Future Work 
- [Adding interactivity to the MCP server](https://glama.ai/mcp/servers/@noopstudios/interactive-feedback-mcp)
- Sending new blogs via Slack to be ingested + used by Brickbrain