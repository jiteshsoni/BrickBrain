# Scheduled BrickBrain Queries

This module contains notebooks that query the BrickBrain Knowledge Assistant on a schedule.

## 📁 Contents

- **`databricks.yml`** - Databricks Asset Bundle configuration (workspace and targets)
- **`scheduled_brickbrain_job.yml`** - Job definition with schedule and task configuration
- **`notebooks/ScheduledBrickBrainQuery.py`** - Databricks notebook that queries BrickBrain via MCP bridge with 3 questions about Spark Streaming best practices

## 🏗️ Architecture

This implementation uses the **MCP (Model Context Protocol) Bridge** to query BrickBrain:

```
Notebook → MCP Bridge (Cloudflare Worker) → BrickBrain Knowledge Agent
```

Benefits:
- ✅ No hardcoded endpoint names
- ✅ Consistent with the MCP architecture
- ✅ Simple HTTP requests instead of Databricks SDK complexity
- ✅ Centralized configuration in MCP server
- ✅ Same authentication as the shell script

## 🚀 Deployment

### Option 1: Using Databricks Asset Bundles (Recommended)

1. **Install Databricks CLI**:
   ```bash
   pip install databricks-cli
   ```

2. **Validate the bundle**:
   ```bash
   cd /Users/jitesh.soni/Documents/Cursor_base/BrickBrain/ssa_agent/scheduled_queries
   databricks bundle validate
   ```

3. **Deploy to Databricks**:
   ```bash
   databricks bundle deploy
   ```

4. **Run the job manually (optional)**:
   ```bash
   databricks bundle run scheduled_brickbrain_query
   ```

### Option 2: Manual Setup via Databricks UI

**Note**: Using Databricks Asset Bundles (Option 1) is strongly recommended for production use.

1. **Upload the notebook**:
   - Go to your Databricks workspace
   - Navigate to **Workspace** → **Users** → your email
   - Create folder: `ssa_agent/scheduled_queries/notebooks/`
   - Upload `ScheduledBrickBrainQuery.py`

2. **Create a job**:
   - Go to **Workflows** → **Jobs**
   - Click **Create Job**
   - **Name**: "Scheduled BrickBrain Query - Spark Streaming Best Practices"
   - **Task**: Notebook task pointing to the uploaded notebook
   - **Compute**: Serverless (required for this workspace)
   - **Schedule**: 
     - Type: Cron
     - Expression: `0 0/25 * * * ?` (every 25 minutes)
     - Timezone: America/Los_Angeles

3. **Configure notifications** (optional):
   - Add email notifications for failures
   - Add Slack webhook for alerts

## 📊 Monitoring

The notebook logs:
- Query timestamp
- All 3 questions asked
- Responses received for each question
- MCP URL used
- Success/failure status for each question
- Overall execution summary

You can enhance this by:
- Creating a Delta table to store all query results
- Setting up dashboards to visualize response times
- Adding alerting for failed queries
- Tracking response quality over time
- Comparing answers across different runs

## 🔧 Configuration

### Update the Questions

Edit the notebook and change the `questions` list:
```python
questions = [
    "Your first question",
    "Your second question",
    "Your third question"
]
```

### Change Schedule Frequency

Edit `scheduled_brickbrain_job.yml`:
```yaml
schedule:
  quartz_cron_expression: "0 0/25 * * * ?"  # Change this
```

Common schedules:
- Every 5 minutes: `"0 0/5 * * * ?"`
- Every 15 minutes: `"0 0/15 * * * ?"`
- Every 20 minutes: `"0 0/20 * * * ?"`
- Every 25 minutes: `"0 0/25 * * * ?"` (current)
- Every 30 minutes: `"0 0/30 * * * ?"`
- Every hour: `"0 0 * * * ?"`

### Change MCP Bridge URL

Edit the notebook and update the MCP configuration:
```python
MCP_URL = "https://your-mcp-bridge.workers.dev/mcp"
MCP_KEY = "your-mcp-key"
```

Note: The actual Knowledge Agent endpoint is configured in the MCP server, not in the notebook.

## 🎯 Use Cases

This scheduled query pattern can be used for:

1. **Monitoring**: Regular health checks of the Knowledge Agent
2. **Benchmarking**: Track response quality and consistency over time
3. **Alerting**: Detect when the Knowledge Agent is down or slow
4. **Testing**: Validate new versions of the Knowledge Agent
5. **Documentation**: Generate periodic reports on specific topics
6. **Training**: Collect training data for fine-tuning

## 📝 Example Output

```
🧠 Starting BrickBrain Multi-Question Query at 2025-10-10T15:20:00
================================================================================

📝 Question 1: Give me spark streaming best practices
⏳ Querying BrickBrain Knowledge Assistant via MCP...
================================================================================
✅ Response 1 received from BrickBrain:
[detailed response here]
================================================================================

📝 Question 2: Give me top 5 spark streaming best practices to save money
⏳ Querying BrickBrain Knowledge Assistant via MCP...
================================================================================
✅ Response 2 received from BrickBrain:
[detailed response here]
================================================================================

📝 Question 3: Give me top 5 spark streaming best practices for operational excellence
⏳ Querying BrickBrain Knowledge Assistant via MCP...
================================================================================
✅ Response 3 received from BrickBrain:
[detailed response here]
================================================================================

📊 FINAL QUERY SUMMARY
================================================================================
⏰ Timestamp: 2025-10-10T15:20:45
🔗 MCP URL: https://brickbrain-mcp-bridge.get2jitesh.workers.dev/mcp
📝 Total Questions: 3
✅ Successful: 3
❌ Failed: 0
🎯 Overall Status: SUCCESS

--------------------------------------------------------------------------------
QUESTIONS ASKED:
--------------------------------------------------------------------------------
✅ Q1: Give me spark streaming best practices
✅ Q2: Give me top 5 spark streaming best practices to save money
✅ Q3: Give me top 5 spark streaming best practices for operational excellence
```

## 🔗 Related

- [BrickBrain MCP Server](../ask_brickbrain/)
- [MCP Bridge Script](../../cloudflare-mcp-worker/mcp-bridge.sh)
- [Knowledge Agent Setup](../agent/)

## 📧 Support

For issues or questions:
- Check notebook execution logs in Databricks
- Review job run history in Workflows UI
- Verify Knowledge Agent endpoint is running
- Check Databricks authentication credentials

