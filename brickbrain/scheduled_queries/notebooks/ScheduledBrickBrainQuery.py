# Databricks notebook source
# MAGIC %md
# MAGIC # Scheduled BrickBrain Query
# MAGIC 
# MAGIC This notebook demonstrates querying the BrickBrain Knowledge Assistant on a schedule.
# MAGIC 
# MAGIC **Questions:** 
# MAGIC 1. Give me spark streaming best practices
# MAGIC 2. Give me top 5 spark streaming best practices to save money
# MAGIC 3. Give me top 5 spark streaming best practices for operational excellence
# MAGIC 
# MAGIC **Schedule:** Runs every 25 minutes
# MAGIC 
# MAGIC **Purpose:** 
# MAGIC - Demonstrates MCP/Knowledge Agent integration
# MAGIC - Can be used for monitoring, periodic knowledge checks, or automated Q&A

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import json
import requests
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## MCP Bridge Configuration
# MAGIC 
# MAGIC Using the MCP bridge to query BrickBrain Knowledge Assistant

# COMMAND ----------

# MCP Bridge URL and authentication
MCP_URL = "https://brickbrain-mcp-bridge.get2jitesh.workers.dev/mcp"
MCP_KEY = "3eacb5a9b446c3507dfa5b84dc0e335a7c9f5e70801579ee9f35247fcd1c7369"

def ask_brickbrain_via_mcp(question):
    """
    Call BrickBrain through the MCP bridge
    
    Args:
        question: The question to ask
        
    Returns:
        dict: MCP response containing the answer
    """
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "ask_brickbrain",
            "arguments": {
                "prompt": question  # Cloudflare Worker expects "prompt", not "question"
            }
        }
    }
    
    response = requests.post(
        MCP_URL,
        headers={
            "Content-Type": "application/json",
            "x-mcp-key": MCP_KEY
        },
        json=request,
        timeout=180  # 3 minutes for long-running queries
    )
    
    response.raise_for_status()
    return response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query BrickBrain

# COMMAND ----------

# Initialize query
print(f"🧠 Starting BrickBrain Multi-Question Query at {datetime.now().isoformat()}")
print("=" * 80)

# Define the questions we want to ask in sequence
questions = [
    "Give me spark streaming best practices",
    "Give me top 5 spark streaming best practices to save money",
    "Give me top 5 spark streaming best practices for operational excellence"
]

# Store all results
all_results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 1: General Best Practices

# COMMAND ----------

# Question 1
print(f"\n📝 Question 1: {questions[0]}\n")
print("⏳ Querying BrickBrain Knowledge Assistant via MCP...")
print("=" * 80)

try:
    mcp_response_1 = ask_brickbrain_via_mcp(questions[0])
    
    # Extract the actual response text from MCP response
    if "result" in mcp_response_1 and "content" in mcp_response_1["result"]:
        response_1 = mcp_response_1["result"]["content"][0]["text"]
    else:
        response_1 = json.dumps(mcp_response_1, indent=2)
    
    print("\n✅ Response 1 received from BrickBrain:")
    print("=" * 80)
    print(response_1)
    print("=" * 80)
    
    all_results.append({
        "question_number": 1,
        "question": questions[0],
        "response": response_1,
        "status": "success"
    })
    
except requests.exceptions.RequestException as e:
    print(f"\n❌ Error querying BrickBrain via MCP: {str(e)}")
    all_results.append({
        "question_number": 1,
        "question": questions[0],
        "error": str(e),
        "status": "error"
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 2: Cost Optimization Best Practices

# COMMAND ----------

# Question 2
print(f"\n📝 Question 2: {questions[1]}\n")
print("⏳ Querying BrickBrain Knowledge Assistant via MCP...")
print("=" * 80)

try:
    mcp_response_2 = ask_brickbrain_via_mcp(questions[1])
    
    if "result" in mcp_response_2 and "content" in mcp_response_2["result"]:
        response_2 = mcp_response_2["result"]["content"][0]["text"]
    else:
        response_2 = json.dumps(mcp_response_2, indent=2)
    
    print("\n✅ Response 2 received from BrickBrain:")
    print("=" * 80)
    print(response_2)
    print("=" * 80)
    
    all_results.append({
        "question_number": 2,
        "question": questions[1],
        "response": response_2,
        "status": "success"
    })
    
except requests.exceptions.RequestException as e:
    print(f"\n❌ Error querying BrickBrain via MCP: {str(e)}")
    all_results.append({
        "question_number": 2,
        "question": questions[1],
        "error": str(e),
        "status": "error"
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 3: Operational Excellence Best Practices

# COMMAND ----------

# Question 3
print(f"\n📝 Question 3: {questions[2]}\n")
print("⏳ Querying BrickBrain Knowledge Assistant via MCP...")
print("=" * 80)

try:
    mcp_response_3 = ask_brickbrain_via_mcp(questions[2])
    
    if "result" in mcp_response_3 and "content" in mcp_response_3["result"]:
        response_3 = mcp_response_3["result"]["content"][0]["text"]
    else:
        response_3 = json.dumps(mcp_response_3, indent=2)
    
    print("\n✅ Response 3 received from BrickBrain:")
    print("=" * 80)
    print(response_3)
    print("=" * 80)
    
    all_results.append({
        "question_number": 3,
        "question": questions[2],
        "response": response_3,
        "status": "success"
    })
    
except requests.exceptions.RequestException as e:
    print(f"\n❌ Error querying BrickBrain via MCP: {str(e)}")
    all_results.append({
        "question_number": 3,
        "question": questions[2],
        "error": str(e),
        "status": "error"
    })

# COMMAND ----------

# Prepare final result summary
result = {
    "timestamp": datetime.now().isoformat(),
    "mcp_url": MCP_URL,
    "total_questions": len(questions),
    "successful_questions": sum(1 for r in all_results if r["status"] == "success"),
    "failed_questions": sum(1 for r in all_results if r["status"] == "error"),
    "results": all_results,
    "status": "success" if all(r["status"] == "success" for r in all_results) else "partial_success"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Results

# COMMAND ----------

# Display result summary
print("\n" + "=" * 80)
print("📊 FINAL QUERY SUMMARY")
print("=" * 80)
print(f"\n⏰ Timestamp: {result['timestamp']}")
print(f"🔗 MCP URL: {result['mcp_url']}")
print(f"📝 Total Questions: {result['total_questions']}")
print(f"✅ Successful: {result['successful_questions']}")
print(f"❌ Failed: {result['failed_questions']}")
print(f"🎯 Overall Status: {result['status'].upper()}")

print("\n" + "-" * 80)
print("QUESTIONS ASKED:")
print("-" * 80)
for i, q in enumerate(questions, 1):
    status_icon = "✅" if all_results[i-1]["status"] == "success" else "❌"
    print(f"{status_icon} Q{i}: {q}")

# Save to a table for tracking (optional)
# You could create a Delta table to track all queries
result_json = json.dumps(result, indent=2)
print(f"\n📄 Full Result (JSON):\n{result_json}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC This notebook demonstrates:
# MAGIC - ✅ Multiple questions in sequence (3 questions about Spark Streaming)
# MAGIC - ✅ MCP bridge integration for Knowledge Agent queries
# MAGIC 
# MAGIC This notebook can be further enhanced with:
# MAGIC - Logging results to a Delta table for historical tracking
# MAGIC - Sending notifications on failures (Slack, email, etc.)
# MAGIC - Tracking response times and quality metrics
# MAGIC - Comparing answers over time to detect drift
# MAGIC - Adding conversation context for follow-up questions

