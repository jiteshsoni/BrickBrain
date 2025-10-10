#!/bin/bash
# Ultra-minimal MCP bridge using only curl (no Python required)
# Supports long-running requests (up to 3 minutes) for Databricks Agent

set -e

while IFS= read -r line; do
  if [ -n "$line" ]; then
    response=$(curl -s -w "\n%{http_code}" -X POST \
      --max-time 180 \
      -H "Content-Type: application/json" \
      -H "x-mcp-key: 3eacb5a9b446c3507dfa5b84dc0e335a7c9f5e70801579ee9f35247fcd1c7369" \
      -d "$line" \
      https://brickbrain-mcp-bridge.get2jitesh.workers.dev/mcp)
    
    # Extract status code (last line) and body (everything else)
    status_code=$(echo "$response" | tail -n 1)
    body=$(echo "$response" | sed '$d')
    
    # Only output body if it's not empty (handles 204 No Content for notifications)
    if [ -n "$body" ]; then
      echo "$body"
    fi
  fi
done

