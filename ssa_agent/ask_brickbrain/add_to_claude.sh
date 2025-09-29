#!/bin/bash
# Add BrickBrain MCP Server to Claude Desktop

set -e

echo "🧠 Adding BrickBrain MCP Server to Claude Desktop"
echo "================================================"

# Check if config exists
if [ ! -f ".env" ]; then
    echo "❌ .env file not found. Run ./setup.sh first."
    exit 1
fi

# Load configuration
source .env

echo "📋 Configuration:"
echo "  Server Name: $SERVER_NAME"
echo "  Databricks Host: $DATABRICKS_HOST"
echo "  Endpoint Name: $ENDPOINT_NAME"
echo ""

# Check if claude CLI is available
if command -v claude &> /dev/null; then
    echo "✅ Claude CLI found"
    
    # Add to Claude Desktop
    echo "🔧 Adding to Claude Desktop..."
    
    # Option 1: Local file installation
    claude mcp add "$SERVER_NAME-local" --scope local -- \
      uvx --with databricks-sdk python "$(pwd)/brickbrain_mcp.py" \
      --databricks-host "$DATABRICKS_HOST" \
      --databricks-token "$DATABRICKS_TOKEN" \
      --endpoint-name "$ENDPOINT_NAME"
    
    echo ""
    echo "💡 Alternative GitHub installation:"
    echo "git clone https://github.com/jiteshsoni/BrickBrain.git"
    echo "cd BrickBrain/ssa_agent/ask_brickbrain"
    echo "claude mcp add brickbrain-mcp-github --scope local -- \\"
    echo "  uvx --with databricks-sdk python \"\$(pwd)/brickbrain_mcp.py\" \\"
    echo "  --databricks-host \"$DATABRICKS_HOST\" \\"
    echo "  --databricks-token \"$DATABRICKS_TOKEN\" \\"
    echo "  --endpoint-name \"$ENDPOINT_NAME\""
    
    echo "✅ Successfully added $SERVER_NAME to Claude Desktop!"
    echo ""
    echo "🎉 You can now use BrickBrain in Claude Desktop!"
    echo "Available tools:"
    echo "  - ask_brickbrain: Ask questions to the Knowledge Agent"
    echo "  - databrickshealth: Check server health"
    echo "  - databricksinfo: Get Databricks feature information"
    echo "  - databricksworkspace: Explore workspace contents"
    echo ""
    echo "Available prompts:"
    echo "  - databricks-troubleshooting: Structured troubleshooting help"
    echo "  - delta-lake-optimization: Delta table optimization guidance"
    echo "  - mlflow-workflow: MLflow best practices"
    echo "  - data-engineering-pipeline: Pipeline design help"
    echo "  - unity-catalog-setup: Unity Catalog guidance"
    echo "  - spark-performance: Spark optimization help"
    
else
    echo "❌ Claude CLI not found."
    echo ""
    echo "Please install Claude CLI or manually add to your MCP configuration:"
    echo ""
    echo "Add this to your ~/.cursor/mcp.json or Claude Desktop config:"
    echo ""
    cat << EOF
{
  "mcpServers": {
    "$SERVER_NAME": {
      "command": "uvx",
      "args": [
        "--with", "databricks-sdk",
        "python",
        "$(pwd)/brickbrain_mcp.py",
        "--databricks-host", "$DATABRICKS_HOST",
        "--databricks-token", "$DATABRICKS_TOKEN",
        "--endpoint-name", "$ENDPOINT_NAME"
      ],
      "notes": "BrickBrain MCP Server with tools and prompts for Databricks"
    }
  }
}
EOF
    echo ""
    echo "Or using environment variables:"
    echo ""
    cat << EOF
{
  "mcpServers": {
    "$SERVER_NAME": {
      "command": "uvx",
      "args": [
        "--with", "databricks-sdk",
        "python",
        "$(pwd)/brickbrain_mcp.py",
        "--endpoint-name", "$ENDPOINT_NAME"
      ],
      "env": {
        "DATABRICKS_HOST": "$DATABRICKS_HOST",
        "DATABRICKS_TOKEN": "$DATABRICKS_TOKEN"
      },
      "notes": "BrickBrain MCP Server with environment-based auth"
    }
  }
}
EOF
fi
