#!/bin/bash
# BrickBrain MCP Server Status Check

set -e

echo "🧠 BrickBrain MCP Server Status"
echo "==============================="

# Check if config exists
if [ ! -f "config.yaml" ]; then
    echo "❌ config.yaml not found. Run ./setup.sh first."
    exit 1
fi

# Load configuration
if [ -f ".env" ]; then
    source .env
    echo "✅ Configuration loaded from .env"
else
    echo "❌ .env file not found. Run ./setup.sh first."
    exit 1
fi

# Check if server file exists
if [ ! -f "brickbrain_mcp.py" ]; then
    echo "❌ brickbrain_mcp.py not found!"
    exit 1
fi

echo "📋 Server Configuration:"
echo "  Server Name: $SERVER_NAME"
echo "  Databricks Host: $DATABRICKS_HOST"
echo "  Endpoint Name: $ENDPOINT_NAME"
echo ""

# Test Databricks connection
echo "🔍 Testing Databricks connection..."
if python -c "
import os
os.environ['DATABRICKS_HOST'] = '$DATABRICKS_HOST'
os.environ['DATABRICKS_TOKEN'] = '$DATABRICKS_TOKEN'
from databricks.sdk import WorkspaceClient
try:
    client = WorkspaceClient()
    user = client.current_user.me()
    print(f'✅ Connected as: {user.user_name}')
    print(f'✅ Workspace: {client.config.host}')
except Exception as e:
    print(f'❌ Connection failed: {e}')
    exit(1)
"; then
    echo "✅ Databricks connection successful!"
else
    echo "❌ Databricks connection failed!"
    exit 1
fi

# Test MCP server
echo ""
echo "🔍 Testing MCP server functionality..."
if timeout 30 python brickbrain_mcp.py test --databricks-host "$DATABRICKS_HOST" --databricks-token "$DATABRICKS_TOKEN" --endpoint-name "$ENDPOINT_NAME" > /dev/null 2>&1; then
    echo "✅ MCP server test passed!"
else
    echo "⚠️ MCP server test failed or timed out"
fi

# Check dependencies
echo ""
echo "🔍 Checking dependencies..."
if python -c "import databricks.sdk; print('✅ databricks-sdk installed')"; then
    echo "✅ All dependencies available"
else
    echo "❌ Missing dependencies. Install with: pip install databricks-sdk"
fi

# Show uvx command
echo ""
echo "🚀 Ready to use! Here's your uvx command:"
echo ""
echo "uvx --with databricks-sdk python $(pwd)/brickbrain_mcp.py \\"
echo "  --databricks-host \"$DATABRICKS_HOST\" \\"
echo "  --databricks-token \"$DATABRICKS_TOKEN\" \\"
echo "  --endpoint-name \"$ENDPOINT_NAME\""
echo ""

# Show GitHub installation
REPO_URL="https://github.com/jiteshsoni/BrickBrain.git"
echo "📦 GitHub installation:"
echo "git clone $REPO_URL"
echo "cd BrickBrain/ssa_agent/ask_brickbrain"
echo "uvx --with databricks-sdk python ./brickbrain_mcp.py \\"
echo "  --databricks-host \"$DATABRICKS_HOST\" \\"
echo "  --databricks-token \"$DATABRICKS_TOKEN\" \\"
echo "  --endpoint-name \"$ENDPOINT_NAME\""
echo ""

echo "✅ BrickBrain MCP Server is ready to use!"
