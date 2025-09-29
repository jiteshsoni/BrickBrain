#!/bin/bash
# BrickBrain MCP Server Setup Script

set -e

echo "🧠 BrickBrain MCP Server Setup"
echo "=============================="

# Default values
DEFAULT_SERVER_NAME="brickbrain-mcp"
DEFAULT_ENDPOINT_NAME="ka-ce79c9c6-endpoint"

# Get server name
read -p "Enter server name (default: $DEFAULT_SERVER_NAME): " SERVER_NAME
SERVER_NAME=${SERVER_NAME:-$DEFAULT_SERVER_NAME}

# Get Databricks host
read -p "Enter Databricks workspace host (e.g., https://dbc-xxxxxxxx-xxxx.cloud.databricks.com): " DATABRICKS_HOST
if [ -z "$DATABRICKS_HOST" ]; then
    echo "❌ Databricks host is required!"
    exit 1
fi

# Get Databricks token
read -s -p "Enter Databricks access token: " DATABRICKS_TOKEN
echo
if [ -z "$DATABRICKS_TOKEN" ]; then
    echo "❌ Databricks token is required!"
    exit 1
fi

# Get Knowledge Agent endpoint name
read -p "Enter Knowledge Agent endpoint name (default: $DEFAULT_ENDPOINT_NAME): " ENDPOINT_NAME
ENDPOINT_NAME=${ENDPOINT_NAME:-$DEFAULT_ENDPOINT_NAME}

# Create config.yaml
cat > config.yaml << EOF
# BrickBrain MCP Server Configuration
server_name: "$SERVER_NAME"
databricks_host: "$DATABRICKS_HOST"
databricks_token: "$DATABRICKS_TOKEN"
endpoint_name: "$ENDPOINT_NAME"
version: "1.0.0"
EOF

echo "✅ Configuration saved to config.yaml"

# Create environment file
cat > .env << EOF
# BrickBrain MCP Server Environment Variables
DATABRICKS_HOST="$DATABRICKS_HOST"
DATABRICKS_TOKEN="$DATABRICKS_TOKEN"
ENDPOINT_NAME="$ENDPOINT_NAME"
SERVER_NAME="$SERVER_NAME"
EOF

echo "✅ Environment variables saved to .env"

# Test connection
echo "🔍 Testing Databricks connection..."
if python -c "
import os
os.environ['DATABRICKS_HOST'] = '$DATABRICKS_HOST'
os.environ['DATABRICKS_TOKEN'] = '$DATABRICKS_TOKEN'
from databricks.sdk import WorkspaceClient
try:
    client = WorkspaceClient()
    user = client.current_user.me()
    print(f'✅ Connected successfully as: {user.user_name}')
except Exception as e:
    print(f'❌ Connection failed: {e}')
    exit(1)
"; then
    echo "✅ Databricks connection successful!"
else
    echo "❌ Databricks connection failed!"
    exit 1
fi

# Test Knowledge Agent endpoint
echo "🔍 Testing Knowledge Agent endpoint..."
if python brickbrain_mcp.py test --databricks-host "$DATABRICKS_HOST" --databricks-token "$DATABRICKS_TOKEN" --endpoint-name "$ENDPOINT_NAME" > /dev/null 2>&1; then
    echo "✅ Knowledge Agent endpoint accessible!"
else
    echo "⚠️ Knowledge Agent endpoint test failed (but continuing...)"
fi

echo ""
echo "🎉 Setup completed successfully!"
echo ""
echo "Next steps:"
echo "1. Run './app_status.sh' to check server status"
echo "2. Run './add_to_claude.sh' to add to Claude Desktop"
echo "3. Or manually add to your MCP client configuration"
echo ""
echo "Configuration files created:"
echo "- config.yaml (server configuration)"
echo "- .env (environment variables)"
