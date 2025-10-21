#!/bin/bash
# Update Cloudflare Worker secrets from .env file
# Usage: ./update-secrets.sh

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}🔐 Updating Cloudflare Worker Secrets${NC}"
echo "======================================"

# Check if .env file exists at project root
ENV_FILE="../.env"
if [ ! -f "$ENV_FILE" ]; then
    echo -e "${RED}❌ Error: .env file not found at $ENV_FILE${NC}"
    exit 1
fi

# Load environment variables from .env file
echo -e "${BLUE}📖 Reading secrets from $ENV_FILE...${NC}"

# Extract specific variables (avoiding sourcing entire file for security)
DATABRICKS_TOKEN=$(grep "^DATABRICKS_TOKEN=" "$ENV_FILE" | cut -d '=' -f2-)
DATABRICKS_SERVING_URL=$(grep "^DATABRICKS_SERVING_URL=" "$ENV_FILE" | cut -d '=' -f2-)
MCP_SHARED_KEY=$(grep "^MCP_SHARED_KEY=" "$ENV_FILE" | cut -d '=' -f2-)

# Validate required secrets exist
if [ -z "$DATABRICKS_TOKEN" ]; then
    echo -e "${RED}❌ Error: DATABRICKS_TOKEN not found in .env file${NC}"
    exit 1
fi

if [ -z "$DATABRICKS_SERVING_URL" ]; then
    echo -e "${RED}❌ Error: DATABRICKS_SERVING_URL not found in .env file${NC}"
    exit 1
fi

# MCP_SHARED_KEY is optional but recommended
if [ -z "$MCP_SHARED_KEY" ]; then
    echo -e "${BLUE}⚠️  Warning: MCP_SHARED_KEY not found in .env file (optional but recommended)${NC}"
fi

# Display what will be updated (masked for security)
echo ""
echo "Secrets to update:"
echo "  - DATABRICKS_TOKEN: ${DATABRICKS_TOKEN:0:10}...${DATABRICKS_TOKEN: -4}"
echo "  - DATABRICKS_SERVING_URL: $DATABRICKS_SERVING_URL"
if [ -n "$MCP_SHARED_KEY" ]; then
    echo "  - MCP_SHARED_KEY: ${MCP_SHARED_KEY:0:10}...${MCP_SHARED_KEY: -4}"
fi
echo ""

# Confirm before proceeding
read -p "Proceed with updating Cloudflare Worker secrets? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${BLUE}❌ Cancelled by user${NC}"
    exit 0
fi

echo ""
echo -e "${BLUE}🚀 Updating secrets on Cloudflare...${NC}"
echo ""

# Update DATABRICKS_SERVING_URL
echo -e "${BLUE}1/3 Updating DATABRICKS_SERVING_URL...${NC}"
if echo "$DATABRICKS_SERVING_URL" | npx wrangler secret put DATABRICKS_SERVING_URL; then
    echo -e "${GREEN}✅ DATABRICKS_SERVING_URL updated successfully${NC}"
else
    echo -e "${RED}❌ Failed to update DATABRICKS_SERVING_URL${NC}"
    exit 1
fi

echo ""

# Update DATABRICKS_TOKEN
echo -e "${BLUE}2/3 Updating DATABRICKS_TOKEN...${NC}"
if echo "$DATABRICKS_TOKEN" | npx wrangler secret put DATABRICKS_TOKEN; then
    echo -e "${GREEN}✅ DATABRICKS_TOKEN updated successfully${NC}"
else
    echo -e "${RED}❌ Failed to update DATABRICKS_TOKEN${NC}"
    exit 1
fi

echo ""

# Update MCP_SHARED_KEY if it exists
if [ -n "$MCP_SHARED_KEY" ]; then
    echo -e "${BLUE}3/3 Updating MCP_SHARED_KEY...${NC}"
    if echo "$MCP_SHARED_KEY" | npx wrangler secret put MCP_SHARED_KEY; then
        echo -e "${GREEN}✅ MCP_SHARED_KEY updated successfully${NC}"
    else
        echo -e "${RED}❌ Failed to update MCP_SHARED_KEY${NC}"
        exit 1
    fi
else
    echo -e "${BLUE}3/3 Skipping MCP_SHARED_KEY (not found in .env)${NC}"
fi

echo ""
echo -e "${GREEN}🎉 All secrets updated successfully!${NC}"
echo ""
echo -e "${BLUE}ℹ️  Note: Secrets take effect immediately (within seconds)${NC}"
echo -e "${BLUE}ℹ️  No need to redeploy unless you changed the code${NC}"

