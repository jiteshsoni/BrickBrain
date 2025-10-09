# Quick Start Guide - BrickBrain MCP on Cloudflare

## 🚀 5-Minute Deployment

### Step 1: Install Dependencies (1 min)

```bash
cd cloudflare-mcp-worker
npm install
```

### Step 2: Login to Cloudflare (30 sec)

```bash
npx wrangler login
```

A browser window will open - authorize Wrangler.

### Step 3: Set Your Secrets (2 min)

```bash
# Databricks endpoint URL
npx wrangler secret put DATABRICKS_SERVING_URL
# Enter: https://dbc-a657af2e-14d9.cloud.databricks.com/serving-endpoints/ka-1ff550b7-endpoint/invocations

# Databricks token
npx wrangler secret put DATABRICKS_TOKEN
# Paste your Databricks PAT token (starts with dapi...)

# Optional: Authentication key
npx wrangler secret put MCP_SHARED_KEY
# Generate one: openssl rand -hex 32
```

### Step 4: Deploy! (30 sec)

```bash
npm run deploy
```

You'll get a URL like:
```
✨ https://brickbrain-mcp-bridge.your-subdomain.workers.dev
```

### Step 5: Test It (1 min)

```bash
# Health check
curl https://brickbrain-mcp-bridge.your-subdomain.workers.dev/

# Ask a question
curl -X POST https://brickbrain-mcp-bridge.your-subdomain.workers.dev/mcp \
  -H "Content-Type: application/json" \
  -H "x-mcp-key: YOUR_KEY" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
      "name": "ask_brickbrain",
      "arguments": {
        "prompt": "What is Delta Lake?"
      }
    }
  }'
```

## 📱 Use in Cursor

Add to `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "brickbrain": {
      "transport": {
        "type": "streamableHttp",
        "url": "https://brickbrain-mcp-bridge.YOUR-SUBDOMAIN.workers.dev/mcp",
        "headers": {
          "x-mcp-key": "YOUR_SHARED_KEY"
        }
      }
    }
  }
}
```

Restart Cursor, then use:
```
@brickbrain ask_brickbrain "How do I optimize Delta tables?"
```

## 🎯 Done!

Your BrickBrain agent is now:
- ✅ Deployed on Cloudflare's edge network
- ✅ Accessible via MCP protocol
- ✅ Secured with authentication
- ✅ Free tier (100k requests/day)

## 🔍 Next Steps

- Monitor: `npm run tail` (live logs)
- Update: Make changes and `npm run deploy`
- Debug: `npm run dev` (local testing)

---

**Need help?** See the full [README.md](./README.md)

