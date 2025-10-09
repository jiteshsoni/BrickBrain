# BrickBrain MCP Bridge - Cloudflare Worker

A Model Context Protocol (MCP) server that bridges to your Databricks Agent (BrickBrain), hosted on Cloudflare Workers.

## 🚀 Features

- **MCP Protocol Support**: SSE and Streamable HTTP transports
- **Databricks Agent Bridge**: Direct connection to your BrickBrain agent
- **Edge Deployment**: Global Cloudflare network for low latency
- **Secure**: Optional shared key authentication for clients
- **Free Tier**: Cloudflare Workers free tier (100,000 requests/day)
- **Zero Maintenance**: Serverless, auto-scaling

## 📋 Prerequisites

1. **Cloudflare Account**: [Sign up for free](https://dash.cloudflare.com/sign-up)
2. **Node.js**: v18 or higher
3. **Databricks Agent**: Your BrickBrain serving endpoint
4. **Wrangler CLI**: Cloudflare's CLI tool

## 🛠️ Setup

### 1. Install Dependencies

```bash
cd cloudflare-mcp-worker
npm install
```

### 2. Authenticate with Cloudflare

```bash
npx wrangler login
```

This will open a browser window to authenticate with your Cloudflare account.

### 3. Set Secrets

Set your Databricks credentials as Worker secrets:

```bash
# Your Databricks serving endpoint URL
npx wrangler secret put DATABRICKS_SERVING_URL
# When prompted, enter: https://dbc-a657af2e-14d9.cloud.databricks.com/serving-endpoints/ka-1ff550b7-endpoint/invocations

# Your Databricks token
npx wrangler secret put DATABRICKS_TOKEN
# When prompted, paste your Databricks PAT token

# Optional: Shared key for client authentication
npx wrangler secret put MCP_SHARED_KEY
# When prompted, enter a strong random key (e.g., generated with: openssl rand -hex 32)
```

### 4. Test Locally

```bash
npm run dev
```

Visit `http://localhost:8787` to see the health check.

Test the MCP endpoint:
```bash
curl -X POST http://localhost:8787/mcp \
  -H "Content-Type: application/json" \
  -H "x-mcp-key: YOUR_SHARED_KEY" \
  -d '{
    "method": "tools/call",
    "params": {
      "name": "ask_brickbrain",
      "arguments": {
        "prompt": "What is Delta Lake?"
      }
    }
  }'
```

### 5. Deploy to Cloudflare

```bash
npm run deploy
```

After deployment, you'll get a URL like:
```
https://brickbrain-mcp-bridge.YOUR-SUBDOMAIN.workers.dev
```

## 🔧 Configuration

### Custom Domain (Optional)

1. Add a route in `wrangler.toml`:
```toml
routes = [
  { pattern = "mcp.yourdomain.com", custom_domain = true }
]
```

2. Redeploy:
```bash
npm run deploy
```

### Environment Variables

Set in `wrangler.toml`:
- Non-sensitive variables: Add to `[vars]` section
- Sensitive variables: Use `wrangler secret put`

## 📱 Usage

### In Cursor

Add to your `~/.cursor/mcp.json`:

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

### In Claude Desktop

Add to your Claude Desktop MCP configuration:

```json
{
  "mcpServers": {
    "brickbrain": {
      "url": "https://brickbrain-mcp-bridge.YOUR-SUBDOMAIN.workers.dev/mcp",
      "transport": "streamableHttp",
      "headers": {
        "x-mcp-key": "YOUR_SHARED_KEY"
      }
    }
  }
}
```

### Available MCP Tools

#### `ask_brickbrain`

Ask questions about Databricks and get expert answers with citations.

**Parameters:**
- `prompt` (required): Your question
- `conversation_id` (optional): Maintain context across questions
- `user_id` (optional): User identifier for tracking
- `return_trace` (optional): Include execution trace

**Example:**
```typescript
{
  "prompt": "How do I optimize Delta Lake tables?",
  "conversation_id": "session-123",
  "return_trace": false
}
```

## 🔍 Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Health check and service info |
| `/mcp` | POST | MCP Streamable HTTP transport (recommended) |
| `/sse` | GET | MCP SSE transport |

## 🐛 Debugging

### View Live Logs

```bash
npm run tail
```

### Test Locally with Dev Server

```bash
npm run dev
# Opens on http://localhost:8787
```

### Check Deployment Status

```bash
wrangler deployments list
```

## 📊 Monitoring

View metrics in Cloudflare Dashboard:
1. Go to [Cloudflare Dashboard](https://dash.cloudflare.com)
2. Navigate to **Workers & Pages**
3. Click on **brickbrain-mcp-bridge**
4. View requests, errors, and performance

## 🔒 Security

### Authentication

The worker supports optional client authentication via the `x-mcp-key` header:

1. Generate a strong random key:
```bash
openssl rand -hex 32
```

2. Set it as a secret:
```bash
npx wrangler secret put MCP_SHARED_KEY
```

3. Include it in client requests:
```bash
curl -H "x-mcp-key: YOUR_KEY" ...
```

### Best Practices

- ✅ Always use secrets for sensitive data
- ✅ Rotate tokens regularly
- ✅ Use custom domain with HTTPS
- ✅ Monitor request patterns for abuse
- ✅ Set rate limits if needed

## 💰 Costs

**Cloudflare Workers Free Tier:**
- 100,000 requests/day
- 10ms CPU time per request
- Unlimited bandwidth

**Beyond Free Tier:**
- $5/month for 10M requests
- Additional $0.50 per million requests

Your BrickBrain usage should easily fit in the free tier!

## 🚨 Troubleshooting

### "Module not found: agents"

The Cloudflare Agents SDK may still be in beta. If you encounter this:

1. Check the [Cloudflare Agents documentation](https://developers.cloudflare.com/agents)
2. Alternatively, implement a basic MCP protocol handler using Hono

### "Authentication failed"

- Verify `DATABRICKS_TOKEN` is set correctly
- Check token permissions in Databricks
- Ensure serving endpoint URL is correct

### "Rate limit exceeded"

If you hit Cloudflare's free tier limits:
1. Upgrade to Workers Paid plan
2. Implement caching for common queries
3. Add request throttling

## 📚 Resources

- [Cloudflare Workers Docs](https://developers.cloudflare.com/workers/)
- [Wrangler CLI Docs](https://developers.cloudflare.com/workers/wrangler/)
- [MCP Specification](https://modelcontextprotocol.io)
- [Databricks Agent Framework](https://docs.databricks.com/en/generative-ai/agent-framework/index.html)

## 🤝 Contributing

Issues and PRs welcome!

## 📄 License

MIT License - see LICENSE file for details.

---

**Deployed at**: `https://brickbrain-mcp-bridge.YOUR-SUBDOMAIN.workers.dev`  
**Health Check**: Visit the root URL to verify deployment

