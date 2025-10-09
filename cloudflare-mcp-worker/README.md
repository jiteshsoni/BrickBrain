# 🧠 BrickBrain - Databricks AI Assistant

Ask questions about Databricks, Spark, Delta Lake, MLflow, Unity Catalog, and data engineering - get expert answers with citations!

---

## ⚡ Quick Setup (2 minutes)

### Step 1: Download the Bridge Script

**Mac/Linux:**
```bash
curl -o ~/brickbrain-bridge.sh https://raw.githubusercontent.com/jiteshsoni/BrickBrain/main/cloudflare-mcp-worker/mcp-bridge.sh
chmod +x ~/brickbrain-bridge.sh
```

**Windows (Git Bash or WSL):**
```bash
curl -o ~/brickbrain-bridge.sh https://raw.githubusercontent.com/jiteshsoni/BrickBrain/main/cloudflare-mcp-worker/mcp-bridge.sh
chmod +x ~/brickbrain-bridge.sh
```

### Step 2: Configure Cursor

Create or edit `~/.cursor/mcp.json`:

**Mac/Linux:**
```bash
mkdir -p ~/.cursor
nano ~/.cursor/mcp.json
```

**Windows (PowerShell):**
```powershell
mkdir $env:USERPROFILE\.cursor -Force
notepad $env:USERPROFILE\.cursor\mcp.json
```

Paste this configuration **(copy exactly as shown - no changes needed!)**:

```json
{
  "mcpServers": {
    "brickbrain": {
      "command": "/bin/bash",
      "args": ["-c", "exec ~/brickbrain-bridge.sh"]
    }
  }
}
```

**✅ That's it!** No username replacements needed - `~/` automatically points to your home directory.

<details>
<summary>🔧 Alternative: Inline Configuration (no separate file needed)</summary>

If you prefer not to download a separate script, use this all-in-one config:

```json
{
  "mcpServers": {
    "brickbrain": {
      "command": "bash",
      "args": [
        "-c",
        "while IFS= read -r line; do [ -n \"$line\" ] && curl -s -X POST --max-time 180 -H 'Content-Type: application/json' -H 'x-mcp-key: 3eacb5a9b446c3507dfa5b84dc0e335a7c9f5e70801579ee9f35247fcd1c7369' -d \"$line\" https://brickbrain-mcp-bridge.get2jitesh.workers.dev/mcp; done"
      ]
    }
  }
}
```

This works but is harder to read and maintain.
</details>

### Step 3: Restart Cursor

Completely quit and reopen Cursor (not just refresh - fully quit and relaunch).

### Step 4: Start Using!

```
@brickbrain ask_brickbrain "What is Delta Lake?"
```

**That's it!** 🎉

---

## 💬 Usage Examples

### Basic Questions
```
@brickbrain ask_brickbrain "How do I optimize Delta Lake tables?"
@brickbrain ask_brickbrain "What is Unity Catalog?"
@brickbrain ask_brickbrain "Show me Delta Live Tables example"
```

### Follow-up Questions (Automatic Context!)
```
@brickbrain ask_brickbrain "What is Unity Catalog?"
@brickbrain ask_brickbrain "Tell me more about the governance features"
@brickbrain ask_brickbrain "How does it compare to Hive metastore?"
```

✨ BrickBrain remembers your conversation for **30 minutes**!

### Start Fresh Conversation
```
@brickbrain reset_conversation
```

---

## 🎯 What Can BrickBrain Answer?

| ✅ Covered Topics | ❌ Not Covered |
|-------------------|----------------|
| Delta Lake, DLT | General programming |
| Unity Catalog | Non-Databricks topics |
| Spark optimization | Real-time news/events |
| MLflow workflows | Code debugging |
| Structured Streaming | Personal data questions |
| Data governance | |

**Every answer includes:**
- 📝 Expert explanations
- 💻 Code examples  
- 🔗 **Citations** (links to source blogs)
- ⚡ Best practices

---

## ⚙️ Features

| Feature | Details |
|---------|---------|
| 🧠 **Smart Context** | Remembers last 30 minutes of conversation |
| 📚 **Citations** | All answers include source links |
| ⚡ **Global Edge** | Fast responses from anywhere |
| 🔒 **Secure** | Encrypted, authenticated |
| 💰 **Free** | No cost to use |

---

## ⏱️ Response Times

- **Typical**: 10-30 seconds
- **Complex queries**: Up to 2 minutes

💡 **Tip**: Be patient! BrickBrain searches through extensive blog content and uses LLMs to generate detailed, accurate answers.

---

## 🆘 Troubleshooting

<details>
<summary><strong>❓ "brickbrain" not appearing in Cursor?</strong></summary>

1. Check `~/.cursor/mcp.json` is valid JSON (paste into JSONLint.com)
2. **Restart Cursor completely** (Quit → Reopen, not just refresh)
3. Verify `bash` and `curl` are installed (they should be by default on Mac/Linux)
4. Check Cursor's MCP logs (Settings → Advanced → Show Logs)

**Test the connection manually:**
```bash
# If using the script file approach:
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | ~/brickbrain-bridge.sh

# Or test the server directly:
curl -X POST https://brickbrain-mcp-bridge.get2jitesh.workers.dev/mcp \
  -H "Content-Type: application/json" \
  -H "x-mcp-key: 3eacb5a9b446c3507dfa5b84dc0e335a7c9f5e70801579ee9f35247fcd1c7369" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

Should return: `{"jsonrpc":"2.0","id":1,"result":{"tools":[...]}}`
</details>

<details>
<summary><strong>⏳ Requests timing out?</strong></summary>

- **Wait patiently** - complex questions take up to 2 minutes
- Try simplifying: "What is Delta Lake?" instead of "Explain Delta Lake architecture, use cases, and optimization strategies in detail"
- Check your internet connection
- Verify the server is up:
  ```bash
  curl https://brickbrain-mcp-bridge.get2jitesh.workers.dev/
  ```
</details>

<details>
<summary><strong>🐛 Getting error messages?</strong></summary>

**Test the server directly:**
```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -H "x-mcp-key: 3eacb5a9b446c3507dfa5b84dc0e335a7c9f5e70801579ee9f35247fcd1c7369" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' \
  https://brickbrain-mcp-bridge.get2jitesh.workers.dev/mcp
```

If this works but Cursor doesn't:
- Problem is with your MCP configuration
- Double-check the JSON formatting in `~/.cursor/mcp.json`
- Make sure quotes and brackets match exactly

If this fails:
- Server might be down (rare)
- Check your network/firewall settings
</details>

<details>
<summary><strong>💻 Windows-specific issues?</strong></summary>

Windows users may need to install bash:
1. Use Git Bash (comes with Git for Windows)
2. Or use WSL (Windows Subsystem for Linux)

Alternatively, use Python instead:
```json
{
  "mcpServers": {
    "brickbrain": {
      "command": "python",
      "args": [
        "-c",
        "import sys,json,requests;[print(json.dumps(requests.post('https://brickbrain-mcp-bridge.get2jitesh.workers.dev/mcp',json=json.loads(l),headers={'x-mcp-key':'3eacb5a9b446c3507dfa5b84dc0e335a7c9f5e70801579ee9f35247fcd1c7369'},timeout=180).json()),flush=True) for l in sys.stdin if l.strip()]"
      ]
    }
  }
}
```
</details>

---

## 📝 Tips for Best Results

1. **Be specific**: 
   - ✅ "How do I optimize small Delta tables with frequent updates?"
   - ❌ "optimization tips"

2. **One topic at a time**: 
   - ✅ Ask about Delta Lake, then Unity Catalog separately
   - ❌ "Tell me about Delta Lake, Unity Catalog, and MLflow"

3. **Use conversation context**:
   - First: "What is Delta Lake?"
   - Then: "What are its ACID properties?" (remembers we're talking about Delta Lake)

4. **Reset between topics**:
   - Use `reset_conversation` when switching to a completely different subject

---

## 🔐 Security & Privacy

**Is it safe to use the shared MCP key?**

✅ **Yes!** The key only allows:
- Asking questions to BrickBrain
- Reading publicly available blog content (Databricksters, Canadian Data Guy, AI on Databricks)

❌ **Cannot**:
- Access your private data
- Modify any systems
- See your Databricks workspaces
- Access other users' conversations

Each user's conversation is isolated by IP address.

---

## 📊 Data Sources

BrickBrain searches curated content from:
- 📰 **Databricksters** (Substack)
- 📰 **Canadian Data Guy** (Substack)
- 📰 **AI on Databricks** (Medium)

All responses include **citations** linking back to original sources!

---

## 🔧 For Developers

**Want to run your own instance?**

<details>
<summary>Click for deployment details</summary>

**Tech Stack:**
- Cloudflare Workers (serverless, global edge network)
- MCP protocol over HTTP (JSON-RPC 2.0)
- Bash bridge script (9 lines, zero dependencies)

**Deploy your own:**
```bash
cd cloudflare-mcp-worker
npm install
npx wrangler deploy
```

**Required secrets:**
```bash
echo "YOUR_TOKEN" | npx wrangler secret put DATABRICKS_TOKEN
echo "YOUR_KEY" | npx wrangler secret put MCP_SHARED_KEY
echo "YOUR_ENDPOINT" | npx wrangler secret put DATABRICKS_SERVING_URL
```

**Architecture:** Cursor (stdio) → mcp-bridge.sh → Cloudflare Worker → Databricks Agent

</details>

---

## 📮 Feedback & Support

- **Issues?** [Open a GitHub issue](https://github.com/jiteshsoni/BrickBrain/issues)
- **Questions?** Check the troubleshooting section above
- **Contributions?** PRs welcome!

---

**Happy Learning with BrickBrain! 🎓✨**
