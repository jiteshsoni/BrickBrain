# 🧠 BrickBrain MCP Server

A Model Context Protocol (MCP) server that provides intelligent Databricks assistance through tools and prompts, powered by your Databricks Knowledge Agent.

## ✨ Features

### 🛠️ **Tools**
- **`ask_brickbrain`** - Direct questions to your Databricks Knowledge Agent
- **`databrickshealth`** - Check server and connection health
- **`databricksinfo`** - Get detailed information about Databricks features
- **`databricksworkspace`** - Explore workspace contents and structure

### 📝 **Prompts**
- **`databricks-troubleshooting`** - Structured troubleshooting assistance
- **`delta-lake-optimization`** - Delta table performance optimization
- **`mlflow-workflow`** - MLflow best practices and workflows
- **`data-engineering-pipeline`** - Pipeline design and optimization
- **`unity-catalog-setup`** - Unity Catalog configuration guidance
- **`spark-performance`** - Apache Spark performance tuning

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Access to a Databricks workspace
- Databricks Knowledge Agent endpoint
- `uv`/`uvx` installed ([installation guide](https://docs.astral.sh/uv/getting-started/installation/))
- `claude` CLI (optional, for easy MCP setup)

### 🎯 Fastest Setup (Recommended)
```bash
# One-command installation from GitHub (replace with your credentials)
claude mcp add databricks-mcp-local --scope local -- \
  uvx --from git+ssh://git@github.com/databricks-solutions/brickbrain-mcp.git brickbrain-mcp \
  --databricks-host "https://dbc-xxxxxxxx-xxxx.cloud.databricks.com" \
  --databricks-token "dapi1234567890abcdef" \
  --endpoint-name "ka-xxxxxxxx-endpoint"
```

### Installation

1. **Clone and setup:**
   ```bash
   git clone https://github.com/databricks-solutions/brickbrain-mcp.git
   cd brickbrain-mcp
   chmod +x *.sh
   ./setup.sh
   ```

2. **Check status:**
   ```bash
   ./app_status.sh
   ```

3. **Add to Claude Desktop:**
   ```bash
   ./add_to_claude.sh
   ```

### Manual Installation

If you prefer manual setup:

```bash
# Copy env.example to .env and configure your values
cp env.example .env
# Edit .env with your actual Databricks credentials, then:
source .env

# Test the server
python brickbrain_mcp.py test

# Add to Claude Desktop (if claude CLI available)
claude mcp add $SERVER_NAME --scope user -- \
  uvx --with databricks-sdk python brickbrain_mcp.py \
  --databricks-host $DATABRICKS_HOST \
  --databricks-token $DATABRICKS_TOKEN \
  --endpoint-name $ENDPOINT_NAME
```

## 🔧 Configuration Options

### Environment Variables
```bash
export DATABRICKS_HOST="https://dbc-xxxxxxxx-xxxx.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890abcdef"
export ENDPOINT_NAME="ka-xxxxxxxx-endpoint"
```

### Command Line Arguments
```bash
python brickbrain_mcp.py \
  --databricks-host "https://dbc-xxxxxxxx-xxxx.cloud.databricks.com" \
  --databricks-token "dapi1234567890abcdef" \
  --endpoint-name "ka-xxxxxxxx-endpoint"
```

### Databricks Config File
Create `~/.databrickscfg`:
```ini
[DEFAULT]
host = https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
token = dapi1234567890abcdef
```

## 📱 Usage Examples

### Using Tools
```
@brickbrain ask_brickbrain "How do I optimize my Delta table performance?"
@brickbrain databrickshealth
@brickbrain databricksinfo topic="mlflow"
```

### Using Prompts
Select prompts from your MCP client and fill in the structured forms:
- **Delta Optimization**: Specify table size, query patterns, and current issues
- **Troubleshooting**: Provide error message, context, and environment details
- **MLflow Workflow**: Describe your use case, model type, and challenges

## 🔒 Authentication

The server supports multiple authentication methods (in priority order):

1. **Command line arguments** (highest priority)
2. **Environment variables**
3. **Databricks config file** (lowest priority)

### Getting Your Credentials

1. **Databricks Host**: Your workspace URL (e.g., `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com`)
2. **Access Token**: Generate from Databricks workspace → User Settings → Access Tokens
3. **Endpoint Name**: Your Knowledge Agent serving endpoint name

## 🎯 MCP Client Configuration

### Cursor
Add to `~/.cursor/mcp.json`:
```json
{
  "mcpServers": {
    "brickbrain": {
      "command": "uvx",
      "args": [
        "--with", "databricks-sdk",
        "python", "/path/to/brickbrain_mcp.py"
      ],
      "env": {
        "DATABRICKS_HOST": "https://dbc-xxxxxxxx-xxxx.cloud.databricks.com",
        "DATABRICKS_TOKEN": "dapi1234567890abcdef"
      }
    }
  }
}
```

### Claude Desktop
Add to your Claude Desktop MCP configuration:
```json
{
  "mcpServers": {
    "brickbrain": {
      "command": "uvx",
      "args": [
        "--with", "databricks-sdk",
        "python", "/path/to/brickbrain_mcp.py",
        "--databricks-host", "https://dbc-xxxxxxxx-xxxx.cloud.databricks.com",
        "--databricks-token", "dapi1234567890abcdef"
      ]
    }
  }
}
```

## 🧪 Testing

Run the built-in test suite:
```bash
python brickbrain_mcp.py test
```

Or with specific credentials:
```bash
python brickbrain_mcp.py test \
  --databricks-host "https://dbc-xxxxxxxx-xxxx.cloud.databricks.com" \
  --databricks-token "dapi1234567890abcdef"
```

## 📦 Distribution Options

### Local File
```bash
uvx --with databricks-sdk python /path/to/brickbrain_mcp.py
```

### From GitHub (SSH)
```bash
claude mcp add databricks-mcp-local --scope local -- \
  uvx --from git+ssh://git@github.com/databricks-solutions/brickbrain-mcp.git brickbrain-mcp \
  --databricks-host "$DATABRICKS_HOST" \
  --databricks-token "$DATABRICKS_TOKEN" \
  --endpoint-name "$ENDPOINT_NAME"
```

### From GitHub (HTTPS)
```bash
uvx --from git+https://github.com/databricks-solutions/brickbrain-mcp.git brickbrain-mcp
```

### From PyPI (future)
```bash
uvx brickbrain-mcp
```

## 🛡️ Security Best Practices

1. **Never commit tokens** to version control
2. **Use `env.example` as template** - Copy to `.env` and fill in actual values
3. **Use environment variables** for production
4. **Rotate tokens regularly**
5. **Use workspace-scoped tokens** when possible
6. **Set appropriate token permissions**
7. **Verify `.env` is in `.gitignore`** (already included)

### Getting Started with Security
```bash
# Copy the example file and configure
cp env.example .env
# Edit .env with your actual values (never commit this file)
# The .env file is automatically ignored by git
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

MIT License - see LICENSE file for details.

## 🆘 Support

- **Issues**: [GitHub Issues](https://github.com/databricks-solutions/brickbrain-mcp/issues)
- **Documentation**: This README and inline code documentation
- **Databricks**: [Databricks Documentation](https://docs.databricks.com/)

## 🔄 Version History

- **v1.0.0**: Initial release with tools and prompts
  - 4 tools: ask_brickbrain, databrickshealth, databricksinfo, databricksworkspace
  - 6 prompts: troubleshooting, delta optimization, mlflow, pipelines, unity catalog, spark performance
  - Multiple authentication methods
  - uvx support
  - Comprehensive testing
