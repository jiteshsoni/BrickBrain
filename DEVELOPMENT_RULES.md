# Development Rules

## Git Commit Guidelines

### Files to NEVER commit:
- `.gitignore` - Should not be tracked in the repository
- `blog_scraper.log` - Log files should remain local only
- `__builtins__.pyi` - Auto-generated files
- `.vscode/` - IDE configuration files should remain local
- Any credentials or sensitive data

### General Rules:
- Never commit credentials to GitHub
- Never create redundant files or functions that do the same thing
- Use DRY (Don't Repeat Yourself) principle: One function, one purpose
- Single Source of Truth: One file per feature
- Environment Detection: Adapt behavior, don't duplicate code
- Configuration Flags: Use parameters, not separate files
- Unified Interfaces: One way to do each thing

## Databricks Specific Rules:
- Always use Databricks Connect for testing scripts like `delta_table_streaming_benchmark.py`
- Test command: `python delta_table_streaming_benchmark.py` (with Databricks Connect)
- Connection check: `databricks-connect test` before running
- Expected behavior: Script should connect to remote Databricks cluster
- Never run Databricks scripts in local PySpark environment
