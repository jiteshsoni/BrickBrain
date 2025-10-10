# Archive - Historical Documentation

This directory contains historical documentation that was useful during development but is no longer actively maintained.

## Files

### NEW_ARCHITECTURE.md
**Date:** October 2025  
**Purpose:** Documents the architectural refactoring from incremental to deterministic processing pipeline  
**Status:** Refactor completed - kept for historical reference  

This document explains the transition from an incremental append-based architecture (which caused duplicates) to a deterministic overwrite-based architecture for the data processing pipeline. The changes described here have been fully implemented in the codebase.

**Why archived:** The refactor is complete and the current architecture is now documented in:
- `TABLE_DEPENDENCY_GRAPH.md` - Complete table lineage and data flow
- `ssa_agent/agent/retriever_tool/DATA_INGESTION.md` - Data ingestion documentation

---

*Archive maintained to preserve context about major architectural decisions*

