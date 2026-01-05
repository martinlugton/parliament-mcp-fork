# Parliament MCP Server (Enhanced Fork)

An MCP server that roughly maps onto a subset of https://developer.parliament.uk/, offering advanced semantic search, historical data backfilling, and automatic synchronization over Hansard and Parliamentary Questions.

## Architecture

This project provides:
- **MCP Server**: FastMCP-based server with standard tools plus advanced discovery/recommendation capabilities.
- **Python package**: A comprehensive library for querying and loading parliamentary data.
- **Qdrant**: Local vector database storing ~330k+ records (July 2024 - Present).
- **Maintenance Suite**: Scripts for robust historical loading and daily synchronization.

## Features

### Advanced MCP Tools
The server exposes tools for real-time and historical research:
- **Speeches & PQs**: Standard semantic search over debates and written questions.
- **Recommendations**: "Find more like this" using existing speech IDs.
- **Discovery**: Contextual search (Target + Positive Example - Negative Example).
- **Diversification**: Spread search results across different debates to avoid clustering.
- **Live APIs**: Real-time data on Members, Committees, and ministerial roles.

---

## Quick Start

### 1. Prerequisites
- Docker & Docker Compose
- Node.js (for Claude integration)
- **Azure OpenAI API Key** (for `text-embedding-3-large`)

### 2. Initial Setup
```bash
# Set up environment
cp .env.example .env
# Fill in AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT in .env

# Start services
docker compose up --build -d

# Initialize database structure
docker compose exec mcp-server uv run parliament-mcp init-qdrant
```

### 3. Loading Historical Data
To perform a full historical load (e.g., from the start of the 2024 Parliament):
```bash
# Uses robust 1-week batches with retries
python batch_load_data.py --start-date 2024-07-04 --type all
```

### 4. Robust Data Loading & Verification
To ensure a **100% complete** and reproducible data set, follow this three-step process:

1. **Initial Batch Load**: Run the batch loader for the full range (as shown above).
2. **Audit for Gaps**: Identify any days that failed to load or were skipped.
   ```bash
   docker compose exec mcp-server uv run python audit_data.py
   ```
   *Note: This script cross-references with the official Parliament API to ignore weekends and non-sitting days.*
3. **Heal Missing Data**: Automatically retry and fill any validated gaps found during the audit.
   ```bash
   docker compose exec mcp-server uv run python heal_data.py
   ```

### 5. Daily Synchronization
To keep your local database up-to-date with the latest parliamentary activity:
```bash
# Automatically detects gaps and pulls missing data
python sync_data.py
```

---

## Usage Modes

### A. Conversational (Claude Desktop)
Add this to your Claude Desktop config (`%APPDATA%/Claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "parliament-mcp": {
      "command": "npx",
      "args": ["mcp-remote", "http://localhost:8080/mcp/", "--allow-http", "--debug"]
    }
  }
}
```

### B. Terminal (Advanced Query Builder)
Use `query_builder.py` for precise technical searches directly against the local Qdrant instance:

```bash
# Find experts/vocal MPs on a topic
docker compose exec mcp-server uv run python query_builder.py contributors "renewable energy" --limit 5

# Diversified search (one hit per debate)
docker compose exec mcp-server uv run python query_builder.py hansard "steel" --diversify

# Contextual Discovery
docker compose exec mcp-server uv run python query_builder.py discover TARGET_ID --context "POS_ID,NEG_ID"
```

---

## Technical Reference

### Data Structure
- **Hansard Contributions**: ~116k entries. Semantic search on spoken words + metadata (Member, Date, House).
- **Parliamentary Questions**: ~218k entries. Semantic search on Question and Answer text.

### Resource Usage
- **Disk Space**: ~2.6 GB for full historical data (July 2024 - Jan 2026).
- **API Cost**: ~$11.00 for the initial 330k record load. ~$0.00003 per search query thereafter.

### Maintenance & Auditing
Check how many records you have and the date range covered:
```bash
docker compose exec mcp-server uv run python check_progress.py
```

## Troubleshooting

**404/406 Errors on connection**
- Ensure the MCP server is mounted correctly. The standard endpoint is `http://localhost:8080/mcp/`.
- If using `mcp-remote`, ensure you include the trailing slash.

**Data Loading Gaps**
- Use `batch_load_data.py` to target specific dates that may have failed during a standard sync.

---
MIT License - Developed for advanced UK Parliamentary research.