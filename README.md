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
- Python 3.12+ (for running loader scripts locally)

### 2. Initial Setup
```bash
# Set up environment
cp .env.example .env
# Fill in AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT in .env

# Start services (Qdrant and MCP Server)
docker compose up -d

# Initialize Qdrant Collection Structure
docker compose exec mcp-server uv run parliament-mcp init-qdrant
```

### 3. Loading Historical Data (100% Confidence)

We use a robust **Harvest-Process-Audit** workflow to ensure 100% of data for a given period is loaded, embedded, and searchable. This system handles API failures, rate limits, and network interruptions automatically.

**Important:** All maintenance scripts should be run **inside the container**. The project uses `uv` for dependency management, which is pre-installed in the Docker image. You do not need to install Python or `uv` on your host machine.

**The Master Script:** `robust_loader.py` (Run via `docker compose exec mcp-server uv run python robust_loader.py ...`)

#### Step 1: Initialize the Queue
Create the local SQLite database (`loader_state.db`) that tracks every single item's state (`PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`).
```bash
docker compose exec mcp-server uv run python robust_loader.py init-db
```

#### Step 2: Harvest Metadata
Scan the Parliament API to find all available items for your date range. This is fast and just populates the "To-Do" list.
```bash
# Example: Load everything from the 2024 Election to present
docker compose exec mcp-server uv run python robust_loader.py harvest --start-date 2024-07-04 --end-date 2026-01-11
```

#### Step 3: Process the Queue
Fetch full text, generate embeddings (Azure OpenAI), and save to Qdrant. This is the heavy lifting.
```bash
# Run in a loop until the queue is empty
docker compose exec mcp-server uv run python robust_loader.py process --loop --batch-size 50
```
*Tip: You can stop (Ctrl+C) and restart this command at any time. It picks up exactly where it left off. The state is persisted in `loader_state.db` on your host.*

#### Step 4: Verify Completeness (The Audit)
**Crucial Step:** Run the audit scripts to prove that 100% of the data is loaded.

1.  **Check Queue Status:**
    ```bash
    docker compose exec mcp-server uv run python robust_loader.py audit --start-date 2024-07-04 --end-date 2026-01-11
    ```
    Checks that every day in the range has been successfully harvested and processed.

2.  **Verify Database Content:**
    ```bash
    docker compose exec mcp-server uv run python audit_data.py
    ```
    Queries Qdrant directly to ensure records exist for every date. For any day with 0 records, it cross-references the live Parliament API to confirm if data actually exists for that day (avoiding false positives on non-sitting days).

**If both audits report NO gaps, you have 100% confidence.**

### 4. Handling Failures
If the `process` step encounters errors (e.g. API timeouts), items are marked as `FAILED` but execution continues.

To fix them:
1.  **Reset** the failed items back to `PENDING`:
    ```bash
    docker compose exec mcp-server uv run python robust_loader.py retry-failed
    ```
2.  **Reset** items stuck in `PROCESSING` (e.g., if the script crashed):
    ```bash
    docker compose exec mcp-server uv run python robust_loader.py reset
    ```
3.  **Run Process Again:**
    ```bash
    docker compose exec mcp-server uv run python robust_loader.py process --loop
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
- **Hansard Contributions**: ~190k entries. Semantic search on spoken words + metadata (Member, Date, House).
- **Parliamentary Questions**: ~117k entries. Semantic search on Question and Answer text.

### Resource Usage
- **Disk Space**: ~2.6 GB for full historical data (July 2024 - Jan 2026).
- **API Cost**: ~$11.00 for the initial 330k record load. ~$0.00003 per search query thereafter.

## Troubleshooting

**404/406 Errors on connection**
- Ensure the MCP server is mounted correctly. The standard endpoint is `http://localhost:8080/mcp/`.
- If using `mcp-remote`, ensure you include the trailing slash.

**Data Loading Gaps**
- Always run `python robust_loader.py audit` to identify specific missing dates.
- If a date is missing, you can re-run `harvest` for just that specific day:
  ```bash
  python robust_loader.py harvest --start-date YYYY-MM-DD --end-date YYYY-MM-DD
  ```

---
MIT License - Developed for advanced UK Parliamentary research.
