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

### 3. Loading the Current Parliamentary Term (July 2024 - Present)

The system distinguishes between **Searchable Data** (ingested into Qdrant) and **Live Reference Data** (fetched in real-time).

**Note:** For consistency and to ensure access to the correct python environment and dependencies (managed by `uv`), all commands below should be run **inside the Docker container** using `docker compose exec mcp-server`.

#### A. Searchable Data (Hansard & PQs)
These must be ingested and embedded to enable semantic search. We use a robust **Harvest-Process-Audit** workflow to handle API failures and rate limits automatically.

**Option 1: Quick Sync (Standard CLI)**
*   **Method:** `make load_current_term`
*   **Behavior:** Stateless. Fetches and pushes data in a single pass.
*   **Best For:** Fast daily/weekly updates when you already have most data.
*   **Note:** If interrupted, it does not track progress and may need to be re-run for the whole range.

**Option 2: Robust Backfill (Recommended Default)**
*   **Method:** `make load_current_term_robust`
*   **Behavior:** Stateful. Uses a SQLite database (`loader_state.db`) to track every record ID.
*   **Best For:** Initial setup or loading large historical ranges (months/years).
*   **Benefit:** Resume-able. If the process crashes or you hit rate limits, it picks up exactly where it left off.
*   **State Management:** The database is persisted on the host at `data/loader_state.db` (mapped to `/app/data/loader_state.db` inside the container).

*Note: Processing tens of thousands of records will take several hours and incur Azure OpenAI API costs for embeddings.*

#### Monitoring & Maintenance
You can monitor the progress of the robust loader or manage its state using these commands:

```bash
# Check current progress (counts of pending/completed items)
docker compose exec mcp-server uv run python robust_loader.py stats

# Retry failed items (marks FAILED items as PENDING)
docker compose exec mcp-server uv run python robust_loader.py retry-failed

# Reset stuck items (marks PROCESSING items as PENDING - use if script crashed)
docker compose exec mcp-server uv run python robust_loader.py reset
```

#### B. Live Reference Data (Members & Committees)
**No manual loading is required.** Member profiles, Committee memberships, and ministerial roles are fetched in real-time via the Parliament API. These tools (e.g., `search_members`, `get_committee_members`) are available as soon as the MCP server is running.

#### C. Verify Completeness (The Audit)
Run the audit scripts to ensure 100% of the searchable data is loaded.
```bash
# Verify database content against live API counts
docker compose exec mcp-server uv run python audit_data.py
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
- **API Cost**: ~$72.00 for the initial load (July 2024 - Jan 2026) using `text-embedding-3-large`. ~$0.00003 per search query thereafter.
- **Load Time**: ~28 hours for the full backfill (July 2024 - Jan 2026) involving ~316k records.

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
