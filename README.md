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

### 3. Loading and Synchronizing Data

The system distinguishes between **Searchable Data** (ingested into Qdrant) and **Live Reference Data** (fetched in real-time).

#### A. Initial Load (Current Parliamentary Term: July 2024 - Present)
For the first time setup, use the robust backfill to ensure all historical data is captured.

```bash
# Initialize Qdrant and perform initial backfill
docker compose exec mcp-server make load_current_term_robust
```

#### B. Daily Synchronization (Catching Up)
To keep the data up to date, run the sync commands regularly.

**Option 1: Robust Sync (Recommended)**
Uses the stateful loader to identify the last date and harvest missing items.
```bash
docker compose exec mcp-server bash -c "uv run parliament-mcp --log-level INFO init-qdrant && uv run python robust_loader.py sync --process"
```
> **Note:** Do not use `make sync_robust` inside the container — the Makefile reads `.env` and overwrites `QDRANT_URL` with `localhost:6333`, breaking the connection to Qdrant.

**Option 2: Quick Sync**
Fast, stateless sync for the last few days.
```bash
docker compose exec mcp-server make sync
```

#### Monitoring & Maintenance
You can monitor progress or manage the state of the robust loader:

```bash
# Check the latest dates and counts per source in the local queue (most reliable)
docker compose exec mcp-server uv run python get_db_dates.py

# Check current queue progress (PENDING / PROCESSING / COMPLETED counts)
docker compose exec mcp-server uv run python robust_loader.py stats

# Check the latest dates stored in Qdrant (uses order_by for accuracy)
docker compose exec mcp-server uv run python get_latest_date.py

# Check API embedding costs
docker compose exec mcp-server uv run python calculate_cost.py

# Retry failed items (marks FAILED items as PENDING)
docker compose exec mcp-server uv run python robust_loader.py retry-failed

# Reset stuck items (marks PROCESSING items as PENDING)
docker compose exec mcp-server uv run python robust_loader.py reset
```

#### C. Live Reference Data (Members & Committees)
**No manual loading is required.** Member profiles, Committee memberships, and ministerial roles are fetched in real-time via the Parliament API. These tools (e.g., `search_members`, `get_committee_members`) are available as soon as the MCP server is running.

#### C. Verify Completeness (The Audit)
Run the audit scripts to ensure 100% of the searchable data is loaded.
```bash
# Verify database content against live API counts
docker compose exec mcp-server uv run python audit_data.py
```

---

## Data Portability (Transferring to another machine)

To avoid re-downloading and re-embedding all data (which incurs time and API costs), you can transfer your existing Qdrant data using snapshots and Google Drive Desktop.

### Prerequisites
- Google Drive Desktop installed and signed in on both machines (assumed mount point `G:\`).
- Docker running on both machines.

### Phase 1: Export (Source Machine)

1.  **Create the Snapshots**:
    ```powershell
    curl.exe -X POST "http://localhost:6333/collections/parliament_mcp_hansard_contributions/snapshots"
    curl.exe -X POST "http://localhost:6333/collections/parliament_mcp_parliamentary_questions/snapshots"
    ```

2.  **Copy Snapshots to Google Drive**:
    ```powershell
    # Create the destination folder
    mkdir "G:\My Drive\QdrantTransfer" -ErrorAction SilentlyContinue

    # Find and copy the latest Hansard snapshot
    $H_FILE = docker exec qdrant sh -c "ls /qdrant/snapshots/parliament_mcp_hansard_contributions/*.snapshot | sort -r | head -n 1"
    docker cp "qdrant:$H_FILE" "G:\My Drive\QdrantTransfer\hansard.snapshot"

    # Find and copy the latest PQs snapshot
    $PQ_FILE = docker exec qdrant sh -c "ls /qdrant/snapshots/parliament_mcp_parliamentary_questions/*.snapshot | sort -r | head -n 1"
    docker cp "qdrant:$PQ_FILE" "G:\My Drive\QdrantTransfer\pqs.snapshot"
    ```

### Phase 2: Import (Destination Machine)

*Wait for Google Drive to finish syncing (the blue icon becomes a green checkmark).*

1.  **Prepare the New Container**:
    Ensure services are running: `docker compose up -d`.

2.  **Copy Snapshots into the Container**:
    ```powershell
    docker cp "G:\My Drive\QdrantTransfer\hansard.snapshot" qdrant:/qdrant/hansard.snapshot
    docker cp "G:\My Drive\QdrantTransfer\pqs.snapshot" qdrant:/qdrant/pqs.snapshot
    ```

3.  **Trigger the Recovery**:
    ```powershell
    # Restore Hansard
    curl.exe -X POST "http://localhost:6333/collections/parliament_mcp_hansard_contributions/snapshots/recover" `
        -H "Content-Type: application/json" `
        -d '{"location": "snapshot:///qdrant/hansard.snapshot"}'

    # Restore PQs
    curl.exe -X POST "http://localhost:6333/collections/parliament_mcp_parliamentary_questions/snapshots/recover" `
        -H "Content-Type: application/json" `
        -d '{"location": "snapshot:///qdrant/pqs.snapshot"}'
    ```

*Note: Recovery will replace any existing collection of the same name on the target machine with the version from the snapshot.*

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
- **API Cost**: ~$10.36 for the initial load (July 2024 - Jan 2026) using `text-embedding-3-large`. ~$0.00003 per search query thereafter.
- **Load Time**: ~32.5 hours for the full backfill (July 2024 - Jan 2026) involving ~317k records.

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
