# Process & Documentation Improvements

During the data load and audit session on 12 January 2026, the following areas for improvement were identified:

### 1. Environment Consistency
*   **Issue:** Difficulty running maintenance scripts on the host machine due to missing dependencies (`uv`, `rich`, etc.).
*   **Improvement:** Mandate that all maintenance operations run inside the Docker container. 
*   **Action taken:** Updated `README.md` to use `docker compose exec mcp-server uv run python ...` for all steps.

### 2. Audit Reliability
*   **Issue:** The initial `robust_loader.py audit` command reported "MISSING" data for days that were validly empty at the source (e.g. parliamentary recesses or non-sitting days). This caused unnecessary concern.
*   **Improvement:** The audit logic should always cross-reference "zero-item days" with the live Parliament API before flagging a gap.
*   **Action taken:** Enhanced `audit_data.py` to verify empty days via API.

### 3. State Persistence
*   **Issue:** The `loader_state.db` file (which tracks the harvest queue) was stored inside the ephemeral container filesystem. Rebuilding the container could lead to losing track of which items were processed.
*   **Improvement:** Always mount persistent state files as host volumes.
*   **Action taken:** Added `./loader_state.db:/app/loader_state.db` to `docker-compose.yaml`.

### 4. Data Transparency (Items vs. Vectors)
*   **Issue:** Users may be confused by the mismatch between the number of harvested items (~310k) and the number of points in Qdrant (~1.8M).
*   **Improvement:** Explicitly document that the system uses a **Recursive Chunker** to split long contributions into multiple vector points. This 1:N relationship is expected and necessary for high-quality semantic search.
*   **Recommendation:** Add a "Data Metrics" section to the README explaining this ratio.

### 5. Logging & UI
*   **Issue:** The audit script produces high volumes of `HTTP 200 OK` logs from `httpx`, making it hard to see actual gap reports.
*   **Improvement:** Configure `httpx` logging to `WARNING` level by default during audits, only showing successful hits as part of a progress summary.

### 6. Dependency Management
*   **Issue:** The `robust_loader.py` script requires `rich`, but this isn't always obvious if a user tries to run a subset of the code.
*   **Improvement:** Add a `requirements-dev.txt` for host-side IDE support, even if execution is intended for Docker.
