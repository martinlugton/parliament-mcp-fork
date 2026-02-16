
import sqlite3
import json
import urllib.request
import os

def check_qdrant_date(collection, date_str, date_field):
    url = f"http://localhost:6333/collections/{collection}/points/scroll"
    # Using a simple filter for the date
    # Qdrant datetime format is usually ISO8601
    query_filter = {
        "must": [
            {
                "key": date_field,
                "range": {
                    "gte": f"{date_str}T00:00:00Z",
                    "lte": f"{date_str}T23:59:59Z"
                }
            }
        ]
    }
    data = {
        "limit": 1,
        "filter": query_filter,
        "with_payload": False,
        "with_vectors": False
    }
    
    req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'), headers={'Content-Type': 'application/json'}, method='POST')
    try:
        with urllib.request.urlopen(req) as response:
            res = json.loads(response.read().decode())
            return len(res.get("result", {}).get("points", [])) > 0
    except Exception as e:
        print(f"Error checking {collection} for {date_str}: {e}")
        return False

# Check some dates from loader_state.db
db_path = 'data/loader_state.db'
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("Checking consistency for 2026-02-06...")
    
    # Hansard
    h_exists = check_qdrant_date("parliament_mcp_hansard_contributions", "2026-02-06", "SittingDate")
    print(f"Hansard 2026-02-06 in Qdrant: {h_exists}")
    
    # PQ
    pq_exists = check_qdrant_date("parliament_mcp_parliamentary_questions", "2026-02-06", "dateTabled")
    print(f"PQ 2026-02-06 in Qdrant: {pq_exists}")
    
    # Check 2025-07-21 for Hansard (the date Qdrant claimed was latest)
    h_old_exists = check_qdrant_date("parliament_mcp_hansard_contributions", "2025-07-21", "SittingDate")
    print(f"Hansard 2025-07-21 in Qdrant: {h_old_exists}")

    conn.close()
else:
    print("DB not found")
