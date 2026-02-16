import json
import urllib.request

def check_range(collection, date_field, start_date):
    url = f"http://localhost:6333/collections/{collection}/points/scroll"
    query_filter = {
        "must": [
            {
                "key": date_field,
                "range": {"gte": f"{start_date}T00:00:00Z"}
            }
        ]
    }
    data = {
        "limit": 5,
        "filter": query_filter,
        "with_payload": True
    }
    
    req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'), headers={'Content-Type': 'application/json'}, method='POST')
    try:
        with urllib.request.urlopen(req) as response:
            res = json.loads(response.read().decode())
            points = res.get("result", {}).get("points", [])
            print(f"Items in {collection} since {start_date}:")
            if not points:
                print(" None found.")
            for p in points:
                print(f" - {p['payload'].get(date_field)}")
    except Exception as e:
        print(f"Error: {e}")

check_range("parliament_mcp_hansard_contributions", "SittingDate", "2026-02-01")
check_range("parliament_mcp_parliamentary_questions", "dateTabled", "2026-02-01")
