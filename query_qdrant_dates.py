import json
import urllib.request

def query_latest(collection, date_field):
    url = f"http://localhost:6333/collections/{collection}/points/query"
    data = {
        "limit": 5,
        "order_by": {
            "key": date_field,
            "direction": "desc"
        },
        "with_payload": True
    }
    
    req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'), headers={'Content-Type': 'application/json'}, method='POST')
    try:
        with urllib.request.urlopen(req) as response:
            res = json.loads(response.read().decode())
            points = res.get("result", {}).get("points", [])
            if points:
                print(f"\nLatest 5 points for {collection}:")
                for p in points:
                    print(f" - {p['payload'].get(date_field)}")
                return points[0]["payload"].get(date_field)
    except Exception as e:
        return f"Error: {e}"
    return "No data"

hansard_latest = query_latest("parliament_mcp_hansard_contributions", "SittingDate")
pq_latest = query_latest("parliament_mcp_parliamentary_questions", "dateTabled")
