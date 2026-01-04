import asyncio
import json
import argparse
from qdrant_client import AsyncQdrantClient, models
from parliament_mcp.settings import settings

async def get_stats(client, collection_name, sort_field):
    if not await client.collection_exists(collection_name):
        return None
        
    count = (await client.count(collection_name)).count
    if count == 0:
        return {"count": 0}

    # Latest
    latest = await client.scroll(
        collection_name=collection_name,
        limit=1,
        with_payload=True,
        order_by=models.OrderBy(key=sort_field, direction=models.Direction.DESC)
    )
    
    # Earliest
    earliest = await client.scroll(
        collection_name=collection_name,
        limit=1,
        with_payload=True,
        order_by=models.OrderBy(key=sort_field, direction=models.Direction.ASC)
    )
    
    latest_date = latest[0][0].payload.get(sort_field) if latest and latest[0] else None
    earliest_date = earliest[0][0].payload.get(sort_field) if earliest and earliest[0] else None
    
    # Clean up T00:00:00 if present
    if latest_date and "T" in latest_date: latest_date = latest_date.split("T")[0]
    if earliest_date and "T" in earliest_date: earliest_date = earliest_date.split("T")[0]

    return {
        "count": count,
        "latest": latest_date,
        "earliest": earliest_date
    }

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    client = AsyncQdrantClient(url=settings.QDRANT_URL, api_key=settings.QDRANT_API_KEY)
    
    try:
        results = {
            "hansard": await get_stats(client, settings.HANSARD_CONTRIBUTIONS_COLLECTION, "SittingDate"),
            "pqs": await get_stats(client, settings.PARLIAMENTARY_QUESTIONS_COLLECTION, "dateTabled")
        }
        
        if args.json:
            print(json.dumps(results))
        else:
            for name, stats in results.items():
                print(f"Checking {name}...")
                if not stats:
                    print("  Collection does not exist.")
                else:
                    print(f"  Total points: {stats['count']}")
                    print(f"  Latest: {stats.get('latest')}")
                    print(f"  Earliest: {stats.get('earliest')}")
                
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
