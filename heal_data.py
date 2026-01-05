import asyncio
import subprocess
from datetime import datetime, timedelta
from qdrant_client import AsyncQdrantClient, models
from parliament_mcp.settings import settings

from parliament_mcp.qdrant_data_loaders import cached_limited_get, HANSARD_BASE_URL

async def is_sitting_day(date_obj):
    date_str = date_obj.strftime("%Y-%m-%d")
    url = f"{HANSARD_BASE_URL}/overview/sectionsforday.json"
    try:
        resp = await cached_limited_get(url, params={"date": date_str, "house": "Commons"})
        resp.raise_for_status()
        if resp.json(): return True
        resp = await cached_limited_get(url, params={"date": date_str, "house": "Lords"})
        resp.raise_for_status()
        if resp.json(): return True
        return False
    except Exception:
        return True # Default to true on error

async def get_missing_ranges(client, collection_name, date_field, start_date, end_date):
    print(f"Auditing {collection_name} for missing sitting days...")
    current_date = start_date
    missing_days = []
    
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        res = await client.scroll(
            collection_name=collection_name,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key=date_field,
                        range=models.DatetimeRange(
                            gte=f"{date_str}T00:00:00Z",
                            lte=f"{date_str}T23:59:59Z"
                        )
                    )
                ]
            ),
            limit=1,
            with_payload=False,
            with_vectors=False
        )
        if len(res[0]) == 0:
            # Check if it was actually a sitting day before marking as missing
            if await is_sitting_day(current_date):
                missing_days.append(current_date)
        current_date += timedelta(days=1)
    
    # Group into ranges
    if not missing_days:
        return []
    
    ranges = []
    if missing_days:
        range_start = missing_days[0]
        for i in range(1, len(missing_days)):
            if missing_days[i] - missing_days[i-1] > timedelta(days=3): 
                ranges.append((range_start, missing_days[i-1]))
                range_start = missing_days[i]
        ranges.append((range_start, missing_days[-1]))
    
    return ranges

def heal_range(start_date, end_date, data_type):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    print(f"\n>>> Healing {data_type}: {start_str} to {end_str}")
    subprocess.run([
        "uv", "run", "python", "internal_batch_load.py", 
        "--start-date", start_str, 
        "--end-date", end_str, 
        "--type", data_type
    ])

async def main():
    client = AsyncQdrantClient(url=settings.QDRANT_URL, api_key=settings.QDRANT_API_KEY)
    start_date = datetime(2024, 7, 4).date()
    end_date = datetime.now().date()
    
    try:
        # 1. Check Hansard
        hansard_ranges = await get_missing_ranges(
            client, settings.HANSARD_CONTRIBUTIONS_COLLECTION, "SittingDate", start_date, end_date
        )
        for r_start, r_end in hansard_ranges:
            heal_range(r_start, r_end, "hansard")
            
        # 2. Check PQs
        pq_ranges = await get_missing_ranges(
            client, settings.PARLIAMENTARY_QUESTIONS_COLLECTION, "dateTabled", start_date, end_date
        )
        for r_start, r_end in pq_ranges:
            heal_range(r_start, r_end, "pqs")
            
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
