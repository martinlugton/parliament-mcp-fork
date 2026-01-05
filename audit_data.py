import asyncio
from datetime import datetime, timedelta
from qdrant_client import AsyncQdrantClient, models
from parliament_mcp.settings import settings
from parliament_mcp.qdrant_data_loaders import cached_limited_get, HANSARD_BASE_URL

async def is_sitting_day(date_obj):
    date_str = date_obj.strftime("%Y-%m-%d")
    # Check Commons
    url = f"{HANSARD_BASE_URL}/overview/sectionsforday.json"
    try:
        resp = await cached_limited_get(url, params={"date": date_str, "house": "Commons"})
        resp.raise_for_status()
        data_commons = resp.json()
        
        resp = await cached_limited_get(url, params={"date": date_str, "house": "Lords"})
        resp.raise_for_status()
        data_lords = resp.json()
        
        is_sitting = bool(data_commons or data_lords)
        # if not is_sitting:
        #     print(f"DEBUG: {date_str} is NOT a sitting day")
        return is_sitting
    except Exception as e:
        print(f"Warning: Failed to check sitting status for {date_str}: {e}")
        return True # Assume true on error to be safe

async def check_gaps(client, collection_name, date_field, start_date, end_date):
    print(f"\nAuditing {collection_name} from {start_date} to {end_date}...")
    
    current_date = start_date
    missing_days = []
    
    while current_date <= end_date:
        # Check if it's a weekend (optimization: usually not sitting, but we should verify if we want to be 100% robust)
        # However, checking API for every day is slow.
        # Strategy: If Qdrant has data, great. If not, THEN check if it was a sitting day.
        
        date_str = current_date.strftime("%Y-%m-%d")
        
        # Robust way: use search/scroll with filter
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
            # No data in Qdrant. Was it a sitting day?
            # We only check API if it's not a weekend, to save calls, 
            # BUT parliament occasionally sits on weekends. 
            # For "total confidence", we should check API if we suspect a gap.
            # But checking API for every weekend is overkill and likely returns false.
            # Let's check API for ALL missing days.
            
            print(f"  Checking source for potential gap: {date_str}...", end="\r")
            if await is_sitting_day(current_date):
                missing_days.append((current_date, current_date.weekday() >= 5))
            else:
                pass # Not a sitting day, so not a gap
            
        current_date += timedelta(days=1)
    
    return missing_days

async def main():
    client = AsyncQdrantClient(url=settings.QDRANT_URL, api_key=settings.QDRANT_API_KEY)
    
    # Start of 2024 Parliament
    start_date = datetime(2024, 7, 4).date()
    end_date = datetime.now().date()
    
    try:
        hansard_gaps = await check_gaps(
            client, 
            settings.HANSARD_CONTRIBUTIONS_COLLECTION, 
            "SittingDate", 
            start_date, 
            end_date
        )
        
        pq_gaps = await check_gaps(
            client, 
            settings.PARLIAMENTARY_QUESTIONS_COLLECTION, 
            "dateTabled", 
            start_date, 
            end_date
        )
        
        def print_gaps(name, gaps):
            if not gaps:
                print(f"No gaps found in {name}!")
                return
            
            print(f"\nDetected {len(gaps)} potential gaps in {name}:")
            # Group consecutive days for cleaner output
            if gaps:
                for date, is_weekend in gaps:
                    tag = "[WEEKEND]" if is_weekend else ""
                    print(f"  - {date} {tag}")

        print_gaps("Hansard", hansard_gaps)
        print_gaps("Parliamentary Questions", pq_gaps)
                
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
