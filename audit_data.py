import asyncio
from datetime import datetime, timedelta
from qdrant_client import AsyncQdrantClient, models
from parliament_mcp.settings import settings
from parliament_mcp.qdrant_data_loaders import cached_limited_get, HANSARD_BASE_URL, PQS_BASE_URL

async def check_gaps(client, collection_name, date_field, start_date, end_date):
    print(f"\nAuditing {collection_name} from {start_date} to {end_date}...")
    
    current_date = start_date
    missing_days = []
    
    while current_date <= end_date:
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
            # No data in Qdrant. Check API to confirm if data SHOULD exist.
            print(f"  Checking API for {date_str}...", end="\r")
            
            api_count = 0
            try:
                if "hansard" in collection_name:
                    # Check Hansard API
                    for c_type in ["Spoken", "Written", "Corrections", "Petitions"]:
                        url = f"{HANSARD_BASE_URL}/search/contributions/{c_type}.json"
                        resp = await cached_limited_get(url, params={"startDate": date_str, "endDate": date_str, "take": 1})
                        if resp.status_code == 200:
                            api_count += resp.json().get("TotalResultCount", 0)
                            if api_count > 0: break # Found some data, so it's a gap
                            
                elif "parliamentary_questions" in collection_name:
                    # Check PQ API - Strictly align with the field being audited.
                    if date_field == "dateTabled":
                        url = f"{PQS_BASE_URL}/writtenquestions/questions"
                        resp = await cached_limited_get(url, params={"tabledWhenFrom": date_str, "tabledWhenTo": date_str, "take": 1})
                        if resp.status_code == 200:
                            api_count = resp.json().get("totalResults", 0)
                    else:
                        # Fallback
                        url = f"{PQS_BASE_URL}/writtenquestions/questions"
                        resp = await cached_limited_get(url, params={f"{date_field}WhenFrom": date_str, f"{date_field}WhenTo": date_str, "take": 1})
                        if resp.status_code == 200:
                            api_count = resp.json().get("totalResults", 0)

            except Exception as e:
                print(f"  API Check failed for {date_str}: {e}")
                continue

            if api_count > 0:
                print(f"  MISSING: {date_str} (API has {api_count} items)        ")
                missing_days.append((current_date, f"API has {api_count} items"))
            
        current_date += timedelta(days=1)
    
    return missing_days

import argparse

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", help="YYYY-MM-DD")
    parser.add_argument("--end-date", help="YYYY-MM-DD")
    args = parser.parse_args()

    client = AsyncQdrantClient(url=settings.QDRANT_URL, api_key=settings.QDRANT_API_KEY)
    
    # Start of 2024 Parliament
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    else:
        start_date = datetime(2024, 7, 4).date()
        
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    else:
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
            if gaps:
                for date, reason in gaps:
                    print(f"  - {date}: {reason}")

        print_gaps("Hansard", hansard_gaps)
        print_gaps("Parliamentary Questions", pq_gaps)
                
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())