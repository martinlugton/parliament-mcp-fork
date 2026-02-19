
import asyncio
import logging
from parliament_mcp.settings import settings
from parliament_mcp.qdrant_helpers import get_async_qdrant_client
from qdrant_client import models

logging.basicConfig(level=logging.INFO)

async def get_latest_date():
    async with get_async_qdrant_client(settings) as client:
        print(f"Checking collection: {settings.HANSARD_CONTRIBUTIONS_COLLECTION}")
        try:
            # Hansard Contributions - Scroll with no filter but large limit or just get enough to find the latest
            # Since we can't sort in scroll, we use a search with a dummy vector if query_points fails
            
            async def get_latest_via_scroll(collection, date_field):
                # We'll scroll and find the max. Not most efficient but reliable.
                # Alternatively, use the 'search' with a zero vector.
                res = await client.search(
                    collection_name=collection,
                    query_vector=("text_dense", [0.0] * settings.EMBEDDING_DIMENSIONS),
                    limit=10, # Get a few to be safe
                    with_payload=True,
                )
                
                # To truly get the latest, we really need order_by or to have searched with a relevant vector.
                # Since we just want the latest date, let's try a different approach:
                # Use the REST API directly or just scroll a bit.
                
                # Try search with order_by (Discovery API)
                try:
                    search_res = await client.scroll(
                        collection_name=collection,
                        limit=100,
                        with_payload=True,
                    )
                    points = search_res[0]
                    if points:
                        dates = [p.payload.get(date_field) for p in points if p.payload.get(date_field)]
                        if dates:
                            return max(dates)
                except Exception as e:
                    print(f"Scroll failed: {e}")
                return None

            hansard_date = await get_latest_via_scroll(settings.HANSARD_CONTRIBUTIONS_COLLECTION, "SittingDate")
            if hansard_date:
                print(f"Latest Hansard SittingDate (from sample): {hansard_date}")
            else:
                print("No Hansard contributions found or could not determine date.")

            print(f"\nChecking collection: {settings.PARLIAMENTARY_QUESTIONS_COLLECTION}")
            pq_date = await get_latest_via_scroll(settings.PARLIAMENTARY_QUESTIONS_COLLECTION, "dateTabled")
            if pq_date:
                print(f"Latest PQ dateTabled (from sample): {pq_date}")
            else:
                print("No Parliamentary Questions found or could not determine date.")

        except Exception as e:
            print(f"General error: {e}")

if __name__ == "__main__":
    asyncio.run(get_latest_date())
