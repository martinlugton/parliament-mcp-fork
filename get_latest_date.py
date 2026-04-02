
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
            async def get_latest_via_scroll(collection, date_field):
                search_res = await client.scroll(
                    collection_name=collection,
                    limit=1,
                    with_payload=True,
                    order_by=models.OrderBy(key=date_field, direction=models.Direction.DESC),
                )
                points = search_res[0]
                if points:
                    return points[0].payload.get(date_field)
                return None

            hansard_date = await get_latest_via_scroll(settings.HANSARD_CONTRIBUTIONS_COLLECTION, "SittingDate")
            if hansard_date:
                print(f"Latest Hansard SittingDate: {hansard_date}")
            else:
                print("No Hansard contributions found or could not determine date.")

            print(f"\nChecking collection: {settings.PARLIAMENTARY_QUESTIONS_COLLECTION}")
            pq_date = await get_latest_via_scroll(settings.PARLIAMENTARY_QUESTIONS_COLLECTION, "dateTabled")
            if pq_date:
                print(f"Latest PQ dateTabled: {pq_date}")
            else:
                print("No Parliamentary Questions found or could not determine date.")

        except Exception as e:
            print(f"General error: {e}")

if __name__ == "__main__":
    asyncio.run(get_latest_date())
