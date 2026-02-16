
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
            # Hansard Contributions
            hansard_res = await client.scroll(
                collection_name=settings.HANSARD_CONTRIBUTIONS_COLLECTION,
                limit=1,
                with_payload=True,
                with_vectors=False,
                # We can't sort in scroll easily without a filter that matches everything and then use search with order_by if supported
            )
            
            # Use search with a dummy vector and order_by to get the latest
            # Qdrant 1.10+ supports order_by in search
            
            try:
                hansard_latest = await client.query_points(
                    collection_name=settings.HANSARD_CONTRIBUTIONS_COLLECTION,
                    limit=1,
                    order_by=models.OrderBy(
                        key="SittingDate",
                        direction=models.Direction.DESC,
                    ),
                    with_payload=True,
                )
                if hansard_latest.points:
                    print(f"Latest Hansard SittingDate: {hansard_latest.points[0].payload.get('SittingDate')}")
                else:
                    print("No Hansard contributions found.")
            except Exception as e:
                print(f"Error querying Hansard: {e}")

            print(f"\nChecking collection: {settings.PARLIAMENTARY_QUESTIONS_COLLECTION}")
            try:
                pq_latest = await client.query_points(
                    collection_name=settings.PARLIAMENTARY_QUESTIONS_COLLECTION,
                    limit=1,
                    order_by=models.OrderBy(
                        key="dateTabled",
                        direction=models.Direction.DESC,
                    ),
                    with_payload=True,
                )
                if pq_latest.points:
                    print(f"Latest PQ dateTabled: {pq_latest.points[0].payload.get('dateTabled')}")
                else:
                    print("No Parliamentary Questions found.")
            except Exception as e:
                print(f"Error querying PQs: {e}")

        except Exception as e:
            print(f"General error: {e}")

if __name__ == "__main__":
    asyncio.run(get_latest_date())
