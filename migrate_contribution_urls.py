"""
One-off migration: rewrite contribution_url and debate_url payloads in Qdrant
to use the debate section title slug instead of the /link placeholder.

Before: https://hansard.parliament.uk/Commons/2025-11-24/debates/{id}/link
After:  https://hansard.parliament.uk/Commons/2025-11-24/debates/{id}/GazaCeasefire

Run inside the Docker container:
    docker compose exec mcp-server uv run python migrate_contribution_urls.py
"""

import asyncio
import logging

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import PointIdsList

from parliament_mcp.settings import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 256


def _build_urls(payload: dict) -> tuple[str, str | None]:
    """Recompute debate_url and contribution_url from stored payload fields."""
    house = payload.get("House") or "Commons"
    sitting_date = payload.get("SittingDate", "")
    date_str = sitting_date[:10] if sitting_date else ""
    debate_section_ext_id = payload.get("DebateSectionExtId") or ""
    debate_section = payload.get("DebateSection") or ""
    contribution_ext_id = payload.get("ContributionExtId")

    slug = debate_section.strip().replace(" ", "")
    debate_url = f"https://hansard.parliament.uk/{house}/{date_str}/debates/{debate_section_ext_id}/{slug}"
    contribution_url = f"{debate_url}#contribution-{contribution_ext_id}" if contribution_ext_id else None
    return debate_url, contribution_url


async def migrate():
    client = AsyncQdrantClient(url=settings.QDRANT_URL, api_key=settings.QDRANT_API_KEY)
    collection = settings.HANSARD_CONTRIBUTIONS_COLLECTION

    info = await client.get_collection(collection)
    total = info.points_count
    logger.info(f"Collection '{collection}' has {total:,} points")

    updated = 0
    skipped = 0
    offset = None

    while True:
        records, offset = await client.scroll(
            collection_name=collection,
            limit=BATCH_SIZE,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )
        if not records:
            break

        points_to_update: list[tuple[str | int, dict]] = []
        for record in records:
            payload = record.payload or {}
            old_debate_url = payload.get("debate_url", "")

            if "/link" not in old_debate_url:
                skipped += 1
                continue

            debate_url, contribution_url = _build_urls(payload)
            new_payload = {"debate_url": debate_url}
            if contribution_url is not None:
                new_payload["contribution_url"] = contribution_url
            points_to_update.append((record.id, new_payload))

        if points_to_update:
            for point_id, new_payload in points_to_update:
                await client.set_payload(
                    collection_name=collection,
                    payload=new_payload,
                    points=PointIdsList(points=[point_id]),
                )
            updated += len(points_to_update)

        logger.info(f"Progress: {updated:,} updated, {skipped:,} already correct")

        if offset is None:
            break

    logger.info(f"Migration complete. {updated:,} records updated, {skipped:,} already had slug URLs.")


if __name__ == "__main__":
    asyncio.run(migrate())
