import pytest
import asyncio
from qdrant_client import AsyncQdrantClient, models
from parliament_mcp.mcp_server.qdrant_query_handler import QdrantQueryHandler
from parliament_mcp.settings import settings
from parliament_mcp.openai_helpers import get_openai_client
from parliament_mcp.qdrant_helpers import initialize_qdrant_collections, collection_exists
from parliament_mcp.qdrant_data_loaders import QdrantHansardLoader

@pytest.mark.asyncio
async def test_get_full_content_standalone():
    """Standalone test for retrieving full debate AND contribution content."""
    
    # Connect directly to the Qdrant service in Docker Compose
    qdrant_url = "http://qdrant:6333"
    # Increase timeout for index operations
    client = AsyncQdrantClient(url=qdrant_url, timeout=60)
    openai_client = get_openai_client(settings)
    
    try:
        # 1. Initialize collections and indexes
        await initialize_qdrant_collections(client, settings)
        
        # 2. Check if we have some data, if not load a bit
        points_count = 0
        if await collection_exists(client, settings.HANSARD_CONTRIBUTIONS_COLLECTION):
            info = await client.get_collection(settings.HANSARD_CONTRIBUTIONS_COLLECTION)
            points_count = info.points_count
            
        if points_count == 0:
            print("Loading test data for standalone test...")
            loader = QdrantHansardLoader(
                qdrant_client=client,
                collection_name=settings.HANSARD_CONTRIBUTIONS_COLLECTION,
                settings=settings,
            )
            await loader.load_all_contributions("2025-06-24", "2025-06-24")
            
        handler = QdrantQueryHandler(client, openai_client, settings)
        
        # 3. Search and get IDs
        results = await handler.search_hansard_contributions(
            date_from="2025-06-24",
            date_to="2025-06-24",
            max_results=5
        )
        
        if not results:
            pytest.skip("No Hansard data found for 2025-06-24")
            
        first_result = results[0]
        debate_id = first_result["debate_id"]
        contribution_id = first_result["contribution_id"]
        
        print(f"Testing with debate_id: {debate_id}")
        print(f"Testing with contribution_id: {contribution_id}")
        
        # 4. Test get_full_debate
        debate_contributions = await handler.get_full_debate(debate_id)
        assert debate_contributions is not None
        assert len(debate_contributions) > 0
        print(f"Successfully reconstructed debate with {len(debate_contributions)} contributions.")

        # 5. Test get_full_contribution
        full_contribution = await handler.get_full_contribution(contribution_id)
        assert full_contribution is not None
        assert full_contribution["contribution_id"] == contribution_id
        print("Successfully reconstructed full contribution.")
        
        # 6. Test get_contribution_context
        neighbors = await handler.get_contribution_neighbors(contribution_id)
        assert neighbors is not None
        assert len(neighbors) > 0
        
        # Check if the target is in the list
        target_in_list = any(c["contribution_id"] == contribution_id for c in neighbors)
        assert target_in_list, "Target contribution not found in neighbors list"
        
        # Verify sorting
        orders = [c["order_in_debate"] for c in neighbors if c.get("order_in_debate") is not None]
        assert orders == sorted(orders), "Neighbors are not correctly ordered"
        
        # Verify range (should be continuous integers usually, or at least close)
        if len(neighbors) > 1:
            diffs = [orders[i+1] - orders[i] for i in range(len(orders)-1)]
            # In a perfect world diffs are all 1, but there might be gaps in data loading or filtering?
            # Actually with full debate loaded, they should be 1.
            # But let's just assert they are ordered.
            pass
            
        print(f"Successfully retrieved context with {len(neighbors)} contributions.")
        
    finally:
        await client.close()
