from collections import defaultdict
from datetime import datetime
from typing import Any, Literal

from fastembed import SparseTextEmbedding
from openai import AsyncAzureOpenAI
from qdrant_client import AsyncQdrantClient, models
from qdrant_client.models import DatetimeRange, FieldCondition, Filter, MatchValue

from parliament_mcp.openai_helpers import embed_single
from parliament_mcp.settings import ParliamentMCPSettings

MINIMUM_DEBATE_HITS = 2


def parse_date(date_str: str | None) -> str | None:
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str).date().isoformat()
    except (ValueError, TypeError):
        return None


def build_date_range_filter(
    date_from: str | None, date_to: str | None, field: str = "SittingDate"
) -> FieldCondition | None:
    """Build a date range filter for Qdrant queries using DatetimeRange."""
    if not date_from and not date_to:
        return None

    return FieldCondition(
        key=field,
        range=DatetimeRange(
            gte=datetime.fromisoformat(date_from).date() if date_from else None,
            lte=datetime.fromisoformat(date_to).date() if date_to else None,
        ),
    )


def build_match_filter(field: str, value: Any) -> FieldCondition | None:
    """Build a match filter for Qdrant queries."""
    if value is None:
        return None
    return FieldCondition(key=field, match=MatchValue(value=value))


def build_filters(conditions: list[FieldCondition | None]) -> Filter | None:
    """Build a Qdrant filter from a list of conditions."""
    valid_conditions = [c for c in conditions if c is not None]
    if not valid_conditions:
        return None

    return Filter(must=valid_conditions)


class DebateCollection:
    """Collection of debates and their contributions.
    Used to track the contributions for each debate and return the substantial debates.
    """

    def __init__(self):
        self._debates = defaultdict(lambda: {"contribution_ids": set(), "info": None})

    def add_contribution(self, contribution):
        debate_id = contribution.get("DebateSectionExtId")
        contribution_id = contribution.get("ContributionExtId")
        debate = self._debates[debate_id]
        new_data = contribution_id not in debate["contribution_ids"]
        debate["contribution_ids"].add(contribution_id)
        if debate["info"] is None:
            debate["info"] = {
                "debate_id": debate_id,
                "title": contribution.get("DebateSection"),
                "date": contribution.get("SittingDate"),
                "house": contribution.get("House"),
                "debate_parents": contribution.get("debate_parents", []),
                "debate_url": contribution.get("debate_url"),
            }
        return new_data

    def get_substantial_debates(self):
        return [
            debate["info"]
            for debate in self._debates.values()
            if len(debate["contribution_ids"]) >= MINIMUM_DEBATE_HITS
        ]

    def get_substantial_debate_ids(self):
        return [
            debate_id
            for debate_id, debate in self._debates.items()
            if len(debate["contribution_ids"]) >= MINIMUM_DEBATE_HITS
        ]


class QdrantQueryHandler:
    def __init__(
        self, qdrant_client: AsyncQdrantClient, openai_client: AsyncAzureOpenAI, settings: ParliamentMCPSettings
    ):
        self.qdrant_client = qdrant_client
        self.openai_client = openai_client
        self.settings = settings
        self.sparse_text_embedding = None

    async def init(self):
        """Initialize the sparse text embedding model asynchronously."""
        if self.sparse_text_embedding is None:
            # This can be slow as it might download the model
            self.sparse_text_embedding = SparseTextEmbedding(model_name=self.settings.SPARSE_TEXT_EMBEDDING_MODEL)

    async def embed_query_dense(self, query: str) -> list[float]:
        """Embed a query using the dense text embedding model."""
        return await embed_single(
            self.openai_client,
            query,
            self.settings.AZURE_OPENAI_EMBEDDING_MODEL,
            self.settings.EMBEDDING_DIMENSIONS,
        )

    def embed_query_sparse(self, query: str) -> models.SparseVector:
        """Embed a query using the sparse text embedding model."""
        embedding = next(self.sparse_text_embedding.embed(query))
        return models.SparseVector(indices=embedding.indices, values=embedding.values)

    async def search_debate_titles(
        self,
        query: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        house: str | None = None,
        max_results: int = 50,
    ) -> list[dict]:
        """
        Search debate titles with optional filters.

        Args:
            query: Text to search for in debate titles (optional if date range is provided)
            date_from: Start date in format 'YYYY-MM-DD' (optional if query is provided)
            date_to: End date in format 'YYYY-MM-DD' (optional if query is provided)
            house: Filter by house (e.g., 'Commons', 'Lords'), optional
            max_results: Maximum number of results to return (default 100)

        Returns:
            List of debate details dictionaries

        Raises:
            ValueError: If neither query nor date range is provided
        """
        # Validate that at least one of query or date range is provided
        if not query and not date_from and not date_to:
            message = "At least one of 'query', 'date_from', or 'date_to' must be provided"
            raise ValueError(message)

        # Build filters
        filter_conditions = [
            build_date_range_filter(date_from, date_to),
            build_match_filter("House", house),
        ]

        if query:
            filter_conditions.append(FieldCondition(key="debate_parents[].Title", match=models.MatchText(text=query)))

        query_filter = build_filters(filter_conditions)

        debates = DebateCollection()

        while len(substantial_ids := debates.get_substantial_debate_ids()) < max_results:
            # Filter out already found substantial debates
            if substantial_ids:
                query_filter.must_not = [
                    FieldCondition(key="DebateSectionExtId", match=models.MatchAny(any=substantial_ids)),
                ]

            contributions, _ = await self.qdrant_client.scroll(
                collection_name=self.settings.HANSARD_CONTRIBUTIONS_COLLECTION,
                scroll_filter=query_filter,
                limit=1000,
                with_payload=True,
                order_by={"key": "SittingDate", "direction": "desc"},
            )

            if not contributions:
                break

            new_data_available = False
            for result in contributions:
                new_data_available |= debates.add_contribution(result.payload)

            if not new_data_available:
                break

        return debates.get_substantial_debates()[:max_results]

    async def search_hansard_contributions(
        self,
        query: str | None = None,
        member_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        debate_id: str | None = None,
        house: Literal["Commons", "Lords"] | None = None,
        max_results: int = 100,
        min_score: float = 0,
        # New filtering options
        exclude_member_ids: list[int] | None = None,
        parties: list[str] | None = None,
        # Diversification
        group_by: str | None = None,
        group_size: int = 1,
    ) -> list[dict] | list[list[dict]]:
        """
        Search Hansard contributions using Qdrant vector search.
        If group_by is provided (e.g. 'DebateSectionExtId'), results are diversified.
        """

        # Build filters
        filter_conditions = [
            build_match_filter("MemberId", member_id),
            build_match_filter("DebateSectionExtId", debate_id),
            build_match_filter("House", house),
            build_date_range_filter(date_from, date_to),
        ]
        
        must_not = []
        if exclude_member_ids:
            must_not.append(models.FieldCondition(key="MemberId", match=models.MatchAny(any=exclude_member_ids)))

        query_filter = models.Filter(must=[c for c in filter_conditions if c], must_not=must_not)

        if query:
            # Generate embedding for search query
            dense_query_vector = await self.embed_query_dense(query)
            sparse_query_vector = self.embed_query_sparse(query)

            if group_by:
                # Diversified Search via Groups
                query_response = await self.qdrant_client.query_points_groups(
                    collection_name=self.settings.HANSARD_CONTRIBUTIONS_COLLECTION,
                    prefetch=[
                        models.Prefetch(query=dense_query_vector, using="text_dense", filter=query_filter),
                        models.Prefetch(query=sparse_query_vector, using="text_sparse", filter=query_filter),
                    ],
                    query=models.FusionQuery(fusion=models.Fusion.RRF),
                    group_by=group_by,
                    group_size=group_size,
                    limit=max_results,
                    with_payload=True
                )
                
                results = []
                for group in query_response.groups:
                    group_hits = []
                    for hit in group.hits:
                        payload = hit.payload
                        group_hits.append({
                            "id": hit.id,
                            "text": payload.get("text", ""),
                            "date": payload.get("SittingDate"),
                            "member_name": payload.get("MemberName"),
                            "debate_title": payload.get("DebateSection", ""),
                            "score": hit.score
                        })
                    results.append(group_hits)
                return results

            # Standard Search
            query_response = await self.qdrant_client.query_points(
                collection_name=self.settings.HANSARD_CONTRIBUTIONS_COLLECTION,
                prefetch=[
                    models.Prefetch(
                        query=dense_query_vector,
                        using="text_dense",
                        limit=max_results,
                        filter=query_filter,
                    ),
                    models.Prefetch(
                        query=sparse_query_vector,
                        using="text_sparse",
                        limit=max_results,
                        filter=query_filter,
                    ),
                ],
                query=models.FusionQuery(
                    fusion=models.Fusion.RRF,
                ),
                limit=max_results,
                score_threshold=min_score,
                with_payload=True,
            )

            query_points = query_response.points
        else:
            # If no query, use scroll to get results with filters only
            query_points, _ = await self.qdrant_client.scroll(
                collection_name=self.settings.HANSARD_CONTRIBUTIONS_COLLECTION,
                scroll_filter=query_filter,
                limit=max_results,
                with_payload=True,
                with_vectors=False,
                order_by={
                    "key": "SittingDate",
                    "direction": "desc",
                },
            )

        results = []
        for result in query_points:
            payload = result.payload
            results.append(
                {
                    "id": result.id,
                    "text": payload.get("text", ""),
                    "date": payload.get("SittingDate"),
                    "house": payload.get("House"),
                    "member_id": payload.get("MemberId"),
                    "member_name": payload.get("MemberName"),
                    "relevance_score": result.score if hasattr(result, "score") else 1.0,
                    "debate_title": payload.get("DebateSection", ""),
                    "debate_id": payload.get("DebateSectionExtId"),
                    "debate_url": payload.get("debate_url", ""),
                    "contribution_id": payload.get("ContributionExtId"),
                    "contribution_url": payload.get("contribution_url", ""),
                    "order_in_debate": payload.get("OrderInDebateSection"),
                    "debate_parents": payload.get("debate_parents", []),
                }
            )

        # Sort by relevance score if we have a query, otherwise by date and order
        if query:
            results.sort(key=lambda x: x["relevance_score"], reverse=True)
        else:
            results.sort(
                key=lambda x: (
                    x.get("date", ""),
                    x.get("order_in_debate", 0),
                )
            )

        return results

    async def recommend_contributions(
        self,
        positive_ids: list[str],
        negative_ids: list[str] | None = None,
        max_results: int = 10,
        filter_conditions: list | None = None,
    ) -> list[dict]:
        """Find contributions similar to positive_ids and dissimilar to negative_ids."""
        query_filter = build_filters(filter_conditions) if filter_conditions else None
        
        response = await self.qdrant_client.query_points(
            collection_name=self.settings.HANSARD_CONTRIBUTIONS_COLLECTION,
            query=models.RecommendQuery(
                recommend=models.RecommendInput(
                    positive=positive_ids,
                    negative=negative_ids or [],
                )
            ),
            using="text_dense",
            query_filter=query_filter,
            limit=max_results,
            with_payload=True
        )
        
        return [
            {
                "id": hit.id,
                "text": hit.payload.get("text", ""),
                "member_name": hit.payload.get("MemberName"),
                "date": hit.payload.get("SittingDate"),
                "score": hit.score
            }
            for hit in response.points
        ]

    async def discover_contributions(
        self,
        target_id: str,
        context_pairs: list[tuple[str, str]],
        max_results: int = 10,
        filter_conditions: list | None = None,
    ) -> list[dict]:
        """
        Use Discovery API to find points based on target and context pairs.
        context_pairs: list of (positive_id, negative_id)
        """
        query_filter = build_filters(filter_conditions) if filter_conditions else None
        
        context = [
            models.ContextPair(positive=p, negative=n)
            for p, n in context_pairs
        ]
        
        response = await self.qdrant_client.query_points(
            collection_name=self.settings.HANSARD_CONTRIBUTIONS_COLLECTION,
            query=models.DiscoverQuery(
                discover=models.DiscoverInput(
                    target=target_id,
                    context=context
                )
            ),
            using="text_dense",
            query_filter=query_filter,
            limit=max_results,
            with_payload=True
        )
        
        return [
            {
                "id": hit.id,
                "text": hit.payload.get("text", ""),
                "member_name": hit.payload.get("MemberName"),
                "date": hit.payload.get("SittingDate"),
                "score": hit.score
            }
            for hit in response.points
        ]

    async def find_relevant_contributors(
        self,
        query: str,
        num_contributors: int = 10,
        num_contributions: int = 10,
        date_from: str | None = None,
        date_to: str | None = None,
        house: Literal["Commons", "Lords"] | None = None,
    ) -> list[dict]:
        """
        Find the most relevant parliamentary contributors and their contributions.

        Groups Hansard contributions by member ID and returns the top contributors
        with their most relevant contributions for the given search query.

        Args:
            query: Text to search for in contributions
            num_contributors: Number of top contributors to return (default 10)
            num_contributions: Number of top contributions per contributor (default 10)
            date_from: Start date filter in 'YYYY-MM-DD' format (optional)
            date_to: End date filter in 'YYYY-MM-DD' format (optional)
            house: Filter by house - "Commons" or "Lords" (optional)

        Returns:
            List of contributor groups, each containing the member's contributions
        """
        # Fail if none of the parameters are provided
        if not query:
            msg = "A query must be provided"
            raise ValueError(msg)

        # Build filters
        query_filter = build_filters(
            [
                build_match_filter("House", house),
                build_date_range_filter(date_from, date_to),
            ]
        )

        # Generate embedding for search query
        dense_query_vector = await self.embed_query_dense(query)
        sparse_query_vector = self.embed_query_sparse(query)

        # Perform vector search
        query_response = await self.qdrant_client.query_points_groups(
            collection_name=self.settings.HANSARD_CONTRIBUTIONS_COLLECTION,
            prefetch=[
                models.Prefetch(
                    query=dense_query_vector,
                    using="text_dense",
                    filter=query_filter,
                ),
                models.Prefetch(
                    query=sparse_query_vector,
                    using="text_sparse",
                    filter=query_filter,
                ),
            ],
            query=models.FusionQuery(
                fusion=models.Fusion.RRF,
            ),
            limit=num_contributors,
            score_threshold=0,
            with_payload=True,
            group_by="MemberId",
            group_size=num_contributions,
        )

        results = []
        for group in query_response.groups:
            group_results = []
            for hit in group.hits:
                payload = hit.payload
                group_results.append(
                    {
                        "text": payload.get("text", ""),
                        "date": payload.get("SittingDate"),
                        "house": payload.get("House"),
                        "member_id": payload.get("MemberId"),
                        "member_name": payload.get("MemberName"),
                        "relevance_score": hit.score if hasattr(hit, "score") else 1.0,
                        "debate_title": payload.get("DebateSection", ""),
                        "debate_url": payload.get("debate_url", ""),
                        "contribution_url": payload.get("contribution_url", ""),
                        "order_in_debate": payload.get("OrderInDebateSection"),
                        "debate_parents": payload.get("debate_parents", []),
                    }
                )

            results.append(group_results)

        return results

    async def search_parliamentary_questions(
        self,
        query: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        party: str | None = None,
        asking_member_id: int | None = None,
        answering_body_name: str | None = None,
        min_score: float = 0,
        max_results: int = 25,
    ) -> list[dict]:
        """
        Search Parliamentary Questions using Qdrant vector search.

        Args:
            query: Text to search for in parliamentary questions
            date_from: Start date in format 'YYYY-MM-DD' (optional)
            date_to: End date in format 'YYYY-MM-DD' (optional)
            party: Filter by party (optional)
            asking_member_id: Filter by member id (optional)
            answering_body_name: Filter by answering body name (optional)
            min_score: Minimum relevance score (default 0)
            max_results: Maximum number of results to return (default 25)
        """
        # Build filters
        filter_conditions = [
            build_date_range_filter(date_from, date_to, "dateTabled"),
            build_match_filter("askingMember.party", party),
            build_match_filter("askingMember.id", asking_member_id),
        ]

        if answering_body_name:
            filter_conditions.append(
                FieldCondition(key="answeringBodyName", match=models.MatchText(text=answering_body_name))
            )

        query_filter = build_filters(filter_conditions)

        # First find the ID of any questions with any relevant chunks
        if query:
            # Generate embedding for search query
            dense_query_vector = await self.embed_query_dense(query)
            sparse_query_vector = self.embed_query_sparse(query)

            # Perform vector search
            query_response = await self.qdrant_client.query_points(
                collection_name=self.settings.PARLIAMENTARY_QUESTIONS_COLLECTION,
                prefetch=[
                    models.Prefetch(
                        query=dense_query_vector,
                        using="text_dense",
                        limit=max_results,
                        filter=query_filter,
                    ),
                    models.Prefetch(
                        query=sparse_query_vector,
                        using="text_sparse",
                        limit=max_results,
                        filter=query_filter,
                    ),
                ],
                query=models.FusionQuery(
                    fusion=models.Fusion.RRF,
                ),
                limit=max_results,
                score_threshold=min_score,
                with_payload=True,
            )

            relevant_questions_ids = [hit.payload["id"] for hit in query_response.points]

        else:
            # If no query, use scroll to get results with filters only
            query_response, _ = await self.qdrant_client.scroll(
                collection_name=self.settings.PARLIAMENTARY_QUESTIONS_COLLECTION,
                scroll_filter=query_filter,
                limit=max_results,
                with_payload=True,
                order_by={
                    "key": "id",
                    "direction": "desc",
                },
            )

            relevant_questions_ids = [record.payload["id"] for record in query_response]

        if len(relevant_questions_ids) == 0:
            return []

        # Then get the full details of the questions
        query_response = await self.qdrant_client.query_points_groups(
            collection_name=self.settings.PARLIAMENTARY_QUESTIONS_COLLECTION,
            query_filter=Filter(
                must=[
                    models.FieldCondition(key="id", match=models.MatchAny(any=relevant_questions_ids)),
                ]
            ),
            limit=max_results,
            with_payload=True,
            with_vectors=False,
            group_by="id",
            group_size=100,
        )

        results = []
        for group in query_response.groups:
            # For PQs, question and answer chunks are stored as separate chunks, so we have to piece them together

            payloads = [hit.payload for hit in group.hits]
            answer_chunks = [(hit["chunk_id"], hit["text"]) for hit in payloads if hit["chunk_type"] == "answer"]
            question_chunks = [(hit["chunk_id"], hit["text"]) for hit in payloads if hit["chunk_type"] == "question"]

            answer_text = "\n".join([text for _, text in sorted(answer_chunks)])
            question_text = "\n".join([text for _, text in sorted(question_chunks)])

            # use the latest created_at payload
            payload = max(payloads, key=lambda x: x.get("created_at"))

            uin = payload.get("uin")
            tabled_date = parse_date(payload.get("dateTabled"))

            results.append(
                (
                    payload.get("created_at"),
                    {
                        "question_text": question_text,
                        "answer_text": answer_text,
                        "chunk_type": payload.get("chunk_type"),
                        "askingMember": payload.get("askingMember"),
                        "answeringMember": payload.get("answeringMember"),
                        "dateTabled": parse_date(payload.get("dateTabled")),
                        "dateAnswered": parse_date(payload.get("dateAnswered")),
                        "answeringBodyName": payload.get("answeringBodyName"),
                        "question_url": f"https://questions-statements.parliament.uk/written-questions/detail/{tabled_date}/{uin}",
                        "created_at": payload.get("created_at"),
                    },
                )
            )
        # return the most recently updated question
        return [result for _, result in sorted(results, key=lambda x: x[0], reverse=True)]

    async def get_full_debate(self, debate_id: str) -> list[dict]:
        """
        Retrieve the full content of a debate by its ID.
        Reconstructs contributions from chunks and sorts them by order.
        """
        # Filter by debate ID
        query_filter = models.Filter(
            must=[
                models.FieldCondition(
                    key="DebateSectionExtId",
                    match=models.MatchValue(value=debate_id),
                )
            ]
        )

        # Scroll to get all chunks for this debate
        all_points = []
        next_page_offset = None
        
        while True:
            points, next_page_offset = await self.qdrant_client.scroll(
                collection_name=self.settings.HANSARD_CONTRIBUTIONS_COLLECTION,
                scroll_filter=query_filter,
                limit=1000,
                with_payload=True,
                with_vectors=False,
                offset=next_page_offset,
            )
            all_points.extend(points)
            if next_page_offset is None:
                break

        if not all_points:
            return []

        # Group chunks by contribution (using document_uri or equivalent unique ID)
        # We can use the 'id' of the point, but we need to know which contribution it belongs to.
        # The payload contains 'ContributionExtId' or we can infer from 'chunk_id' structure if available,
        # but 'ContributionExtId' might be None for some.
        # However, 'OrderInDebateSection' + 'DebateSectionExtId' should be unique per contribution if 'ContributionExtId' is missing.
        # Let's rely on grouping by the prefix of the chunk_id if possible, or OrderInDebateSection.
        
        # Actually, in models.py: chunk_id = f"{self.document_uri}_chunk_{chunk_id}"
        # So we can group by extracting the document_uri from the chunk_id (everything before the last _chunk_)
        
        contributions_map = defaultdict(list)
        
        for point in all_points:
            payload = point.payload
            chunk_id_str = payload.get("chunk_id", "")
            
            # Extract base ID (everything before the last _chunk_X)
            # Find the last occurrence of "_chunk_"
            if "_chunk_" in chunk_id_str:
                base_id = chunk_id_str.rsplit("_chunk_", 1)[0]
                chunk_index = int(chunk_id_str.rsplit("_chunk_", 1)[1])
            else:
                # Fallback if naming convention changes, though it shouldn't based on models.py
                base_id = payload.get("ContributionExtId") or f"{payload.get('OrderInDebateSection')}"
                chunk_index = 0

            contributions_map[base_id].append((chunk_index, point))

        results = []
        
        for base_id, chunks in contributions_map.items():
            # Sort chunks by index
            chunks.sort(key=lambda x: x[0])
            
            # Reconstruct full text
            full_text = "".join(chunk[1].payload.get("text", "") for chunk in chunks)
            
            # Get metadata from the first chunk (should be same for all)
            first_payload = chunks[0][1].payload
            
            results.append({
                "contribution_id": base_id,
                "text": full_text,
                "speaker": first_payload.get("AttributedTo"),
                "member_id": first_payload.get("MemberId"),
                "member_name": first_payload.get("MemberName"),
                "order_in_debate": first_payload.get("OrderInDebateSection"),
                "house": first_payload.get("House"),
                "date": first_payload.get("SittingDate"),
                "debate_section": first_payload.get("DebateSection"),
                "debate_id": first_payload.get("DebateSectionExtId"),
            })

        # Sort contributions by order in debate
        results.sort(key=lambda x: x.get("order_in_debate") or 0)
        
        return results

    async def get_full_contribution(self, contribution_id: str) -> dict | None:
        """
        Retrieve the full content of a specific contribution by its ID.
        Reconstructs the contribution from chunks.
        """
        # Filter by ContributionExtId
        query_filter = models.Filter(
            must=[
                models.FieldCondition(
                    key="ContributionExtId",
                    match=models.MatchValue(value=contribution_id),
                )
            ]
        )

        # Scroll to get all chunks for this contribution
        all_points = []
        next_page_offset = None
        
        while True:
            points, next_page_offset = await self.qdrant_client.scroll(
                collection_name=self.settings.HANSARD_CONTRIBUTIONS_COLLECTION,
                scroll_filter=query_filter,
                limit=100, # Contributions shouldn't be massive, 100 chunks is plenty
                with_payload=True,
                with_vectors=False,
                offset=next_page_offset,
            )
            all_points.extend(points)
            if next_page_offset is None:
                break

        if not all_points:
            return None

        # Sort chunks by chunk_id index
        # chunk_id format: ..._chunk_{index}
        def get_chunk_index(point):
            chunk_id_str = point.payload.get("chunk_id", "")
            if "_chunk_" in chunk_id_str:
                return int(chunk_id_str.rsplit("_chunk_", 1)[1])
            return 0

        all_points.sort(key=get_chunk_index)

        # Reconstruct full text
        full_text = "".join(point.payload.get("text", "") for point in all_points)
        
        # Get metadata from the first chunk
        first_payload = all_points[0].payload
        
        return {
            "contribution_id": first_payload.get("ContributionExtId"),
            "text": full_text,
            "speaker": first_payload.get("AttributedTo"),
            "member_id": first_payload.get("MemberId"),
            "member_name": first_payload.get("MemberName"),
            "order_in_debate": first_payload.get("OrderInDebateSection"),
            "house": first_payload.get("House"),
            "date": first_payload.get("SittingDate"),
            "debate_section": first_payload.get("DebateSection"),
            "debate_id": first_payload.get("DebateSectionExtId"),
            "debate_url": first_payload.get("debate_url"),
            "contribution_url": first_payload.get("contribution_url"),
        }

    async def get_contribution_neighbors(self, contribution_id: str) -> list[dict]:
        """
        Retrieve the contribution itself plus the one immediately before and after it.
        Useful for context (question/answer pairs).
        """
        # 1. Get target contribution metadata
        # We use a limit=1 scroll to get just one chunk of the target
        target_filter = models.Filter(
            must=[
                models.FieldCondition(
                    key="ContributionExtId",
                    match=models.MatchValue(value=contribution_id),
                )
            ]
        )
        
        points, _ = await self.qdrant_client.scroll(
            collection_name=self.settings.HANSARD_CONTRIBUTIONS_COLLECTION,
            scroll_filter=target_filter,
            limit=1,
            with_payload=True,
            with_vectors=False,
        )
        
        if not points:
            return []
            
        payload = points[0].payload
        debate_id = payload.get("DebateSectionExtId")
        order = payload.get("OrderInDebateSection")
        
        if debate_id is None or order is None:
            # Fallback: if we can't determine order, return just the target wrapped in list
            result = await self.get_full_contribution(contribution_id)
            return [result] if result else []
            
        # 2. Query neighbors
        # Range: [order-1, order+1]
        neighbor_filter = models.Filter(
            must=[
                models.FieldCondition(
                    key="DebateSectionExtId",
                    match=models.MatchValue(value=debate_id),
                ),
                models.FieldCondition(
                    key="OrderInDebateSection",
                    range=models.Range(
                        gte=order - 1,
                        lte=order + 1
                    ),
                )
            ]
        )
        
        # Scroll to get all chunks for these 3 (max) contributions
        all_points = []
        next_page_offset = None
        while True:
            points, next_page_offset = await self.qdrant_client.scroll(
                collection_name=self.settings.HANSARD_CONTRIBUTIONS_COLLECTION,
                scroll_filter=neighbor_filter,
                limit=300, # 3 contributions * ~100 chunks
                with_payload=True,
                with_vectors=False,
                offset=next_page_offset,
            )
            all_points.extend(points)
            if next_page_offset is None:
                break
                
        if not all_points:
            return []

        # 3. Reconstruct
        contributions_map = defaultdict(list)
        for point in all_points:
            payload = point.payload
            # Use ContributionExtId or fallback
            base_id = payload.get("ContributionExtId") or f"{payload.get('OrderInDebateSection')}"
            
            chunk_id_str = payload.get("chunk_id", "")
            if "_chunk_" in chunk_id_str:
                chunk_index = int(chunk_id_str.rsplit("_chunk_", 1)[1])
            else:
                chunk_index = 0
                
            contributions_map[base_id].append((chunk_index, point))

        results = []
        for base_id, chunks in contributions_map.items():
            chunks.sort(key=lambda x: x[0])
            full_text = "".join(chunk[1].payload.get("text", "") for chunk in chunks)
            first_payload = chunks[0][1].payload
            
            results.append({
                "contribution_id": base_id,
                "text": full_text,
                "speaker": first_payload.get("AttributedTo"),
                "member_id": first_payload.get("MemberId"),
                "member_name": first_payload.get("MemberName"),
                "order_in_debate": first_payload.get("OrderInDebateSection"),
                "house": first_payload.get("House"),
                "date": first_payload.get("SittingDate"),
                "debate_section": first_payload.get("DebateSection"),
                "debate_id": first_payload.get("DebateSectionExtId"),
                "debate_url": first_payload.get("debate_url"),
                "contribution_url": first_payload.get("contribution_url"),
            })

        # Sort by order
        results.sort(key=lambda x: x.get("order_in_debate") or 0)
        return results
