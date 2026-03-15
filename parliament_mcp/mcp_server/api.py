import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, Literal

import sentry_sdk
from mcp.server.fastmcp.server import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import Field

from parliament_mcp.mcp_server.members import register_members_tools
from parliament_mcp.mcp_server.qdrant_query_handler import QdrantQueryHandler
from parliament_mcp.openai_helpers import get_openai_client
from parliament_mcp.qdrant_helpers import get_async_qdrant_client
from parliament_mcp.settings import settings

from .committees import register_committee_tools
from .utils import log_tool_call

logger = logging.getLogger(__name__)


@asynccontextmanager
async def mcp_lifespan(_server: FastMCP) -> AsyncGenerator[dict]:
    """Manage application lifecycle with type-safe context"""
    # Initialize on startup

    openai_client = get_openai_client(settings)
    async with get_async_qdrant_client(settings) as qdrant_client:
        yield {
            "qdrant_query_handler": QdrantQueryHandler(qdrant_client, openai_client, settings),
            "openai_client": openai_client,
        }


mcp_server = FastMCP(
    name="Parliament MCP Server",
    stateless_http=False,
    lifespan=mcp_lifespan,
    # Configure transport security with allowed hosts from settings
    transport_security=TransportSecuritySettings(
        allowed_hosts=[h.strip() for h in settings.MCP_ALLOWED_HOSTS.split(",") if h.strip()],
    ),
)

register_committee_tools(mcp_server)
register_members_tools(mcp_server)

# init Sentry if configured
if settings.SENTRY_DSN and settings.ENVIRONMENT in ["dev", "preprod", "prod"] and settings.SENTRY_DSN != "placeholder":
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        traces_sample_rate=1.0,
        profiles_sample_rate=1.0,
    )


@mcp_server.tool("search_parliamentary_questions")
@log_tool_call
async def search_parliamentary_questions(
    query: str | None = Field(None, description="Search query"),
    date_from: str | None = Field(None, description="Start date (YYYY-MM-DD)"),
    date_to: str | None = Field(None, description="End date (YYYY-MM-DD)"),
    party: str | None = Field(None, description="Party"),
    asking_member_id: int | None = Field(None, description="Member ID of the asking member"),
    answering_body_name: str | None = Field(
        None,
        description="Answering body name (e.g. 'Department for Transport, Cabinet Office, etc.)",
    ),
    max_results: int = Field(25, description="Max results, default 25"),
) -> Any:
    """
    Search Parliamentary Written Questions (sometimes known as PQs)

    With no query provided, this function will return the most recent written questions matching the other filters.

    Written questions allow MPs and Members of the House of Lords to ask for information on the work, policy and activities of Government departments, related bodies, and the administration of Parliament.

    Common use case for this function:
    - Provide a query to search for written questions on a specific topic for all time
    - Provide a query and date range to search for written questions on a specific topic in a specific date range
    - Provide a query, date range, party and member name to search for written questions on a specific topic in a specific date range by a specific member of a specific party
    - Provide a member name to search for all written questions by a specific member for all time
    - Provide a member id to search for all written questions by a specific member for all time
    - Provide an answering body name to search for all questions answered by a specific body or department such as 'Department for Transport' or 'Cabinet Office'
    """
    ctx = mcp_server.get_context()
    qdrant_query_handler: QdrantQueryHandler = ctx.request_context.lifespan_context["qdrant_query_handler"]
    result = await qdrant_query_handler.search_parliamentary_questions(
        query=query,
        date_from=date_from,
        date_to=date_to,
        party=party,
        asking_member_id=asking_member_id,
        answering_body_name=answering_body_name,
        max_results=max_results,
    )

    if not result:
        return "No results found"

    return result


@mcp_server.tool("search_debate_titles")
@log_tool_call
async def search_debate_titles(
    query: str | None = Field(None, description="Query used to search debate titles"),
    date_from: str | None = Field(None, description="Date from (YYYY-MM-DD)"),
    date_to: str | None = Field(None, description="Date to (YYYY-MM-DD)"),
    house: Literal["Commons", "Lords"] | None = Field(None, description="House (Commons|Lords)"),
    max_results: int = Field(50, description="Max results"),
) -> Any:
    """
    Search through the titles of debates for a given query, or by date range, and house.
    Only returns the debate ID, title, and date, not the content of the debate.
    Useful for finding relevant debates, but must be used in conjunction with search_contributions to get the full text of the debate.

    Use date_from and date_to to search for debates in a specific date range.
    Use query to search for debates with a keyword or phrase in the title.

    Returns a list of debate details (ID, title, date) in order the most recent first.

    Common use case for this function:
    - Provide a query to search for debates on a specific topic
    - Provide the date for date_from and date_to to search for debates within a specific date range
    - Provide a query and date range to search for debates on a specific topic in a specific date range

    Args:
        query: Text to search for in debate titles (optional if date range is provided)
        date_from: Start date in format 'YYYY-MM-DD' (optional if query is provided)
        date_to: End date in format 'YYYY-MM-DD' (optional if query is provided)
        house: Filter by house (e.g., 'Commons', 'Lords'), optional
        max_results: Maximum number of results to return (default 50)

    Returns:
        List of debate details dictionaries
    """
    ctx = mcp_server.get_context()
    qdrant_query_handler: QdrantQueryHandler = ctx.request_context.lifespan_context["qdrant_query_handler"]
    result = await qdrant_query_handler.search_debate_titles(
        query=query,
        date_from=date_from,
        date_to=date_to,
        house=house,
        max_results=max_results,
    )

    if not result:
        return "No results found"

    return result


@mcp_server.tool("find_relevant_contributors")
@log_tool_call
async def find_relevant_contributors(
    query: str = Field(..., description="Query used to search for relevant contributors"),
    num_contributors: int = Field(10, description="Number of contributors to return"),
    num_contributions: int = Field(10, description="Number of contributions to return"),
    date_from: str | None = Field(None, description="Date from (YYYY-MM-DD)"),
    date_to: str | None = Field(None, description="Date to (YYYY-MM-DD)"),
    house: Literal["Commons", "Lords"] | None = Field(None, description="House (Commons|Lords)"),
) -> Any:
    """
    Find the most relevant parliamentary contributors and their contributions for a given query.

    Returns a list of contributor groups, each containing the member's contributions.

    Common use cases:
    - Provide a query to search for relevant contributors and their contributions for a specific topic
    - Provide a query, date range, and house to search for relevant contributors and their contributions for a specific topic in a specific date range and house
    """
    ctx = mcp_server.get_context()
    qdrant_query_handler: QdrantQueryHandler = ctx.request_context.lifespan_context["qdrant_query_handler"]
    result = await qdrant_query_handler.find_relevant_contributors(
        query=query,
        num_contributors=num_contributors,
        num_contributions=num_contributions,
        date_from=date_from,
        date_to=date_to,
        house=house,
    )

    if not result:
        return "No results found"

    return result


# Hansard endpoints
@mcp_server.tool("search_contributions")
@log_tool_call
async def search_contributions(
    query: str | None = Field(
        None,
        description="""Searches within the actual spoken words/text of parliamentary contributions.
        Use this to find specific phrases, words, or topics that were mentioned during debates.
        For example, 'climate change' would find any time a member actually said something related to climate change.""",
    ),
    member_id: int | None = Field(None, description="Member ID, used to filter by a specific member"),
    date_from: str | None = Field(None, description="Date from (YYYY-MM-DD)"),
    date_to: str | None = Field(None, description="Date to (YYYY-MM-DD)"),
    debate_id: str | None = Field(None, description="Debate ID (Also called)"),
    house: Literal["Commons", "Lords"] | None = Field(None, description="House (Commons|Lords)"),
    max_results: int = Field(50, description="Max results"),
) -> Any:
    """
    Search Hansard parliamentary records for contributions using searching within the actual spoken words
    A contribution is something a member said in the houses of parliament during a debate.

    With no query provided, this function will return the most recent contributions matching the other filters.

    Common use cases:
    - Search what was actually said: Use query="climate change" to find mentions in speeches
    - Member-specific search: Add memberId to find contributions by a specific member
    - Search by topic for a given memberId to find contributions on a specific topic by a specific member
    - Time-bounded search: Add date_from/date_to to search within a specific period

    For the best results, you probably want to use a large max_results.

    Returns:
        List of Hansard details dictionaries. Each one is a single contribution made during
        a debate in either the House of Commons or the House of Lords.
        Each dictionary contains:
            - contribution_id: ID of the contribution
            - text: Text of the contribution
            - member_name: Member who made the contribution
            - date: Date of the contribution
            - house: House of the contribution
            - member_id: ID of the member who made the contribution
            - debate_id: ID of the debate (used for get_full_debate_content)
    """
    ctx = mcp_server.get_context()
    qdrant_query_handler: QdrantQueryHandler = ctx.request_context.lifespan_context["qdrant_query_handler"]
    result = await qdrant_query_handler.search_hansard_contributions(
        query=query,
        member_id=member_id,
        date_from=date_from,
        date_to=date_to,
        debate_id=debate_id,
        house=house,
        max_results=max_results,
    )

    if not result:
        return "No results found"

    return result


@mcp_server.tool("get_full_debate_content")
@log_tool_call
async def get_full_debate_content(
    debate_id: str = Field(..., description="The DebateSectionExtId of the debate to retrieve"),
) -> Any:
    """
    Retrieve the full transcript of a specific debate.
    This reconstructs the entire debate by gathering all contributions (speeches),
    ordering them correctly, and returning the full text.

    Common use cases:
    - After finding a relevant speech via search_contributions, use the debate_id to read the whole debate.
    - Read a full day's proceedings for a specific committee or session.

    Returns:
        List of contribution dictionaries in chronological order.
    """
    ctx = mcp_server.get_context()
    qdrant_query_handler: QdrantQueryHandler = ctx.request_context.lifespan_context["qdrant_query_handler"]
    result = await qdrant_query_handler.get_full_debate(debate_id=debate_id)

    if not result:
        return "No results found"

    return result


@mcp_server.tool("get_full_contribution_content")
@log_tool_call
async def get_full_contribution_content(
    contribution_id: str = Field(..., description="The ContributionExtId of the contribution to retrieve"),
) -> Any:
    """
    Retrieve the full text of a specific contribution (submission) by its ID.
    This reconstructs the contribution from its vector chunks.

    Common use cases:
    - Read the full text of a long speech that was truncated in search results.
    - Get a clean, unabridged version of a specific member's statement.

    Returns:
        A dictionary containing the full text and metadata for the contribution.
    """
    ctx = mcp_server.get_context()
    qdrant_query_handler: QdrantQueryHandler = ctx.request_context.lifespan_context["qdrant_query_handler"]
    result = await qdrant_query_handler.get_full_contribution(contribution_id=contribution_id)

    if not result:
        return "No results found"

    return result


@mcp_server.tool("get_contribution_context")
@log_tool_call
async def get_contribution_context(
    contribution_id: str = Field(..., description="The ContributionExtId of the contribution to retrieve context for"),
) -> Any:
    """
    Retrieve a specific contribution along with its immediate context (previous and next contributions).
    This is useful for seeing a question and its answer, or an intervention and the response.

    Common use cases:
    - See the question that prompted a specific answer.
    - See the immediate reaction/next speech following a contribution.

    Returns:
        List of up to 3 contributions (previous, target, next), sorted by their order in the debate.
    """
    ctx = mcp_server.get_context()
    qdrant_query_handler: QdrantQueryHandler = ctx.request_context.lifespan_context["qdrant_query_handler"]
    result = await qdrant_query_handler.get_contribution_neighbors(contribution_id=contribution_id)

    if not result:
        return "No results found"

    return result
