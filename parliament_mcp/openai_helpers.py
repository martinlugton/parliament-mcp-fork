import logging
import re

try:
    from itertools import batched
except ImportError:
    from itertools import islice

    def batched(iterable, n):
        if n < 1:
            raise ValueError('n must be at least one')
        it = iter(iterable)
        while batch := tuple(islice(it, n)):
            yield batch

import httpx
from aiolimiter import AsyncLimiter
from openai import AsyncAzureOpenAI, RateLimitError
from tenacity import retry, stop_after_attempt, wait_exponential

from parliament_mcp.settings import ParliamentMCPSettings, settings

logger = logging.getLogger(__name__)

# Global rate limiter for OpenAI embedding requests
# We use max_rate=1 and adjust time_period to achieve the desired rate (e.g. 0.1 req/s = 1 req / 10s)
openai_rate_limiter = AsyncLimiter(max_rate=1.0, time_period=1.0 / settings.EMBEDDING_MAX_RATE_PER_SECOND)


def get_openai_client(settings: ParliamentMCPSettings) -> AsyncAzureOpenAI:
    """Get an async Azure OpenAI client."""
    return AsyncAzureOpenAI(
        api_key=settings.AZURE_OPENAI_API_KEY,
        api_version=settings.AZURE_OPENAI_API_VERSION,
        azure_endpoint=settings.AZURE_OPENAI_ENDPOINT,
        http_client=httpx.AsyncClient(timeout=30.0),
    )


async def embed_single(
    client: AsyncAzureOpenAI,
    text: str,
    model: str,
    dimensions: int = 1024,
) -> list[float]:
    """Generate a single embedding for a text using Azure OpenAI."""
    response = await client.embeddings.create(
        input=text,
        model=model,
        dimensions=dimensions,
    )
    return response.data[0].embedding


class wait_azure_rate_limit(wait_exponential):
    """Wait strategy that handles Azure OpenAI rate limit errors."""

    def __call__(self, retry_state):
        """Calculate wait time based on exception or exponential backoff."""
        exc = retry_state.outcome.exception()
        if isinstance(exc, RateLimitError):
            # Check for "retry after X seconds" in the error message
            match = re.search(r"retry after (\d+) seconds", str(exc), re.IGNORECASE)
            if match:
                wait_time = float(match.group(1)) + 5.0  # Add 5s buffer
                logger.warning("Rate limit hit. Waiting for %s seconds.", wait_time)
                return wait_time

        return super().__call__(retry_state)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_azure_rate_limit(multiplier=1, min=4, max=60),
)
async def _embed_chunk(
    client: AsyncAzureOpenAI,
    batch: tuple[str, ...],
    model: str,
    dimensions: int,
) -> list[list[float]]:
    """Embed a single chunk of texts with retry logic."""
    try:
        async with openai_rate_limiter:
            response = await client.embeddings.create(
                input=batch,
                model=model,
                dimensions=dimensions,
            )
        return [item.embedding for item in response.data]
    except ValueError as e:
        logger.error("ValueError in _embed_chunk: %s. Batch size: %d", e, len(batch))
        raise
    except Exception:
        # Other exceptions are handled by tenacity and eventually bubbled up
        raise


async def embed_batch(
    client: AsyncAzureOpenAI,
    texts: list[str],
    model: str,
    dimensions: int = 1024,
    batch_size: int = 100,
) -> list[list[float]]:
    """Generate embeddings for a list of texts using Azure OpenAI."""
    all_embeddings = []
    
    total_chunks = (len(texts) + batch_size - 1) // batch_size
    logger.info("Generating embeddings for %d texts in %d batches", len(texts), total_chunks)

    for i, batch in enumerate(batched(texts, batch_size)):
        logger.info("Progress: %d/%d embedding batches completed", i + 1, total_chunks)
            
        batch_embeddings = await _embed_chunk(client, batch, model, dimensions)
        all_embeddings.extend(batch_embeddings)

    return all_embeddings
