import logging
import re
import csv
import os
from datetime import datetime

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

class CostTracker:
    def __init__(self, log_file="data/token_usage.csv"):
        self.log_file = log_file
        # Ensure directory exists
        try:
            os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
            if not os.path.exists(self.log_file):
                with open(self.log_file, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(["timestamp", "model", "prompt_tokens", "total_tokens", "cost_usd"])
        except Exception as e:
            logger.error(f"Failed to initialize CostTracker: {e}")

    def log_usage(self, model: str, prompt_tokens: int, total_tokens: int):
        # Pricing for text-embedding-3-large is $0.00013 / 1k tokens
        price_per_1k = 0.00013
        cost = (total_tokens / 1000) * price_per_1k
        
        try:
            with open(self.log_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([datetime.now().isoformat(), model, prompt_tokens, total_tokens, f"{cost:.6f}"])
        except Exception as e:
            logger.error(f"Failed to log cost: {e}")

_cost_tracker = CostTracker()

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
        
        # Log usage
        if hasattr(response, 'usage'):
            _cost_tracker.log_usage(model, response.usage.prompt_tokens, response.usage.total_tokens)

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
