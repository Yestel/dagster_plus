import asyncio
import pytest
from dagster_plus.resources.async_crawler import AsyncCrawler
from unittest.mock import MagicMock

@pytest.mark.asyncio
async def test_concurrency_limit():
    # Create an instance with a small limit
    crawler = AsyncCrawler(concurrency_limit=2)
    
    # Counter for active requests
    active_requests = 0
    max_active_requests = 0
    
    async def mock_fetch(session, url, headers):
        nonlocal active_requests, max_active_requests
        active_requests += 1
        max_active_requests = max(max_active_requests, active_requests)
        await asyncio.sleep(0.1) # Simulate network delay
        active_requests -= 1
        return {"url": url}

    # Mock _fetch to use our tracking function
    # We need to bind it to the instance or just replace it if we don't use 'self' inside mock_fetch (we don't)
    crawler._fetch = mock_fetch
    
    urls = [f"http://example.com/{i}" for i in range(10)]
    
    # Run fetch_all
    results = await crawler.fetch_all(urls)
    
    # Verify results
    assert len(results) == 10
    
    # Verify concurrency limit was respected
    print(f"Max active requests: {max_active_requests}")
    assert max_active_requests <= 2
    assert max_active_requests > 0

if __name__ == "__main__":
    asyncio.run(test_concurrency_limit())
