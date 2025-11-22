import aiohttp
import asyncio
from dagster import ConfigurableResource, get_dagster_logger
from typing import Any, Dict, Optional
from tenacity import retry, stop_after_attempt, wait_fixed


def _retry_error_callback(context):
    log = context.args[0].log
    log.error(f"Max retries exceeded: {context.outcome.exception()}")
    raise Exception('Max retries exceeded', context.outcome.exception())


class AsyncCrawlerClient:
    def __init__(self, log):
        self.log = log

    async def get(self, url: str, headers: Optional[Dict[str, str]] = None) -> Any:
        self.log.info(f"Fetching URL: {url}")
        async with aiohttp.ClientSession() as session:
            return await self._fetch(session, url, headers)

    async def fetch_all(self, urls: list[str], headers: Optional[Dict[str, str]] = None, concurrency_limit: Optional[int] = 10) -> list[Any]:
        self.log.info(f"Fetching {len(urls)} URLs with concurrency limit {concurrency_limit}")
        semaphore = asyncio.Semaphore(concurrency_limit)

        async def fetch_with_semaphore(session, url, headers):
            async with semaphore:
                try:
                    return await self._fetch(session, url, headers)
                except Exception as e:
                    # self.log.error(f"Failed to fetch {url} after retries: {e}")
                    return None

        async with aiohttp.ClientSession() as session:
            tasks = []
            for url in urls:
                tasks.append(fetch_with_semaphore(session, url, headers))
            return await asyncio.gather(*tasks)

    @retry(stop=stop_after_attempt(1), wait=wait_fixed(1), retry_error_callback=_retry_error_callback)
    async def _fetch(self, session: aiohttp.ClientSession, url: str, headers: Optional[Dict[str, str]] = None) -> Any:
        self.log.debug(f"Fetching {url}")
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            return await response.json()


class AsyncCrawler(ConfigurableResource):
    """
    A resource for making asynchronous HTTP requests using aiohttp.
    """

    def create_resource(self, context) -> AsyncCrawlerClient:
        return AsyncCrawlerClient(log=context.log)
