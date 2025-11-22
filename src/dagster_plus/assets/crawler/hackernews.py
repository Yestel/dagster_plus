from typing import List
import json
import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset


@asset(group_name="hackernews", compute_kind="HackerNews API", required_resource_keys={"async_crawler"})
async def hackernews_topstory_ids(context: AssetExecutionContext) -> List[int]:
    """Get up to 500 top stories from the HackerNews topstories endpoint.

    API Docs: https://github.com/HackerNews/API#new-top-and-best-stories
    """
    async_crawler = context.resources.async_crawler
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_500_newstories = await async_crawler.get(newstories_url)
    context.add_output_metadata(
        {
            "num_records": len(top_500_newstories),
            "preview": MetadataValue.md(json.dumps(top_500_newstories[:5])),
        }
    )
    return top_500_newstories


@asset(group_name="hackernews", compute_kind="HackerNews API", required_resource_keys={"async_crawler", "postgres"})
async def hackernews_topstories(
    context: AssetExecutionContext, 
    hackernews_topstory_ids: List[int],
) -> pd.DataFrame:
    """Get items based on story ids from the HackerNews items endpoint. It may take 1-2 minutes to fetch all 500 items.

    API Docs: https://github.com/HackerNews/API#items
    """
    async_crawler = context.resources.async_crawler
    postgres = context.resources.postgres

    urls = [f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json" for item_id in hackernews_topstory_ids[:50]]
    results = await async_crawler.fetch_all(urls)
    
    # Filter out None results if any failed
    results = [r for r in results if r is not None]
    
    context.log.info(f"Got {len(results)} items.")

    df = pd.DataFrame(results)

    # Dagster supports attaching arbitrary metadata to asset materializations. This metadata will be
    # shown in the run logs and also be displayed on the "Activity" tab of the "Asset Details" page in the UI.
    # This metadata would be useful for monitoring and maintaining the asset as you iterate.
    # Read more about in asset metadata in https://docs.dagster.io/concepts/assets/software-defined-assets#recording-materialization-metadata
    context.add_output_metadata(
        {
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )
    upsert_keys = ['id']
    postgres.write_dataframe(df, "stories", upsert_keys)
    return df
