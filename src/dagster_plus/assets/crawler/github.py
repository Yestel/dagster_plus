import json
import pandas as pd
from dagster import asset, Output, AssetExecutionContext
from dagster_plus.resources.async_crawler import AsyncCrawler

@asset(group_name="github", compute_kind="GitHub API", required_resource_keys={"async_crawler", "postgres"})
async def github_events(context: AssetExecutionContext):
    """
    Fetches the latest public events from GitHub.
    """
    async_crawler = context.resources.async_crawler
    postgres = context.resources.postgres

    per_page = 50
    urls = []
    for page in range(1, 2):
        url = f"https://api.github.com/events?page={page}&per_page={per_page}"
        urls.append(url)
    results = await async_crawler.fetch_all(urls)
    events = [r for r in results if r is not None]
    events = [event for sublist in events for event in sublist]
    
    context.log.info(f"Fetched {len(events)} events")
    df = pd.DataFrame(events)
    df = df.drop_duplicates(subset=['id'], keep='first')

    upsert_keys = ['id']
    postgres.write_dataframe(df, "github_events", upsert_keys)
    return Output(events, metadata={"count": len(events)})

@asset(group_name="github", compute_kind="GitHub API")
def github_repos(context: AssetExecutionContext, github_events):
    """
    Extracts unique repositories from the events.
    """
    repos = {}
    for event in github_events:
        repo = event.get("repo", {})
        if repo:
            repos[repo["id"]] = repo
    
    repo_list = list(repos.values())
    context.log.info(f"Extracted {len(repo_list)} unique repos")
    return Output(repo_list, metadata={"count": len(repo_list)})

@asset(group_name="github", compute_kind="GitHub API", required_resource_keys={"async_crawler", "postgres"})
async def github_repo_details(context: AssetExecutionContext, github_repos):
    """
    Fetches details for each repository.
    """
    async_crawler = context.resources.async_crawler
    postgres = context.resources.postgres

    urls = []
    for repo in github_repos:
        url = repo.get("url")
        if url:
            urls.append(url)
            
    results = await async_crawler.fetch_all(urls)
    details = [r for r in results if r is not None]
    
    context.log.info(f"Fetched details for {len(details)} repos")
    df = pd.DataFrame(details)
    upsert_keys = ['id']
    postgres.write_dataframe(df, "github_repos", upsert_keys)
    return Output(details, metadata={"count": len(details)})
