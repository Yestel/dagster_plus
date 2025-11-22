import dagster as dg
from dagster_plus import assets, schedules
from dagster_plus.resources.snowflake_db import SnowflakeDB
from dagster_plus.resources.postgres_db import PostgresDB
from dagster_plus.resources.async_crawler import AsyncCrawler


@dg.definitions
def defs():
    return dg.Definitions.merge(
        dg.Definitions(
            assets=dg.load_assets_from_package_module(assets),
        ),
        schedules.defs(),
        dg.Definitions(
            resources={
                "snowflake": SnowflakeDB.configure_at_launch(),
                "postgres": PostgresDB.configure_at_launch(),
                "async_crawler": AsyncCrawler.configure_at_launch(),
            }
        )
    )
