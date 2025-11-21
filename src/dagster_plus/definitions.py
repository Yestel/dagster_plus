import dagster as dg
from dagster_plus.resources.snowflake_db import SnowflakeDB
from dagster_plus.assets.crawler import hackernews, todos
from dagster_plus import schedules


@dg.definitions
def defs():
    return dg.Definitions.merge(
        dg.Definitions(
            assets=dg.load_assets_from_modules([hackernews, todos]),
        ),
        schedules.defs(),
        dg.Definitions(
            resources={
                "snowflake": SnowflakeDB.configure_at_launch(),
            }
        )
    )
