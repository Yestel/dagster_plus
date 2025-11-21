from pathlib import Path

import dagster as dg
from dagster_plus.resources.snowflake_db import SnowflakeDB

 
@dg.definitions
def defs():
    return dg.Definitions.merge(
        dg.load_from_defs_folder(project_root=Path(__file__).parent.parent),
        dg.Definitions(
            resources={
                "snowflake": SnowflakeDB.configure_at_launch(),
            }
        )
    )
