from pathlib import Path

import dagster as dg


from crawler.defs.resources.postgres_db import PostgresDB
 
@dg.definitions
def defs():
    return dg.Definitions.merge(
        dg.load_from_defs_folder(project_root=Path(__file__).parent.parent),
        dg.Definitions(
            resources={
                "postgres": PostgresDB.configure_at_launch(),
            }
        )
    )
