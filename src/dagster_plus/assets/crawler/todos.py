import json
import pandas as pd
import requests
from dagster import AssetExecutionContext, MetadataValue, asset


@asset(group_name="todos", compute_kind="Todos API", required_resource_keys={"postgres"})
def todos_list(context: AssetExecutionContext) -> pd.DataFrame:
    """Get up to 50 todos from the Todos API."""
    todos_url = 'https://jsonplaceholder.typicode.com/todos'
    todos = requests.get(todos_url).json()
    todos = todos[:50]
    context.add_output_metadata(
        {
            "num_records": len(todos),
            "preview": MetadataValue.md(json.dumps(todos[:5])),
        }
    )
    df = pd.DataFrame(todos)
    upsert_keys = ['id']
    context.resources.postgres.write_dataframe(df, "todos", upsert_keys)
    return df
