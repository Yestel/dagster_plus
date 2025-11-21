import dagster as dg
from dagster import AssetSelection, ScheduleDefinition


def schedule_asset_group(group_name: str, cron_schedule: str):
    return ScheduleDefinition(
        job=dg.define_asset_job(
            name=f"{group_name}_job",
            selection=dg.AssetSelection.groups(group_name),
        ),
        cron_schedule=cron_schedule,
    )


@dg.definitions
def defs():
    return dg.Definitions(
        schedules=[
            schedule_asset_group("todos", "0 0 * * *"),
            schedule_asset_group("hackernews", "0 1 * * *"),
        ]
    )
