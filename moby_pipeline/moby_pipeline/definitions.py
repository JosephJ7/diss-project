from dagster import Definitions,ScheduleDefinition

from moby_pipeline.assets import full_pipeline 
from moby_pipeline.resources import s3
defs = Definitions(
    jobs=[full_pipeline],
    resources={"s3": s3},
    schedules=[
        ScheduleDefinition(job=full_pipeline, cron_schedule="*/5 * * * *"),
    ],
)

