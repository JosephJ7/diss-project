from dagster import ScheduleDefinition
from moby_pipeline.assets.ingest_moby import moby_snapshot_to_s3
from moby_pipeline.resources import defs

five_min = ScheduleDefinition(
    job=moby_snapshot_to_s3.define_job("snapshot_job"),
    cron_schedule="*/5 * * * *",
)

defs.get_schedules().append(five_min)
