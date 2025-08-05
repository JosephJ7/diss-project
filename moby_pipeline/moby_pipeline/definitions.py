from dagster import Definitions, load_assets_from_modules

from moby_pipeline.assets import *
from moby_pipeline.resources import s3
defs = Definitions(
    assets=[moby_snapshot_to_s3,raw_to_bronze_step, bronze_to_silver_step],
    resources={"s3": s3},
)

