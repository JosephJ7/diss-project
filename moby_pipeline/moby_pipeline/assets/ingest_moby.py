import requests, gzip, io, datetime, config 
from dagster import asset

URL = "https://data.smartdublin.ie/dublinbikes-api/bikes/mobymoby_dublin/current/bikes.geojson"

@asset(required_resource_keys={"s3"})
def moby_snapshot_to_s3(context):
    """Fetches the current bikes.geojson and gzips it to S3/raw."""
    r = requests.get(URL, timeout=30)
    r.raise_for_status()

    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d-%H-%M")
    key = f"moby/raw/{ts}.json.gz"

    buf = io.BytesIO(gzip.compress(r.content))
    s3 = context.resources.s3.get_client()
    s3.upload_fileobj(
        Fileobj=buf,
        Bucket=config.S3_BUCKET,  
        Key=key
        )

    context.log.info(f"Uploaded snapshot ➜ s3://{config.S3_BUCKET}/{key}")
