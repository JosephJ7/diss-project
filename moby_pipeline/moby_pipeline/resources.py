import boto3, config
from dagster import ConfigurableResource, Definitions

class S3Resource(ConfigurableResource):
    def get_client(self):
        return boto3.client("s3")

s3 = S3Resource()


