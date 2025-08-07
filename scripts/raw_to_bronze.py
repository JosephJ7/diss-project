# scripts/raw_to_bronze.py
import config 
from pyspark.sql import SparkSession

RAW_BUCKET    = config.S3_BUCKET
DELTA_BUCKET = config.DELTA_BUCKET

spark = (SparkSession.builder
         .appName("RawToBronze")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .getOrCreate())

df = spark.read.json(f"s3://{RAW_BUCKET}/moby/raw/**/*.json.gz")

df.write.format("delta").mode("append").save(f"s3://{DELTA_BUCKET}/bronze/moby")
