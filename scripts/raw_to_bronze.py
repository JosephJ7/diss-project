# scripts/raw_to_bronze.py
from moby_pipeline.config import *
from pyspark.sql import SparkSession

RAW_BUCKET   = S3_BUCKET        # diss-raw-anj
DELTA_BUCKET = DELTA_BUCKET    # diss-delta-anj

spark = (SparkSession.builder
         .appName("RawToBronze")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

df = spark.read.json(f"s3://{RAW_BUCKET}/moby/raw/")

(df.write.format("delta")
   .mode("append")
   .save(f"s3://{DELTA_BUCKET}/bronze/moby"))
