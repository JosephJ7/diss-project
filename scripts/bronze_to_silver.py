# scripts/bronze_to_silver.py
from moby_pipeline.config import *
from pyspark.sql import SparkSession, functions as F
from sedona.register import SedonaRegistrator

RAW_BOUNDARY = "s3://diss-raw-boundary/electoral_divisions.geojson"
DELTA_BUCKET = DELTA_BUCKET

spark = (SparkSession.builder
         .appName("BronzeToSilver")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.sedona.sql.SedonaSqlExtensions")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

SedonaRegistrator.registerAll(spark)

bronze = spark.read.format("delta").load(f"s3://{DELTA_BUCKET}/bronze/moby")
ed     = spark.read.format("geojson").load(RAW_BOUNDARY)

bronze.createOrReplaceTempView("b")
ed.createOrReplaceTempView("ed")

silver = spark.sql("""
SELECT
  b.properties.bike_id            AS bike_id,
  to_timestamp(b.properties.timestamp) AS ts,
  b.properties.battery            AS battery,
  b.properties.ebike_state_id     AS state,
  b.geometry.coordinates[0]       AS lon,
  b.geometry.coordinates[1]       AS lat,
  first(e.name)                   AS neighbourhood
FROM b
LEFT JOIN ed
ON ST_Contains(ed.geometry, ST_Point(b.geometry.coordinates[0], b.geometry.coordinates[1]))
GROUP BY bike_id, ts, battery, state, lon, lat
""")

(silver.write.format("delta")
       .mode("append")
       .partitionBy("neighbourhood")
       .save(f"s3://{DELTA_BUCKET}/silver/moby"))
