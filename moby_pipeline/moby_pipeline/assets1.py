import gzip, io, datetime, subprocess, json, os, requests, pymongo, config
from dagster import op, job, get_dagster_logger

URL = ("https://data.smartdublin.ie/dublinbikes-api/bikes/mobymoby_dublin/current/bikes.geojson")

# ---------- op 1: download & push to S3 ----------
@op(required_resource_keys={"s3"})
def fetch_to_s3(context):
    r = requests.get(URL, timeout=30); r.raise_for_status()
    ts  = datetime.datetime.utcnow().strftime("%Y/%m/%d/%H/%M")
    key = f"moby/raw/{ts}.json.gz"
    buf = io.BytesIO(gzip.compress(r.content))
    context.resources.s3.get_client().upload_fileobj(buf,
        Bucket=config.S3_BUCKET, Key=key)
    context.log.info(f"↑  s3://{config.S3_BUCKET}/{key}")
    return f"s3://{config.S3_BUCKET}/{key}"

# ---------- op 2: Spark job (local) ----------
@op
def spark_process(context, s3_path:str):
    code= """from pyspark.sql import SparkSession, functions as F
import pymongo, json, config, h3

spark = (SparkSession.builder.appName("nightly").getOrCreate())
df = spark.read.json("{s3}")

silver = (df.select(
            F.col("properties.bike_id").alias("bike_id"),
            F.to_timestamp("properties.last_updated_dt").alias("ts"),
            F.col("properties.current_range_meters").alias("range_m"),
            F.col("geometry.coordinates")[0].alias("lon"),
            F.col("geometry.coordinates")[1].alias("lat")))
            
# ---- Analysis 1 : battery-decay KPI ----
w = (spark.window.Window.partitionBy("bike_id").orderBy("ts"))
events = (silver.withColumn("prev",F.lag("range_m").over(w))
        .withColumn("prev_ts",F.lag("ts").over(w))
        .withColumn("d_m",F.col("range_m")-F.col("prev"))
        .withColumn("d_h",(F.col("ts").cast("long")-F.col("prev_ts").cast("long"))/3600)
        .where("d_m < 0 AND d_h > 0")
        .withColumn("decay_pct_h",-F.col("d_m")/config.MAX_RANGE_M*100))
decay = (events.groupBy(F.hour("ts").alias("hour"))
        .agg(F.avg("decay_pct_h").alias("avg_decay_pct_h")))
        
# ---- Analysis 2 : demand heat-map (H3) ----
@F.udf("string")\n\
def to_h3(lat,lon): import h3; return h3.geo_to_h3(lat,lon,9)
demand = (silver.withColumn("h3",to_h3("lat","lon")).groupBy("h3").count())

# ---- Analysis 3 : idle / low-range alerts ----
idle = silver.filter((F.col("range_m")<config.MAX_RANGE_M*0.05))

# ---- write everything to Mongo ----
cli = pymongo.MongoClient("{mongo}");
cli.moby.telemetry.insert_many(json.loads(silver.toJSON().collect().__str__()));
cli.moby.decay_summary.delete_many({{}}); 
cli.moby.decay_summary.insert_many(json.loads(decay.toJSON().collect().__str__()));
cli.moby.h3_demand.delete_many({{}});      
cli.moby.h3_demand.insert_many(json.loads(demand.toJSON().collect().__str__()))
cli.moby.idle_alerts.delete_many({{}});    
cli.moby.idle_alerts.insert_many(json.loads(idle.toJSON().collect().__str__()))""".format(s3=s3_path, mongo=config.MONGO_URI)
    
    job_file = "spark_job.py"
    open(job_file,"w").write(code)
    context.log.info("Starting the Process")
    result = subprocess.run(["spark-submit.cmd",
                    "--master", "local[*]", 
                    "--packages","io.delta:delta-spark_2.12:3.2.1",
                    job_file],  
                    # check=True,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT)
    context.log.info(result.stdout)

    if result.returncode:
        raise RuntimeError("Spark failed - see log above")

# ---------- single job ----------
@job()
def full_pipeline():
    spark_process(fetch_to_s3())

