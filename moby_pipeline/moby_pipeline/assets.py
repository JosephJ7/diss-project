import gzip, io, datetime, subprocess, json, os, requests, pymongo, config
from dagster import op, job, get_dagster_logger

URL = "https://data.smartdublin.ie/dublinbikes-api/bikes/mobymoby_dublin/current/bikes.geojson"

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
    return f"s3a://{config.S3_BUCKET}/{key}"

# ---------- op 2: Spark job (local) ----------
@op
def spark_process(context, s3_path:str):
    code= """from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import pymongo, json, config, h3

spark = (SparkSession.builder.appName("nightly").config("spark.hadoop.fs.s3a.fast.upload", "true").config("spark.mongodb.write.connection.uri", "{mongo}").getOrCreate())
raw = spark.read.json("{s3}")

features = (raw
            .selectExpr("explode(features) AS f")               
            .select("f.geometry.coordinates",                  
                    "f.properties.*"))                         

silver = (features.select(
            F.col("bike_id"),
            F.to_timestamp("last_updated_dt").alias("ts"),
            F.col("current_range_meters").alias("range_m"),
            F.col("coordinates")[0].alias("lon"),          
            F.col("coordinates")[1].alias("lat")
        ))
print("▶︎ silver            :", silver.count())
            
# ---- Analysis 1 : battery-decay KPI ----
w = Window.partitionBy("bike_id").orderBy("ts")
events = (silver.withColumn("prev_range",F.lag("range_m").over(w))
        .withColumn("prev_ts",F.lag("ts").over(w))
        .where("prev_range IS NOT NULL")
        .withColumn("d_m",F.col("range_m")-F.col("prev_range"))
        .withColumn("d_h",(F.col("ts").cast("long")-F.col("prev_ts").cast("long"))/3600)
        .where("d_m < 0 AND d_h > 0")
        .withColumn("decay_pct_h",-F.col("d_m")/config.MAX_RANGE_M*100))
print("▶︎ after the two lags:", events.count())
decay = (events.groupBy(F.window("ts", "1 hour").alias("hour"))
        .agg(F.avg("decay_pct_h").alias("avg_decay_pct_h"))
        .selectExpr("hour.start AS hour", "avg_decay_pct_h"))
        
# ---- Analysis 2 : demand heat-map (H3) ----
@F.udf("string")\n\
def to_h3(lat,lon):  
    return h3.latlng_to_cell(lat,lon,9)
    
demand = (silver.withColumn("h3",to_h3("lat","lon")).groupBy("h3").count())

# ---- Analysis 3 : idle / low-range alerts ----
idle = silver.filter((F.col("range_m")<config.MAX_RANGE_M*0.05))

# ---- write everything to Mongo ----
def write(df, collection:str, mode="append"):
    (df.write
        .format("mongodb")
        .option("database", "moby")
        .option("collection", collection)
        .mode(mode)
        .save())

write(silver , "telemetry")       
write(decay  , "decay_summary", "overwrite")
write(demand , "h3_demand"   , "overwrite")
write(idle   , "idle_alerts" , "overwrite")

spark.stop()
    """.format(s3=s3_path, mongo=config.MONGO_URI)
    
    job_file = "spark_job.py"
    open(job_file,"w",encoding="utf-8").write(code)
    AWS_PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.650,"
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
        )
    context.log.info("Starting the Process")
    result = subprocess.run(["spark-submit.cmd",
                    "--master", "local[*]", 
                    "--packages",AWS_PACKAGES,
                    "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.profile.ProfileCredentialsProvider" ,
                    job_file],  
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT)
    context.log.info(result.stdout)

    if result.returncode:
        raise RuntimeError("Spark failed - see log above")


@job()
def full_pipeline():
    spark_process(fetch_to_s3())

