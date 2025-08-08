from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import pymongo, json, config, h3

spark = (SparkSession.builder.appName("nightly").config("spark.hadoop.fs.s3a.fast.upload", "true").config("spark.mongodb.write.connection.uri", "mongodb+srv://sparkuser:sparkpassword@advp.xqmcaw4.mongodb.net/").getOrCreate())
raw = spark.read.json("s3a://diss-raw-anj/moby/raw/2025/08/08/06/26.json.gz")

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
@F.udf("string")
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
    