from pyspark.sql import SparkSession, functions as F
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
@F.udf("string")
def to_h3(lat,lon): import h3; return h3.geo_to_h3(lat,lon,9)
demand = (silver.withColumn("h3",to_h3("lat","lon")).groupBy("h3").count())

# ---- Analysis 3 : idle / low-range alerts ----
idle = silver.filter((F.col("range_m")<config.MAX_RANGE_M*0.05))

# ---- write everything to Mongo ----
cli = pymongo.MongoClient("{mongo}")
cli.moby.telemetry.insert_many(json.loads(silver.toJSON().collect().__str__()))
cli.moby.decay_summary.delete_many({{}}); 
cli.moby.decay_summary.insert_many(json.loads(decay.toJSON().collect().__str__()))
cli.moby.h3_demand.delete_many({{}});      
cli.moby.h3_demand.insert_many(json.loads(demand.toJSON().collect().__str__()))
cli.moby.idle_alerts.delete_many({{}});    
cli.moby.idle_alerts.insert_many(json.loads(idle.toJSON().collect().__str__()))