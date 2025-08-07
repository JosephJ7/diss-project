import streamlit as st, pymongo, pandas as pd, plotly.express as px, h3
# h3ronpy.geojson
from moby_pipeline.config import MONGO_URI

cli = pymongo.MongoClient(MONGO_URI)
db  = cli["moby"]

st.title("🛴 Moby Dublin – Telemetry Dashboard")

# -------- Heat-map tab --------
tab1, tab2, tab3 = st.tabs(["Demand map", "Battery decay", "Idle alerts"])

# with tab1:
#     st.subheader("Hex demand (last load)")
#     demand = pd.DataFrame(db.h3_demand.find({}, {"_id":0}))
#     # convert H3 → polygon GeoJSON
#     geo = {"type":"FeatureCollection","features":[]}
#     for _,row in demand.iterrows():
#         geo["features"].append({
#             "type":"Feature",
#             "properties":{"count":row["count"]},
#             "geometry":h3ronpy.geojson.polygon_from_h3(row["h3"])
#         })
#     st.map(geo, zoom=12)

with tab2:
    st.subheader("Average battery decay % per hour")
    decay = pd.DataFrame(db.battery_decay.find({}, {"_id":0}))
    fig = px.line(decay, x="hour", y="avg_decay_pct_h")
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.subheader("Idle / low-range bikes")
    idle = pd.DataFrame(db.idle_alerts.find({}, {"_id":0})).sort_values("ts", ascending=False)
    st.dataframe(idle)
