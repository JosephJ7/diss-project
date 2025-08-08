import streamlit as st, pymongo, pandas as pd, plotly.express as px, h3,os
import config
import pydeck as pdk

cli = pymongo.MongoClient(config.MONGO_URI)
db  = cli["moby"]

st.set_page_config(page_title="Moby Dublin – Telemetry", layout="wide")
st.title("🛴 Moby Dublin – Telemetry Dashboard")

# -------- Heat-map tab --------
tab1, tab2, tab3 = st.tabs(["Demand map", "Battery decay", "Idle alerts"])
    
with tab1:
    st.subheader("Hex demand (last load)")
    demand = pd.DataFrame(db.h3_demand.find({}, {"_id":0}))
    
    if demand.empty:
        st.info("No demand data found.")
    else:
        # make sure h3 index is a string
        demand["h3"] = demand["h3"].astype(str)

        # Map initial view (centre on Dublin’s city centre)
        view_state = pdk.ViewState(latitude=53.3498, longitude=-6.2603, zoom=11, pitch=45)

        layer = pdk.Layer(
            "H3HexagonLayer",
            data=demand,
            get_hexagon="h3",
            get_fill_color="[255, count * 4, 100]",
            get_elevation="count",
            elevation_scale=30,
            pickable=True,
            extruded=True,
        )
        
        token = os.getenv("MAPBOX_TOKEN", "") 
        
        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip={"text": "Trips: {count}"},
            map_style="mapbox://styles/mapbox/light-v9",
            api_keys={"mapbox": token} if token else None,
        )

        st.pydeck_chart(deck, use_container_width=True)

with tab2:
    st.subheader("Average battery decay % per hour")
    decay = pd.DataFrame(db.battery_decay.find({}, {"_id":0}))
    if decay.empty:
        st.info("No battery-decay data found.")
    else:
        fig = px.line(decay, x="hour", y="avg_decay_pct_h",
                        title="Average battery decay per hour",
                        markers=True)
        st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.subheader("Idle / low-range bikes")
    idle = pd.DataFrame(db.idle_alerts.find({}, {"_id":0})).sort_values("ts", ascending=False)
    st.dataframe(idle,use_container_width=True)
