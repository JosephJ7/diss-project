import streamlit as st, pymongo, pandas as pd, plotly.express as px, h3,os
import config
import pydeck as pdk

cli = pymongo.MongoClient(config.MONGO_URI)
db  = cli["moby"]

st.set_page_config(page_title="Moby Dublin – Telemetry", layout="wide")
st.title("🛴 Moby Dublin – Telemetry Dashboard")


tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Demand map", "Battery decay", "Idle alerts",
    "Station crowding", "Ghost bikes", "Station churn"
])

# -------- Heat-map tab --------
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

# -------- Station crowding --------
with tab4:
    st.subheader("Station crowding (bikes per station)")
    crowd = pd.DataFrame(db.station_crowding.find({}, {"_id":0}))
    if crowd.empty:
        st.info("No station-crowding data found.")
    else:
        # Prefer pct_full if present, otherwise bikes
        if "pct_full" in crowd.columns:
            crowd = crowd.sort_values("pct_full", ascending=False).head(15)
            fig = px.bar(crowd, x="station_id", y="pct_full",
                            labels={"pct_full": "% full", "station_id": "Station"},
                            title="Top stations by % full (latest snapshot)")
        else:
            crowd = crowd.sort_values("bikes", ascending=False).head(15)
            fig = px.bar(crowd, x="station_id", y="bikes",
                            labels={"bikes": "Bikes", "station_id": "Station"},
                            title="Top stations by bike count (latest snapshot)")
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Raw table"):
            st.dataframe(crowd, use_container_width=True)

# -------- Ghost bikes --------
with tab5:
    st.subheader("Ghost bikes (no update > 6h)")
    ghosts = pd.DataFrame(db.ghost_bikes.find({}, {"_id":0}))
    if ghosts.empty:
        st.info("No ghost-bikes detected.")
    else:
        for col in ("ts",):
            if col in ghosts.columns:
                ghosts[col] = pd.to_datetime(ghosts[col])
        ghosts = ghosts.sort_values("ts", ascending=False)
        st.dataframe(ghosts, use_container_width=True)

# -------- Station churn --------
with tab6:
    st.subheader("Station churn (last 24h)")
    churn = pd.DataFrame(db.station_churn.find({}, {"_id":0}))
    if churn.empty:
        st.info("No station-churn data found.")
    else:
        # Calculate % if needed
        if "churn_rate" in churn.columns:
            churn["churn_pct"] = churn["churn_rate"] * 100
        elif {"distinct_bikes_24h", "current_bikes"}.issubset(churn.columns):
            churn["churn_pct"] = churn["distinct_bikes_24h"] / churn["current_bikes"] * 100
        else:
            churn["churn_pct"] = None

        top = churn.sort_values("churn_pct", ascending=False).head(15)
        fig = px.bar(top, x="station_id", y="churn_pct",
                        labels={"churn_pct": "Churn % (24h)", "station_id": "Station"},
                        title="Top stations by churn (last 24h)")
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Raw table"):
            st.dataframe(churn, use_container_width=True)