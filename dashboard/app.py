"""
dashboard/app.py
─────────────────────────────────────────────────────────────────
Vélib' Real-Time Analytics Dashboard — Streamlit

5 visualisations:
  1. KPI cards        : total bikes, docks, stations, e-bikes
  2. Map              : live station map (colour = fill rate)
  3. Bar chart        : bikes available by arrondissement
  4. Heatmap table    : empty vs full stations by arrondissement
  5. Event stream     : latest Kafka events (rolling 50 rows)

Auto-refreshes every 60 seconds.
"""

import os
import time
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timezone, timedelta

# ─── Config ────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   "velib",
    "user":     "airflow",
    "password": "airflow",
}

st.set_page_config(
    page_title="Vélib' Live Analytics",
    page_icon="🚲",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Custom CSS
st.markdown("""
<style>
    .block-container { padding-top: 1rem; }
    .metric-card {
        background: #1e2130;
        border-radius: 10px;
        padding: 1rem 1.5rem;
        border-left: 4px solid #00c4b4;
    }
    h1 { color: #00c4b4; }
    .stMetric label { font-size: 0.8rem; color: #aaa; }
</style>
""", unsafe_allow_html=True)


# ─── DB helpers ────────────────────────────────────────────────────



def query(sql: str, params=None) -> pd.DataFrame:
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql(sql, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        st.warning(f"DB query failed: {e}")
        return pd.DataFrame()


# ─── Data loaders ──────────────────────────────────────────────────

def load_kpis() -> dict:
    df = query("""
        SELECT
            SUM(num_bikes_available)  AS total_bikes,
            SUM(num_docks_available)  AS total_docks,
            COUNT(*)                  AS total_stations,
            SUM(num_ebikes)           AS total_ebikes,
            COUNT(*) FILTER (WHERE num_bikes_available = 0) AS empty_stations,
            ROUND(AVG(
                100.0 * num_bikes_available / NULLIF(capacity, 0)
            )::numeric, 1)            AS avg_fill_pct
        FROM v_current_status
        WHERE is_renting = TRUE
    """)
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


def load_station_map() -> pd.DataFrame:
    return query("""
        SELECT
            name, lat, lon, arrondissement,
            num_bikes_available, num_docks_available,
            capacity, fill_rate_pct, is_renting
        FROM v_current_status
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """)


def load_by_arrondissement() -> pd.DataFrame:
    return query("""
        SELECT
            arrondissement,
            snapshot_time,
            total_bikes,
            total_docks,
            total_stations,
            avg_fill_rate,
            empty_stations,
            full_stations
        FROM agg_availability_by_arrondissement
        WHERE snapshot_time = (
            SELECT MAX(snapshot_time)
            FROM agg_availability_by_arrondissement
        )
        ORDER BY arrondissement
    """)


def load_stream_events() -> pd.DataFrame:
    return query("""
        SELECT
            consumed_at,
            station_code,
            event_type,
            num_bikes_available,
            num_docks_available
        FROM stream_events
        ORDER BY consumed_at DESC
        LIMIT 50
    """)


def load_hourly_trend(station_code: str) -> pd.DataFrame:
    return query("""
        SELECT hour_bucket, avg_bikes, avg_docks, snapshots_count
        FROM agg_hourly_trend
        WHERE station_code = %s
          AND hour_bucket > NOW() - INTERVAL '24 hours'
        ORDER BY hour_bucket
    """, params=(station_code,))


# ─── Layout ────────────────────────────────────────────────────────

@st.fragment(run_every=5)
def stream_section():
    st.subheader("Live Kafka Event Stream (last 50 events)")
    events_df = load_stream_events()
    if not events_df.empty:
        def colour_event(val):
            colours = {
                "empty_station": "background-color: #d62728; color: white",
                "low_bikes":     "background-color: #ff7f0e; color: white",
                "full_station":  "background-color: #1f77b4; color: white",
                "snapshot":      "",
            }
            return colours.get(val, "")

        st.dataframe(
            events_df.style.map(colour_event, subset=["event_type"]),
            use_container_width=True,
            height=300,
        )
    else:
        st.info("No streaming events yet, Kafka consumer starting up…")


def render_dashboard():
    st.title("Vélib' Real-Time Analytics Pipeline")
    paris_tz = timezone(timedelta(hours=2))
    st.caption(f"Last refresh: {datetime.now(paris_tz).strftime('%H:%M:%S')} (Paris) · Auto-refresh every 60s")
    # ── VIZ 1: KPI Cards ──────────────────────────────────────────
    st.subheader("Live KPIs")
    kpis = load_kpis()
    if kpis:
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Bikes available",  int(kpis.get("total_bikes") or 0))
        c2.metric("E-bikes",          int(kpis.get("total_ebikes") or 0))
        c3.metric("Docks free",       int(kpis.get("total_docks") or 0))
        c4.metric("Stations active",  int(kpis.get("total_stations") or 0))
        c5.metric("Empty stations",   int(kpis.get("empty_stations") or 0))
        c6.metric("Avg fill rate",    f"{float(kpis.get('avg_fill_pct') or 0):.1f}%")
    else:
        st.info("No data yet — waiting for first ETL run …")

    st.divider()

    # ── VIZ 2: Live Station Map ────────────────────────────────────
    st.subheader("Live Station Map")
    map_df = load_station_map()
    if not map_df.empty:
        fig_map = px.scatter_mapbox(
            map_df,
            lat="lat", lon="lon",
            color="fill_rate_pct",
            size="capacity",
            size_max=15,
            color_continuous_scale=["#d62728", "#ff7f0e", "#2ca02c"],
            range_color=[0, 100],
            hover_name="name",
            hover_data={
                "arrondissement": True,
                "num_bikes_available": True,
                "num_docks_available": True,
                "fill_rate_pct": True,
                "lat": False, "lon": False,
            },
            mapbox_style="carto-darkmatter",
            zoom=11,
            center={"lat": 48.857, "lon": 2.347},
            height=500,
            labels={"fill_rate_pct": "Fill rate %"},
        )
        fig_map.update_layout(
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            coloraxis_colorbar=dict(title="Fill %"),
        )
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.info("Map data loading …")

    st.divider()

    col_left, col_right = st.columns(2)

    # ── VIZ 3: Bikes by Arrondissement ────────────────────────────
    with col_left:
        st.subheader("Availability by Arrondissement")
        arr_df = load_by_arrondissement()
        if not arr_df.empty:
            fig_bar = px.bar(
                arr_df,
                x="arrondissement",
                y=["total_bikes", "empty_stations"],
                barmode="group",
                color_discrete_map={
                    "total_bikes":    "#00c4b4",
                    "empty_stations": "#d62728",
                },
                labels={
                    "arrondissement": "Arrondissement",
                    "value": "Count",
                    "variable": "",
                },
                height=350,
            )
            fig_bar.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                legend=dict(orientation="h", y=1.1),
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("Aggregation data loading …")

    # ── VIZ 4: Fill Rate Heatmap ───────────────────────────────────
    with col_right:
        st.subheader("Fill Rate Heatmap")
        arr_df2 = load_by_arrondissement()
        if not arr_df2.empty:
            fig_heat = go.Figure(go.Bar(
                x=arr_df2["arrondissement"].astype(str),
                y=arr_df2["avg_fill_rate"],
                marker=dict(
                    color=arr_df2["avg_fill_rate"],
                    colorscale="RdYlGn",
                    cmin=0,
                    cmax=100,
                    showscale=True,
                    colorbar=dict(title="Fill %"),
                ),
                text=arr_df2["avg_fill_rate"].apply(lambda x: f"{x:.1f}%"),
                textposition="outside",
            ))
            fig_heat.update_layout(
                xaxis_title="Arrondissement",
                yaxis_title="Avg fill rate (%)",
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                height=350,
            )
            st.plotly_chart(fig_heat, use_container_width=True)
        else:
            st.info("Heatmap data loading …")

    st.divider()

    # ── VIZ 5: Live Event Stream ───────────────────────────────────
    stream_section()


# ─── Main loop with auto-refresh ───────────────────────────────────

render_dashboard()
time.sleep(60)
st.rerun()
