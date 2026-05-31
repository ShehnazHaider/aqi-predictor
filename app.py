import os
from dotenv import load_dotenv
import streamlit as st
import hopsworks
import pandas as pd
import numpy as np
import joblib
from datetime import datetime, timedelta
import plotly.graph_objects as go

# ==================== SETUP ====================
load_dotenv()

st.set_page_config(
    page_title="AQI Dashboard - Skardu",
    page_icon="🌫️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== MODERN UI STYLING ====================
st.markdown("""
<style>

/* ========== GLOBAL ========== */
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

.stApp {
    background: linear-gradient(135deg, #eef2f7 0%, #f7f9fc 40%, #eef3fb 100%);
}

/* ========== HEADER ========== */
.main-header {
    font-size: 2.6rem;
    font-weight: 800;
    background: linear-gradient(90deg, #2563eb, #06b6d4);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}

/* ========== GLASS CARD SYSTEM ========== */
.card {
    background: rgba(255, 255, 255, 0.75);
    border-radius: 18px;
    padding: 18px 20px;
    box-shadow: 0 10px 30px rgba(0,0,0,0.06);
    border: 1px solid rgba(255,255,255,0.4);
    backdrop-filter: blur(12px);
}

/* ========== METRICS ========== */
[data-testid="stMetric"] {
    background: rgba(255,255,255,0.8);
    border-radius: 16px;
    padding: 14px;
    box-shadow: 0 6px 18px rgba(0,0,0,0.06);
}

/* metric labels */
[data-testid="stMetricLabel"] {
    font-size: 0.85rem;
    color: #6b7280;
}

/* metric value */
[data-testid="stMetricValue"] {
    font-size: 1.6rem;
    font-weight: 700;
}

/* ========== SIDEBAR ========== */
section[data-testid="stSidebar"] {
    background: rgba(255,255,255,0.6);
    backdrop-filter: blur(14px);
}

/* ========== BUTTONS ========== */
.stButton button {
    background: linear-gradient(90deg, #2563eb, #06b6d4);
    color: white;
    border-radius: 12px;
    border: none;
    font-weight: 600;
    padding: 0.5rem 1rem;
}

.stButton button:hover {
    transform: translateY(-2px);
    box-shadow: 0 10px 20px rgba(37,99,235,0.25);
}

/* ========== DIVIDERS ========== */
hr {
    border: none;
    height: 1px;
    background: rgba(0,0,0,0.08);
}

/* ========== AQI COLORS ========== */
.good { color: #00c853; font-weight: 700; }
.moderate { color: #facc15; font-weight: 700; }
.unhealthy { color: #ef4444; font-weight: 700; }
.hazardous { color: #7c2d12; font-weight: 700; }

</style>
""", unsafe_allow_html=True)

# ==================== HOPSWORKS ====================
@st.cache_resource(ttl=3600)
def connect_hopsworks():
    project = hopsworks.login(api_key_value=os.environ["HOPSWORKS_API_KEY"])
    fs = project.get_feature_store()
    mr = project.get_model_registry()
    return fs, mr

# ==================== DATA ====================
@st.cache_data(ttl=1800)
def load_data():
    fs, _ = connect_hopsworks()
    fg = fs.get_feature_group(name="processed_aqi_skardu_v2", version=1)
    df = fg.read().sort_values("timestamp")
    return df

@st.cache_data(ttl=1800)
def load_raw():
    fs, _ = connect_hopsworks()
    fg = fs.get_feature_group(name="aqi_predictionv2", version=1)
    df = fg.read().sort_values("timestamp")
    return df

# ==================== MODEL ====================
@st.cache_resource(ttl=3600)
def load_model():
    _, mr = connect_hopsworks()
    model = mr.get_best_model(name="aqi_xgb", metric="rmse_overall", direction="min")
    path = model.download()
    file = os.path.join(path, os.listdir(path)[0])
    return joblib.load(file), model

# ==================== AQI HELPERS ====================
def aqi_label(v):
    if v <= 50:
        return "Good", "#00c853"
    elif v <= 100:
        return "Moderate", "#facc15"
    elif v <= 150:
        return "Unhealthy", "#fb923c"
    else:
        return "Hazardous", "#ef4444"

# ==================== HEADER ====================
st.markdown('<div class="main-header">🌫️ AQI Dashboard — Skardu</div>', unsafe_allow_html=True)
st.caption("AI-powered air quality forecasting with real-time insights")

# ==================== LOAD ====================
with st.spinner("Loading system..."):
    df = load_data()
    df_raw = load_raw()
    model, meta = load_model()

# ==================== SIDEBAR (SaaS STYLE NAV) ====================
with st.sidebar:
    st.markdown("## 🌫️ AQI System")
    page = st.radio("Navigation", ["Dashboard", "Forecast", "Analytics"])

    st.divider()
    st.markdown("### 📊 Model")
    st.write(meta.name)
    st.write(f"v{meta.version}")

# ==================== DASHBOARD ====================
if page == "Dashboard":

    latest = df_raw.iloc[-1]

    st.markdown("## 📊 Live Conditions")

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.markdown('<div class="card">🌡️ Temp<br><b>{:.1f}°C</b></div>'.format(latest.get("temperature", 0)), unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="card">💧 Humidity<br><b>{:.0f}%</b></div>'.format(latest.get("humidity", 0)), unsafe_allow_html=True)
    with c3:
        st.markdown('<div class="card">💨 Wind<br><b>{:.1f} m/s</b></div>'.format(latest.get("wind_speed", 0)), unsafe_allow_html=True)
    with c4:
        val = latest.get("calculated_aqi", 0)
        label, color = aqi_label(val)
        st.markdown(f'<div class="card">📊 AQI<br><b style="color:{color}">{val:.0f} ({label})</b></div>', unsafe_allow_html=True)

# ==================== FORECAST ====================
if page == "Forecast":

    st.markdown("## 🔮 3-Day Forecast")

    features = [c for c in df.columns if c.endswith("_scaled")]
    latest_row = df.iloc[-1:][features]

    pred = model.predict(latest_row)[0]

    cols = st.columns(3)

    for i, c in enumerate(cols):
        v = pred[i]
        label, color = aqi_label(v)

        c.markdown(f"""
        <div class="card" style="text-align:center">
            <h3>Day {i+1}</h3>
            <h1 style="color:{color}">{v:.0f}</h1>
            <p>{label}</p>
        </div>
        """, unsafe_allow_html=True)

    fig = go.Figure()
    fig.add_trace(go.Bar(y=pred, x=["Day 1","Day 2","Day 3"]))
    st.plotly_chart(fig, use_container_width=True)

# ==================== ANALYTICS ====================
if page == "Analytics":

    st.markdown("## 📈 Trends")

    daily = df.set_index("timestamp")["calculated_aqi"].resample("D").mean().reset_index()

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=daily["timestamp"], y=daily["calculated_aqi"], mode="lines"))
    st.plotly_chart(fig, use_container_width=True)

# ==================== FOOTER ====================
st.divider()
st.caption("Built with Streamlit • AI Forecasting System • Skardu AQI Project")