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
    page_title="AQI — Skardu",
    page_icon="●",
    layout="wide"
)

# ==================== DESIGN SYSTEM ====================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    * { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; }

    .stApp {
        background: linear-gradient(160deg, #e8f0fe 0%, #f3f5f9 30%, #fafbfd 70%, #f0f4ff 100%);
    }

    /* Hide Streamlit branding */
    #MainMenu, footer, header { visibility: hidden; }

    /* Glass card */
    .glass-card {
        background: rgba(255, 255, 255, 0.65);
        backdrop-filter: blur(20px);
        -webkit-backdrop-filter: blur(20px);
        border-radius: 20px;
        border: 1px solid rgba(255, 255, 255, 0.8);
        box-shadow: 0 4px 24px rgba(0, 0, 0, 0.04), 0 1px 2px rgba(0, 0, 0, 0.02);
        padding: 28px;
        transition: all 0.3s ease;
    }
    .glass-card:hover {
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.07);
        transform: translateY(-1px);
    }

    /* Hero AQI number */
    .aqi-hero {
        font-size: 5.5rem;
        font-weight: 800;
        letter-spacing: -2px;
        line-height: 1;
    }

    /* Location label */
    .location-label {
        font-size: 0.85rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #9ca3af;
    }

    /* Section title */
    .section-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #374151;
        letter-spacing: -0.01em;
        margin-bottom: 16px;
    }

    /* AQI color system */
    .aqi-color-good { color: #00c853; }
    .aqi-color-moderate { color: #fbc02d; }
    .aqi-color-sensitive { color: #fb8c00; }
    .aqi-color-unhealthy { color: #e53935; }
    .aqi-color-very-unhealthy { color: #8e24aa; }
    .aqi-color-hazardous { color: #6d001a; }

    /* Forecast day card */
    .forecast-day {
        text-align: center;
        padding: 12px 0;
    }
    .forecast-date {
        font-size: 0.8rem;
        font-weight: 500;
        color: #9ca3af;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .forecast-value {
        font-size: 2.2rem;
        font-weight: 700;
        margin: 8px 0;
    }
    .forecast-label {
        font-size: 0.8rem;
        font-weight: 500;
    }

    /* Metric pill */
    .metric-pill {
        background: rgba(255, 255, 255, 0.5);
        border-radius: 12px;
        padding: 12px 16px;
        text-align: center;
    }
    .metric-value {
        font-size: 1.3rem;
        font-weight: 600;
        color: #1f2937;
    }
    .metric-label {
        font-size: 0.7rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: #9ca3af;
        margin-top: 2px;
    }

    /* Hide default Streamlit padding */
    .block-container { padding-top: 2rem; padding-bottom: 1rem; }

    /* Divider */
    .soft-divider {
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(0,0,0,0.06), transparent);
        margin: 8px 0;
    }
</style>
""", unsafe_allow_html=True)

# ==================== DATA CONNECTIONS ====================
@st.cache_resource(ttl=3600)
def connect_hopsworks():
    project = hopsworks.login(api_key_value=os.environ["HOPSWORKS_API_KEY"])
    return project.get_feature_store(), project.get_model_registry()

@st.cache_data(ttl=1800)
def load_data():
    fs, _ = connect_hopsworks()
    fg = fs.get_feature_group(name="processed_aqi_skardu_v2", version=1)
    df = fg.read().sort_values("timestamp")
    return df

@st.cache_data(ttl=1800)
def load_raw_data():
    fs, _ = connect_hopsworks()
    fg = fs.get_feature_group(name="aqi_predictionv2", version=1)
    df = fg.read().sort_values("timestamp")
    return df

@st.cache_resource(ttl=3600)
def load_model():
    _, mr = connect_hopsworks()
    try:
        best = mr.get_best_model(name="aqi_xgb", metric="rmse_overall", direction="min")
        model_path = best.download()
        if os.path.isdir(model_path):
            pkl_files = [f for f in os.listdir(model_path) if f.endswith('.pkl')]
            model_file = os.path.join(model_path, pkl_files[0]) if pkl_files else os.path.join(model_path, "model.pkl")
        else:
            model_file = model_path
        return joblib.load(model_file), best
    except:
        models = mr.get_models(name="aqi_xgb")
        latest = models[-1]
        model_path = latest.download()
        if os.path.isdir(model_path):
            pkl_files = [f for f in os.listdir(model_path) if f.endswith('.pkl')]
            model_file = os.path.join(model_path, pkl_files[0]) if pkl_files else os.path.join(model_path, "model.pkl")
        else:
            model_file = model_path
        return joblib.load(model_file), latest

# ==================== HELPERS ====================
def get_aqi_info(val):
    if val <= 50: return "Good", "#00c853", "aqi-color-good"
    if val <= 100: return "Moderate", "#fbc02d", "aqi-color-moderate"
    if val <= 150: return "Sensitive", "#fb8c00", "aqi-color-sensitive"
    if val <= 200: return "Unhealthy", "#e53935", "aqi-color-unhealthy"
    if val <= 300: return "Very Unhealthy", "#8e24aa", "aqi-color-very-unhealthy"
    return "Hazardous", "#6d001a", "aqi-color-hazardous"

def get_advice(val):
    if val <= 50: return "Air quality is ideal for outdoor activities."
    if val <= 100: return "Generally safe. Sensitive individuals should take breaks."
    if val <= 150: return "Sensitive groups may experience effects."
    if val <= 200: return "Everyone may feel effects. Limit outdoor time."
    if val <= 300: return "Serious health risks. Avoid outdoor exposure."
    return "Emergency conditions. Stay indoors."

# ==================== LOAD ====================
with st.spinner(""):
    df = load_data()
    df_raw = load_raw_data()
    model, model_meta = load_model()

feature_cols = [c for c in df.columns if c.endswith("_scaled")]
latest = df.sort_values("timestamp").iloc[-1:]
latest_raw = df_raw.sort_values("timestamp").iloc[-1]
prediction = model.predict(latest[feature_cols])[0]
current_aqi = latest_raw.get("calculated_aqi", prediction[0])

level, color, css_class = get_aqi_info(current_aqi)
advice = get_advice(max(prediction))

# ==================== LAYOUT ====================

# ── TOP ROW: Current AQI Hero + Weather ──
st.markdown("""
<div style="display: flex; align-items: center; gap: 48px; padding: 8px 0 24px 0;">
    <div>
        <div class="location-label">● Skardu, Pakistan</div>
        <div style="font-size: 2.4rem; font-weight: 700; color: #111827; letter-spacing: -0.5px;">Air Quality</div>
    </div>
</div>
""", unsafe_allow_html=True)

col_hero, col_weather = st.columns([1.2, 1])

with col_hero:
    st.markdown(f"""
    <div class="glass-card" style="padding: 32px 36px;">
        <div class="location-label" style="margin-bottom: 4px;">Current AQI</div>
        <div class="aqi-hero {css_class}" style="margin: 4px 0;">{current_aqi:.0f}</div>
        <div style="font-size: 1.1rem; font-weight: 600; color: {color}; margin-bottom: 8px;">{level}</div>
        <div style="font-size: 0.85rem; color: #6b7280; line-height: 1.5;">{advice}</div>
    </div>
    """, unsafe_allow_html=True)

with col_weather:
    temp = latest_raw.get("temperature", None)
    hum = latest_raw.get("humidity", None)
    wind = latest_raw.get("wind_speed", None)
    
    st.markdown(f"""
    <div class="glass-card" style="padding: 28px 32px;">
        <div class="section-title">Weather</div>
        <div style="display: flex; gap: 24px; margin-top: 12px;">
            <div class="metric-pill" style="flex:1;">
                <div class="metric-value">{temp:.1f}°</div>
                <div class="metric-label">Temperature</div>
            </div>
            <div class="metric-pill" style="flex:1;">
                <div class="metric-value">{hum:.0f}%</div>
                <div class="metric-label">Humidity</div>
            </div>
            <div class="metric-pill" style="flex:1;">
                <div class="metric-value">{wind:.1f}</div>
                <div class="metric-label">Wind m/s</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

# ── SOFT DIVIDER ──
st.markdown('<div class="soft-divider"></div>', unsafe_allow_html=True)

# ── 3-DAY FORECAST ──
st.markdown('<div class="section-title" style="margin-top: 8px;">3–Day Forecast</div>', unsafe_allow_html=True)

fc_col1, fc_col2, fc_col3 = st.columns(3)
days = [(datetime.now() + timedelta(days=i+1)).strftime("%a, %b %d") for i in range(3)]

for col, day, pred in zip([fc_col1, fc_col2, fc_col3], days, prediction):
    lvl, clr, _ = get_aqi_info(pred)
    with col:
        st.markdown(f"""
        <div class="glass-card forecast-day">
            <div class="forecast-date">{day}</div>
            <div class="forecast-value" style="color: {clr};">{pred:.0f}</div>
            <div class="forecast-label" style="color: {clr};">{lvl}</div>
        </div>
        """, unsafe_allow_html=True)

# ── MINI SPARKLINE CHART ──
st.markdown('<div style="margin-top: 16px;"></div>', unsafe_allow_html=True)
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=days, y=prediction,
    mode="lines+markers",
    line=dict(color="#6366f1", width=2.5, shape="spline"),
    marker=dict(size=10, color=[get_aqi_info(p)[1] for p in prediction], line=dict(width=2, color="white")),
    hovertemplate="%{x}<br>AQI: %{y:.0f}<extra></extra>"
))
fig.update_layout(
    height=180, margin=dict(l=0, r=0, t=0, b=0),
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    xaxis=dict(showgrid=False, zeroline=False, showticklabels=True, color="#9ca3af"),
    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
)
st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

# ── SOFT DIVIDER ──
st.markdown('<div class="soft-divider"></div>', unsafe_allow_html=True)

# ── POLLUTANTS ──
st.markdown('<div class="section-title">Key Pollutants</div>', unsafe_allow_html=True)

pollutants = [
    ("PM₂.₅", latest_raw.get("pm2_5", 0), 35, "Fine particles"),
    ("PM₁₀", latest_raw.get("pm10", 0), 150, "Coarse particles"),
    ("NO₂", latest_raw.get("no2", 0), 200, "Nitrogen dioxide"),
    ("O₃", latest_raw.get("o3", 0), 180, "Ozone"),
]

p_cols = st.columns(4)
for col, (name, val, limit, desc) in zip(p_cols, pollutants):
    pct = min(val / limit * 100, 100) if limit > 0 else 0
    bar_color = "#00c853" if pct < 50 else "#fbc02d" if pct < 80 else "#e53935"
    with col:
        st.markdown(f"""
        <div class="glass-card" style="padding: 20px;">
            <div style="display: flex; justify-content: space-between; align-items: baseline;">
                <span style="font-weight: 600; color: #374151;">{name}</span>
                <span style="font-size: 0.75rem; color: #9ca3af;">{desc}</span>
            </div>
            <div style="font-size: 2rem; font-weight: 700; color: #111827; margin: 8px 0;">{val:.1f}</div>
            <div style="font-size: 0.7rem; color: #9ca3af; margin-bottom: 6px;">μg/m³ · Limit: {limit}</div>
            <div style="background: #f3f4f6; border-radius: 8px; height: 6px;">
                <div style="background: {bar_color}; width: {pct}%; height: 6px; border-radius: 8px; transition: width 0.5s ease;"></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

# ── FOOTER ──
st.markdown(f"""
<div style="text-align: center; padding: 24px 0 8px 0; color: #cbd5e1; font-size: 0.75rem; font-weight: 400;">
    Updated {datetime.now().strftime('%b %d, %H:%M')} · Models retrain daily
</div>
""", unsafe_allow_html=True)