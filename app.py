import os
from dotenv import load_dotenv
import streamlit as st
import hopsworks
import pandas as pd
import numpy as np
import joblib
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px

# ==================== SETUP ====================
load_dotenv()

# ==================== PAGE CONFIG ====================
st.set_page_config(
    page_title="AQI Predictor - Skardu",
    page_icon="🌫️",
    layout="wide"
)

# ==================== STYLING ====================
st.markdown("""
<style>
    .main-header { font-size: 2.5rem; font-weight: 700; margin-bottom: 0; }
    .aqi-good { color: #00e400; font-weight: bold; }
    .aqi-moderate { color: #ffff00; font-weight: bold; }
    .aqi-sensitive { color: #ff7e00; font-weight: bold; }
    .aqi-unhealthy { color: #ff0000; font-weight: bold; }
    .aqi-very-unhealthy { color: #8f3f97; font-weight: bold; }
    .aqi-hazardous { color: #7e0023; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# ==================== CONNECT TO HOPSWORKS ====================
@st.cache_resource(ttl=3600)
def connect_hopsworks():
    project = hopsworks.login(api_key_value=os.environ["HOPSWORKS_API_KEY"])
    fs = project.get_feature_store()
    mr = project.get_model_registry()
    return fs, mr

# ==================== LOAD DATA ====================
@st.cache_data(ttl=1800)
def load_data():
    fs, _ = connect_hopsworks()
    fg = fs.get_feature_group(name="processed_aqi_skardu_v2", version=1)
    df = fg.read()
    df = df.sort_values("timestamp")
    return df

@st.cache_data(ttl=1800)
def load_raw_data():
    fs, _ = connect_hopsworks()
    fg = fs.get_feature_group(name="aqi_predictionv2", version=1)
    df = fg.read()
    df = df.sort_values("timestamp")
    return df

# ==================== LOAD BEST MODEL ====================
@st.cache_resource(ttl=3600)
def load_best_model():
    _, mr = connect_hopsworks()
    try:
        best = mr.get_best_model(name="aqi_xgb", metric="rmse_overall", direction="min")
        model_path = best.download()
        if os.path.isdir(model_path):
            files = os.listdir(model_path)
            pkl_files = [f for f in files if f.endswith('.pkl')]
            model_file = os.path.join(model_path, pkl_files[0]) if pkl_files else os.path.join(model_path, "model.pkl")
        else:
            model_file = model_path
        model = joblib.load(model_file)
        return model, best
    except Exception as e:
        st.warning(f"Best model not found, using fallback")
        models = mr.get_models(name="aqi_xgb")
        latest = models[-1]
        model_path = latest.download()
        if os.path.isdir(model_path):
            files = os.listdir(model_path)
            pkl_files = [f for f in files if f.endswith('.pkl')]
            model_file = os.path.join(model_path, pkl_files[0]) if pkl_files else os.path.join(model_path, "model.pkl")
        else:
            model_file = model_path
        model = joblib.load(model_file)
        return model, latest

# ==================== AQI HELPERS ====================
def get_aqi_level(aqi_value):
    if aqi_value <= 50:
        return "Good", "🟢", "#00e400"
    elif aqi_value <= 100:
        return "Moderate", "🟡", "#ffff00"
    elif aqi_value <= 150:
        return "Unhealthy for Sensitive Groups", "🟠", "#ff7e00"
    elif aqi_value <= 200:
        return "Unhealthy", "🔴", "#ff0000"
    elif aqi_value <= 300:
        return "Very Unhealthy", "🟣", "#8f3f97"
    else:
        return "Hazardous", "🟤", "#7e0023"

def get_aqi_health_advice(aqi_value):
    if aqi_value <= 50:
        return "Air quality is satisfactory. Enjoy outdoor activities!"
    elif aqi_value <= 100:
        return "Air quality is acceptable. Sensitive individuals should limit prolonged outdoor exertion."
    elif aqi_value <= 150:
        return "Sensitive groups may experience health effects. General public is less likely to be affected."
    elif aqi_value <= 200:
        return "Everyone may begin to experience health effects. Limit outdoor activities."
    elif aqi_value <= 300:
        return "Health alert: Everyone may experience more serious health effects. Avoid outdoor activities."
    else:
        return "Health warning: Emergency conditions. Stay indoors with windows closed."

# ==================== MAIN APP ====================
st.markdown('<p class="main-header">🌫️ AQI Predictor — Skardu</p>', unsafe_allow_html=True)
st.caption("3-Day Air Quality Index Forecast | Powered by Machine Learning")

# Load data and model
with st.spinner("🔄 Loading data and model..."):
    df = load_data()
    df_raw = load_raw_data()
    model, model_meta = load_best_model()

# ==================== SIDEBAR ====================
with st.sidebar:
    st.header("📊 Model Information")
    st.metric("Best Model", model_meta.name.upper())
    st.metric("Version", model_meta.version)
    
    metrics = model_meta.training_metrics
    st.metric("R² Score", f"{metrics.get('r2_overall', 0):.3f}")
    st.metric("RMSE", f"{metrics.get('rmse_overall', 0):.1f}")
    st.metric("MAE", f"{metrics.get('mae_overall', 0):.1f}")
    
    st.divider()
    st.header("📅 Dataset Info")
    st.metric("Total Records", f"{len(df):,}")
    st.metric("Date Range", f"{df['timestamp'].min().strftime('%Y-%m-%d')} → {df['timestamp'].max().strftime('%Y-%m-%d')}")
    
    st.divider()
    st.header("🎯 About")
    st.markdown("""
    This dashboard predicts **Air Quality Index (AQI)** for Skardu, Pakistan 
    for the next 3 days using machine learning models trained on historical 
    weather and pollutant data.
    
    **Features:** PM2.5, PM10, CO, NO₂, O₃, SO₂, NH₃, temperature, 
    humidity, wind speed + rolling statistics and lag features.
    
    **Models:** Linear Regression, Random Forest, XGBoost, Gradient Boosting
    """)

# ==================== PREDICTIONS SECTION ====================
st.header("🔮 3-Day AQI Forecast")

# Get feature columns
feature_cols = [c for c in df.columns if c.endswith("_scaled")]

# Get latest data point
latest = df.sort_values("timestamp").iloc[-1:]

# Predict
prediction = model.predict(latest[feature_cols])[0]

# Display forecast cards
col1, col2, col3 = st.columns(3, gap="medium")

days = [
    (datetime.now() + timedelta(days=i+1)).strftime("%A, %b %d")
    for i in range(3)
]

for i, (col, day, pred) in enumerate(zip([col1, col2, col3], days, prediction)):
    level, emoji, color = get_aqi_level(pred)
    
    with col:
        with st.container(border=True):
            st.markdown(f"### 📅 {day}")
            st.markdown(f"<h1 style='text-align: center; color: {color}; font-size: 3rem;'>{pred:.0f}</h1>", unsafe_allow_html=True)
            st.markdown(f"<p style='text-align: center;'>{emoji} <b>{level}</b></p>", unsafe_allow_html=True)

# Health advice
st.divider()
max_aqi = max(prediction)
advice = get_aqi_health_advice(max_aqi)

if max_aqi > 150:
    st.error(f"⚠️ **HEALTH ADVISORY:** {advice}")
elif max_aqi > 100:
    st.warning(f"⚡ **NOTE:** {advice}")
else:
    st.success(f"✅ {advice}")

# ==================== 3-DAY FORECAST CHART ====================
st.header("📈 3-Day AQI Forecast Trend")

forecast_dates = [(datetime.now() + timedelta(days=i+1)).strftime("%a, %b %d") for i in range(3)]

fig_forecast = go.Figure()

colors_forecast = [get_aqi_level(p)[2] for p in prediction]
fig_forecast.add_trace(go.Bar(
    x=forecast_dates,
    y=prediction,
    marker_color=colors_forecast,
    text=[f"{p:.0f}" for p in prediction],
    textposition="outside",
    name="Predicted AQI"
))

thresholds = [
    (50, "Good", "#00e400"),
    (100, "Moderate", "#ffff00"),
    (150, "Unhealthy (Sensitive)", "#ff7e00"),
    (200, "Unhealthy", "#ff0000"),
]

for threshold, label, color in thresholds:
    fig_forecast.add_hline(
        y=threshold, line_dash="dash", line_color=color,
        annotation_text=f"{label} ({threshold})",
        annotation_position="right"
    )

fig_forecast.update_layout(
    title="Predicted AQI — Next 3 Days",
    yaxis_title="AQI Value",
    height=400,
    showlegend=False,
    margin=dict(t=40, b=20)
)

st.plotly_chart(fig_forecast, use_container_width=True)

# ==================== HISTORICAL TREND ====================
st.header("📉 Historical AQI Trend")

period_options = {
    "Last 7 Days": 7,
    "Last 14 Days": 14,
    "Last 30 Days": 30,
    "Last 90 Days": 90,
    "All Time": 999,
}
selected_period = st.selectbox("Select time period", list(period_options.keys()), index=2)

days_to_show = period_options[selected_period]
if days_to_show == 999:
    chart_df = df.copy()
else:
    cutoff = df["timestamp"].max() - timedelta(days=days_to_show)
    chart_df = df[df["timestamp"] >= cutoff]

daily_aqi = chart_df.set_index("timestamp")["calculated_aqi"].resample("D").max().reset_index()
daily_aqi.columns = ["Date", "Max AQI"]
daily_aqi["Color"] = daily_aqi["Max AQI"].apply(lambda x: get_aqi_level(x)[2])

fig_hist = go.Figure()

fig_hist.add_trace(go.Scatter(
    x=daily_aqi["Date"],
    y=daily_aqi["Max AQI"],
    mode="lines+markers",
    marker=dict(color=daily_aqi["Color"], size=6),
    line=dict(color="#1f77b4", width=1),
    name="Daily Max AQI"
))

fig_hist.update_layout(
    title=f"Daily Maximum AQI — {selected_period}",
    yaxis_title="AQI Value",
    height=400,
    margin=dict(t=40, b=20),
    hovermode="x unified"
)

st.plotly_chart(fig_hist, use_container_width=True)

# ==================== CURRENT CONDITIONS ====================
st.header("🌡️ Current Conditions")

latest_raw = df_raw.sort_values("timestamp").iloc[-1]

c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    temp = latest_raw.get('temperature', None)
    st.metric("🌡️ Temperature", f"{temp:.1f}°C" if pd.notna(temp) else "N/A")
with c2:
    hum = latest_raw.get('humidity', None)
    st.metric("💧 Humidity", f"{hum:.0f}%" if pd.notna(hum) else "N/A")
with c3:
    wind = latest_raw.get('wind_speed', None)
    st.metric("💨 Wind Speed", f"{wind:.1f} m/s" if pd.notna(wind) else "N/A")
with c4:
    aqi_val = latest_raw.get('calculated_aqi', None)
    if pd.notna(aqi_val):
        level, emoji, _ = get_aqi_level(aqi_val)
        st.metric("📊 Current AQI", f"{aqi_val:.0f}")
    else:
        st.metric("📊 Current AQI", "N/A")
with c5:
    ts = latest_raw.get("timestamp", None)
    st.metric("🕐 Last Updated", pd.to_datetime(ts).strftime("%H:%M") if pd.notna(ts) else "N/A")

# ==================== POLLUTANT BREAKDOWN ====================
st.header("🔬 Current Pollutant Levels")

pollutants = {
    "PM2.5": ("pm2_5", "μg/m³", 35),
    "PM10": ("pm10", "μg/m³", 150),
    "CO": ("co", "μg/m³", 10000),
    "NO₂": ("no2", "μg/m³", 200),
    "O₃": ("o3", "μg/m³", 180),
    "SO₂": ("so2", "μg/m³", 75),
    "NH₃": ("nh3", "μg/m³", 200),
}

p_col1, p_col2, p_col3, p_col4 = st.columns(4)

for i, (name, (col, unit, safe_limit)) in enumerate(pollutants.items()):
    with [p_col1, p_col2, p_col3, p_col4][i % 4]:
        value = latest_raw.get(col, 0)
        if pd.notna(value):
            status = "🟢" if value <= safe_limit else "🔴"
            st.metric(f"{status} {name}", f"{value:.1f} {unit}")
        else:
            st.metric(f"⚪ {name}", "N/A")

# ==================== FOOTER ====================
st.divider()
st.caption("🔄 Data updates hourly | Models retrain daily at 3 AM UTC | Built with ❤️ for Skardu, Pakistan")
st.caption(f"Last data refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")