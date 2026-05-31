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
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* Main background */
    .stApp {
        background: linear-gradient(135deg, #0a0e1a 0%, #0d1421 50%, #0a1628 100%);
    }

    /* Hide default header */
    header[data-testid="stHeader"] {
        background: transparent;
    }

    /* Main header */
    .main-header {
        font-size: 2.8rem;
        font-weight: 700;
        background: linear-gradient(135deg, #60a5fa, #a78bfa, #34d399);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        letter-spacing: -0.5px;
        margin-bottom: 0;
    }

    /* Metric cards */
    [data-testid="metric-container"] {
        background: rgba(255, 255, 255, 0.04);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 16px;
        padding: 16px 20px;
        backdrop-filter: blur(10px);
        transition: all 0.2s ease;
    }

    [data-testid="metric-container"]:hover {
        background: rgba(255, 255, 255, 0.07);
        border-color: rgba(96, 165, 250, 0.3);
        transform: translateY(-2px);
    }

    [data-testid="stMetricLabel"] {
        font-size: 0.75rem !important;
        font-weight: 500 !important;
        color: rgba(255, 255, 255, 0.5) !important;
        text-transform: uppercase;
        letter-spacing: 0.8px;
    }

    [data-testid="stMetricValue"] {
        font-size: 1.6rem !important;
        font-weight: 600 !important;
        color: #f1f5f9 !important;
    }

    /* Containers with border */
    [data-testid="stVerticalBlockBorderWrapper"] {
        background: rgba(255, 255, 255, 0.03) !important;
        border: 1px solid rgba(255, 255, 255, 0.07) !important;
        border-radius: 20px !important;
        padding: 8px !important;
        backdrop-filter: blur(20px);
        transition: all 0.3s ease;
    }

    [data-testid="stVerticalBlockBorderWrapper"]:hover {
        border-color: rgba(96, 165, 250, 0.25) !important;
        background: rgba(255, 255, 255, 0.05) !important;
    }

    /* Section headers */
    h1, h2, h3 {
        color: #f1f5f9 !important;
        font-weight: 600 !important;
        letter-spacing: -0.3px;
    }

    /* Selectbox */
    [data-testid="stSelectbox"] > div > div {
        background: rgba(255, 255, 255, 0.05) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        border-radius: 12px !important;
        color: #f1f5f9 !important;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: rgba(10, 14, 26, 0.95) !important;
        border-right: 1px solid rgba(255, 255, 255, 0.06) !important;
    }

    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {
        color: #94a3b8 !important;
        font-size: 0.75rem !important;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-weight: 600 !important;
    }

    /* Alert boxes */
    [data-testid="stAlert"] {
        border-radius: 14px !important;
        border: none !important;
        backdrop-filter: blur(10px);
    }

    /* Success alert */
    [data-testid="stAlert"][data-baseweb="notification"] {
        background: rgba(52, 211, 153, 0.08) !important;
        border: 1px solid rgba(52, 211, 153, 0.2) !important;
    }

    /* Warning alert */
    .stWarning {
        background: rgba(251, 191, 36, 0.08) !important;
        border: 1px solid rgba(251, 191, 36, 0.2) !important;
        border-radius: 14px !important;
    }

    /* Error alert */
    .stError {
        background: rgba(239, 68, 68, 0.08) !important;
        border: 1px solid rgba(239, 68, 68, 0.2) !important;
        border-radius: 14px !important;
    }

    /* Spinner */
    [data-testid="stSpinner"] {
        color: #60a5fa !important;
    }

    /* Divider */
    hr {
        border-color: rgba(255, 255, 255, 0.06) !important;
        margin: 24px 0 !important;
    }

    /* Caption text */
    [data-testid="stCaptionContainer"] {
        color: rgba(255, 255, 255, 0.35) !important;
        font-size: 0.75rem !important;
    }

    /* Plotly chart background */
    .js-plotly-plot {
        border-radius: 16px;
        overflow: hidden;
    }

    /* Scrollbar */
    ::-webkit-scrollbar {
        width: 4px;
    }
    ::-webkit-scrollbar-track {
        background: transparent;
    }
    ::-webkit-scrollbar-thumb {
        background: rgba(255, 255, 255, 0.1);
        border-radius: 4px;
    }

    /* AQI color classes */
    .aqi-good            { color: #34d399; font-weight: 600; }
    .aqi-moderate        { color: #fbbf24; font-weight: 600; }
    .aqi-sensitive       { color: #f97316; font-weight: 600; }
    .aqi-unhealthy       { color: #ef4444; font-weight: 600; }
    .aqi-very-unhealthy  { color: #a855f7; font-weight: 600; }
    .aqi-hazardous       { color: #7e0023; font-weight: 600; }

    /* Forecast card AQI number */
    .aqi-value {
        text-align: center;
        font-size: 3.5rem;
        font-weight: 700;
        letter-spacing: -2px;
        line-height: 1;
        margin: 12px 0;
        text-shadow: 0 0 30px currentColor;
    }

    .aqi-label {
        text-align: center;
        font-size: 0.85rem;
        font-weight: 500;
        color: rgba(255,255,255,0.6);
        margin-top: 6px;
    }

    .forecast-date {
        text-align: center;
        font-size: 0.8rem;
        font-weight: 600;
        color: rgba(255,255,255,0.4);
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 4px;
    }
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
        return "Good", "🟢", "#34d399"
    elif aqi_value <= 100:
        return "Moderate", "🟡", "#fbbf24"
    elif aqi_value <= 150:
        return "Unhealthy for Sensitive Groups", "🟠", "#f97316"
    elif aqi_value <= 200:
        return "Unhealthy", "🔴", "#ef4444"
    elif aqi_value <= 300:
        return "Very Unhealthy", "🟣", "#a855f7"
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
st.caption("3-Day Air Quality Index Forecast · Powered by Machine Learning · Data updates hourly")

st.markdown("<br>", unsafe_allow_html=True)

# Load data and model
with st.spinner("Loading data and model..."):
    df = load_data()
    df_raw = load_raw_data()
    model, model_meta = load_best_model()

# ==================== SIDEBAR ====================
with st.sidebar:
    st.markdown("### MODEL")
    st.metric("Active Model", model_meta.name.upper())
    st.metric("Version", model_meta.version)

    metrics = model_meta.training_metrics
    st.metric("R² Score", f"{metrics.get('r2_overall', 0):.3f}")
    st.metric("RMSE", f"{metrics.get('rmse_overall', 0):.1f}")
    st.metric("MAE", f"{metrics.get('mae_overall', 0):.1f}")

    st.divider()
    st.markdown("### DATASET")
    st.metric("Total Records", f"{len(df):,}")
    st.metric("From", df['timestamp'].min().strftime('%b %d, %Y'))
    st.metric("To", df['timestamp'].max().strftime('%b %d, %Y'))

    st.divider()
    st.markdown("### ABOUT")
    st.markdown("""
    <p style='color: rgba(255,255,255,0.5); font-size: 0.8rem; line-height: 1.6;'>
    Predicts AQI for <b style='color:#60a5fa'>Skardu, Pakistan</b> 
    for the next 3 days using ML models trained on historical 
    weather and pollutant data.<br><br>
    Features: PM2.5, PM10, CO, NO₂, O₃, SO₂, NH₃, 
    temperature, humidity, wind speed, rolling statistics 
    and lag features.
    </p>
    """, unsafe_allow_html=True)

# ==================== PREDICTIONS SECTION ====================
st.header("3-Day AQI Forecast")

feature_cols = [c for c in df.columns if c.endswith("_scaled")]
latest = df.sort_values("timestamp").iloc[-1:]
prediction = model.predict(latest[feature_cols])[0]

days = [
    (datetime.now() + timedelta(days=i+1)).strftime("%A, %b %d")
    for i in range(3)
]

col1, col2, col3 = st.columns(3, gap="medium")

for i, (col, day, pred) in enumerate(zip([col1, col2, col3], days, prediction)):
    level, emoji, color = get_aqi_level(pred)
    with col:
        with st.container(border=True):
            st.markdown(f'<p class="forecast-date">{day}</p>', unsafe_allow_html=True)
            st.markdown(
                f'<p class="aqi-value" style="color:{color};">{pred:.0f}</p>',
                unsafe_allow_html=True
            )
            st.markdown(
                f'<p class="aqi-label">{emoji} {level}</p>',
                unsafe_allow_html=True
            )

# Health advice
st.markdown("<br>", unsafe_allow_html=True)
max_aqi = max(prediction)
advice = get_aqi_health_advice(max_aqi)

if max_aqi > 150:
    st.error(f"⚠️ **HEALTH ADVISORY:** {advice}")
elif max_aqi > 100:
    st.warning(f"⚡ **NOTE:** {advice}")
else:
    st.success(f"✅ {advice}")

# ==================== 3-DAY FORECAST CHART ====================
st.markdown("<br>", unsafe_allow_html=True)
st.header("Forecast Trend")

forecast_dates = [
    (datetime.now() + timedelta(days=i+1)).strftime("%a, %b %d")
    for i in range(3)
]

fig_forecast = go.Figure()

colors_forecast = [get_aqi_level(p)[2] for p in prediction]
fig_forecast.add_trace(go.Bar(
    x=forecast_dates,
    y=prediction,
    marker=dict(
        color=colors_forecast,
        line=dict(width=0),
        opacity=0.85
    ),
    text=[f"{p:.0f}" for p in prediction],
    textposition="outside",
    textfont=dict(color="rgba(255,255,255,0.8)", size=14, family="Inter"),
    name="Predicted AQI"
))

thresholds = [
    (50, "Good", "#34d399"),
    (100, "Moderate", "#fbbf24"),
    (150, "Sensitive Groups", "#f97316"),
    (200, "Unhealthy", "#ef4444"),
]

for threshold, label, color in thresholds:
    fig_forecast.add_hline(
        y=threshold, line_dash="dot",
        line_color=color, line_width=1.5,
        opacity=0.5,
        annotation_text=label,
        annotation_position="right",
        annotation_font=dict(color=color, size=11)
    )

fig_forecast.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(255,255,255,0.02)",
    font=dict(family="Inter", color="rgba(255,255,255,0.7)"),
    yaxis=dict(
        title="AQI Value",
        gridcolor="rgba(255,255,255,0.05)",
        zerolinecolor="rgba(255,255,255,0.05)"
    ),
    xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
    height=380,
    showlegend=False,
    margin=dict(t=20, b=20, l=20, r=80)
)

st.plotly_chart(fig_forecast, use_container_width=True)

# ==================== HISTORICAL TREND ====================
st.markdown("<br>", unsafe_allow_html=True)
st.header("Historical AQI Trend")

period_options = {
    "Last 7 Days": 7,
    "Last 14 Days": 14,
    "Last 30 Days": 30,
    "Last 90 Days": 90,
    "All Time": 999,
}
selected_period = st.selectbox(
    "Select time period", list(period_options.keys()), index=2
)

days_to_show = period_options[selected_period]
if days_to_show == 999:
    chart_df = df.copy()
else:
    cutoff = df["timestamp"].max() - timedelta(days=days_to_show)
    chart_df = df[df["timestamp"] >= cutoff]

daily_aqi = (
    chart_df.set_index("timestamp")["calculated_aqi"]
    .resample("D").max()
    .reset_index()
)
daily_aqi.columns = ["Date", "Max AQI"]
daily_aqi["Color"] = daily_aqi["Max AQI"].apply(lambda x: get_aqi_level(x)[2])

fig_hist = go.Figure()

fig_hist.add_trace(go.Scatter(
    x=daily_aqi["Date"],
    y=daily_aqi["Max AQI"],
    mode="lines",
    line=dict(color="rgba(96,165,250,0.8)", width=2),
    fill="tozeroy",
    fillcolor="rgba(96,165,250,0.06)",
    name="Daily Max AQI",
    hovertemplate="<b>%{x|%b %d}</b><br>Max AQI: %{y:.0f}<extra></extra>"
))

fig_hist.add_trace(go.Scatter(
    x=daily_aqi["Date"],
    y=daily_aqi["Max AQI"],
    mode="markers",
    marker=dict(
        color=daily_aqi["Color"],
        size=6,
        line=dict(width=0)
    ),
    showlegend=False,
    hoverinfo="skip"
))

fig_hist.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(255,255,255,0.02)",
    font=dict(family="Inter", color="rgba(255,255,255,0.7)"),
    yaxis=dict(
        title="AQI Value",
        gridcolor="rgba(255,255,255,0.05)",
        zerolinecolor="rgba(255,255,255,0.05)"
    ),
    xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
    height=380,
    margin=dict(t=20, b=20, l=20, r=20),
    hovermode="x unified"
)

st.plotly_chart(fig_hist, use_container_width=True)

# ==================== CURRENT CONDITIONS ====================
st.markdown("<br>", unsafe_allow_html=True)
st.header("Current Conditions")

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
    st.metric(
        "🕐 Last Updated",
        pd.to_datetime(ts).strftime("%H:%M") if pd.notna(ts) else "N/A"
    )

# ==================== POLLUTANT BREAKDOWN ====================
st.markdown("<br>", unsafe_allow_html=True)
st.header("Pollutant Levels")

pollutants = {
    "PM2.5": ("pm2_5", "μg/m³", 35),
    "PM10":  ("pm10",  "μg/m³", 150),
    "CO":    ("co",    "μg/m³", 10000),
    "NO₂":   ("no2",   "μg/m³", 200),
    "O₃":    ("o3",    "μg/m³", 180),
    "SO₂":   ("so2",   "μg/m³", 75),
    "NH₃":   ("nh3",   "μg/m³", 200),
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
st.caption(
    "🔄 Data updates hourly · Models retrain daily at 3 AM UTC · "
    "Built for Skardu, Pakistan"
)
st.caption(
    f"Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
)