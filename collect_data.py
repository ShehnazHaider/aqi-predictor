import os
import requests
import pandas as pd
from datetime import datetime, timezone
import hopsworks
from dotenv import load_dotenv

# =========================================================
# LOAD ENV VARIABLES
# =========================================================

load_dotenv()

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

HOPSWORKS_API_KEY = os.getenv("HOPSWORKS_API_KEY")
HOPSWORKS_PROJECT = os.getenv("HOPSWORKS_PROJECT_NAME")
HOPSWORKS_HOST = os.getenv("HOPSWORKS_HOST")

# =========================================================
# LOCATION — SKARDU
# =========================================================

LAT = 35.29
LON = 75.63

# =========================================================
# FETCH DATA
# =========================================================

def fetch_data():

    # -----------------------------
    # OPEN-METEO WEATHER DATA
    # -----------------------------
    
    weather_url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={LAT}"
        f"&longitude={LON}"
        f"&current="
        f"temperature_2m,relative_humidity_2m,"
        f"wind_speed_10m,surface_pressure"
        f"&timezone=UTC"
    )

    weather_response = requests.get(weather_url)
    weather_data = weather_response.json()["current"]

    # -----------------------------
    # OPENWEATHER AIR POLLUTION
    # -----------------------------

    pollution_url = (
        f"https://api.openweathermap.org/data/2.5/air_pollution?"
        f"lat={LAT}&lon={LON}&appid={OPENWEATHER_API_KEY}"
    )

    pollution_response = requests.get(pollution_url)
    pollution_data = pollution_response.json()["list"][0]

    # -----------------------------
    # TIMESTAMP
    # -----------------------------

    now = datetime.now(timezone.utc)

    # -----------------------------
    # FEATURE ENGINEERING
    # -----------------------------

    hour = now.hour
    day_of_week = now.weekday()

    df = pd.DataFrame([{

        # PRIMARY TIME COLUMN
        "timestamp": now,

        # TIME FEATURES
        "year": now.year,
        "month": now.month,
        "day": now.day,
        "hour": hour,
        "day_of_week": day_of_week,
        "is_weekend": int(day_of_week >= 5),

        # WEATHER FEATURES
        "temperature": float(weather_data["temperature_2m"]),
        "humidity": float(weather_data["relative_humidity_2m"]),
        "wind_speed": float(weather_data["wind_speed_10m"]),
        "surface_pressure": float(weather_data["surface_pressure"]),

        # POLLUTION FEATURES
        "aqi_index": int(pollution_data["main"]["aqi"]),
        "co": float(pollution_data["components"]["co"]),
        "no2": float(pollution_data["components"]["no2"]),
        "o3": float(pollution_data["components"]["o3"]),
        "so2": float(pollution_data["components"]["so2"]),
        "pm2_5": float(pollution_data["components"]["pm2_5"]),
        "pm10": float(pollution_data["components"]["pm10"]),
        "nh3": float(pollution_data["components"]["nh3"]),
    }])

    return df

# =========================================================
# UPLOAD TO HOPSWORKS
# =========================================================

def upload_to_hopsworks(df):

    project = hopsworks.login(
        api_key_value=HOPSWORKS_API_KEY,
        project=HOPSWORKS_PROJECT,
        host=HOPSWORKS_HOST
    )

    fs = project.get_feature_store()

    # NEW FEATURE GROUP VERSION
    fg = fs.get_or_create_feature_group(
        name="aqi_features_skardu",
        version=2,
        primary_key=["timestamp"],
        event_time="timestamp",
        description="Improved AQI + weather feature group for Skardu",
        online_enabled=True
    )

    fg.insert(
        df,
        write_options={
            "wait_for_job": False,
            "use_kafka": False
        }
    )

    print("✅ Data inserted into Hopsworks successfully")

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    print("🔄 Fetching data...")

    df = fetch_data()

    print(df)

    print("🔄 Uploading to Hopsworks...")

    upload_to_hopsworks(df)

    print("✅ Pipeline completed!")