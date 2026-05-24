import os
import requests
import pandas as pd
from datetime import datetime
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
# FETCH CURRENT DATA
# =========================================================

def fetch_data():

    # =====================================================
    # POLLUTION DATA — OPENWEATHER
    # =====================================================

    pollution_url = (
        f"https://api.openweathermap.org/data/2.5/air_pollution?"
        f"lat={LAT}"
        f"&lon={LON}"
        f"&appid={OPENWEATHER_API_KEY}"
    )

    pollution_response = requests.get(
        pollution_url,
        timeout=30
    )

    pollution = pollution_response.json()["list"][0]

    # =====================================================
    # WEATHER DATA — OPENWEATHER
    # =====================================================

    weather_url = (
        f"https://api.openweathermap.org/data/2.5/weather?"
        f"lat={LAT}"
        f"&lon={LON}"
        f"&appid={OPENWEATHER_API_KEY}"
        f"&units=metric"
    )

    weather_response = requests.get(
        weather_url,
        timeout=30
    )

    weather = weather_response.json()

    # =====================================================
    # TIMESTAMP
    # =====================================================

    now = datetime.utcnow().replace(
        minute=0,
        second=0,
        microsecond=0
    )

    # =====================================================
    # FINAL ROW
    # =====================================================

    row = {

        "timestamp": now,

        # WEATHER FEATURES
        "temperature":
            float(weather["main"]["temp"]),

        "humidity":
            int(weather["main"]["humidity"]),

        "wind_speed":
            float(weather["wind"]["speed"]),

        # TIME FEATURES
        "year": now.year,

        "month": now.month,

        "day": now.day,

        "hour": now.hour,

        "day_of_week": now.weekday(),

        "is_weekend":
            int(now.weekday() >= 5),

        # AQI + POLLUTANTS
        "aqi_index":
            int(pollution["main"]["aqi"]),

        "co":
            float(pollution["components"]["co"]),

        "no2":
            float(pollution["components"]["no2"]),

        "o3":
            float(pollution["components"]["o3"]),

        "so2":
            float(pollution["components"]["so2"]),

        "pm2_5":
            float(pollution["components"]["pm2_5"]),

        "pm10":
            float(pollution["components"]["pm10"]),

        "nh3":
            float(pollution["components"]["nh3"]),
    }

    return pd.DataFrame([row])

# =========================================================
# UPLOAD TO HOPSWORKS
# =========================================================

def upload_to_hopsworks(df):

    print("🔄 Connecting to Hopsworks...")

    project = hopsworks.login(
        api_key_value=HOPSWORKS_API_KEY,
        project=HOPSWORKS_PROJECT,
        host=HOPSWORKS_HOST
    )

    fs = project.get_feature_store()

    # =====================================================
    # SAME FEATURE GROUP AS FINAL DATASET
    # =====================================================

    fg = fs.get_feature_group(
        name="skardu_aqi_prediction",
        version=1
    )

    print("🔄 Uploading new hourly row...")

    fg.insert(
        df,
        write_options={
            "wait_for_job": False,
            "use_kafka": False
        }
    )

    print("✅ Data inserted successfully")

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    print("🔄 Fetching latest AQI data...")

    df = fetch_data()

    print(df)

    print("🔄 Uploading to Hopsworks...")

    upload_to_hopsworks(df)

    print("✅ Hourly pipeline completed!")