import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
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
# FETCH HISTORICAL DATA
# =========================================================

def fetch_historical_data(start_date, end_date):

    all_rows = []

    current = start_date

    while current <= end_date:

        timestamp = int(current.timestamp())

        # =================================================
        # OPENWEATHER HISTORICAL AIR POLLUTION
        # =================================================

        pollution_url = (
            f"https://api.openweathermap.org/data/2.5/air_pollution/history"
            f"?lat={LAT}"
            f"&lon={LON}"
            f"&start={timestamp}"
            f"&end={timestamp + 3600}"
            f"&appid={OPENWEATHER_API_KEY}"
        )

        pollution_resp = requests.get(pollution_url).json()

        if not pollution_resp.get("list"):
            print(f"⚠️ No pollution data for {current}")
            current += timedelta(hours=1)
            time.sleep(1)
            continue

        pollution = pollution_resp["list"][0]

        # =================================================
        # OPEN-METEO HISTORICAL WEATHER
        # =================================================

        date_str = current.strftime("%Y-%m-%d")
        hour_index = current.hour

        weather_url = (
            f"https://archive-api.open-meteo.com/v1/archive?"
            f"latitude={LAT}"
            f"&longitude={LON}"
            f"&start_date={date_str}"
            f"&end_date={date_str}"
            f"&hourly=temperature_2m,"
            f"relative_humidity_2m,"
            f"wind_speed_10m"
            f"&timezone=UTC"
        )

        weather_resp = requests.get(weather_url).json()

        try:
            hourly = weather_resp["hourly"]

            temperature = float(hourly["temperature_2m"][hour_index])
            humidity = float(hourly["relative_humidity_2m"][hour_index])
            wind_speed = float(hourly["wind_speed_10m"][hour_index])

        except Exception:
            print(f"⚠️ Weather data missing for {current}")

            temperature = 0.0
            humidity = 0.0
            wind_speed = 0.0

        # =================================================
        # FEATURE ENGINEERING
        # =================================================

        row = {

            # TIMESTAMP
            "timestamp": current,

            # TIME FEATURES
            "year": current.year,
            "month": current.month,
            "day": current.day,
            "hour": current.hour,
            "day_of_week": current.weekday(),
            "is_weekend": int(current.weekday() >= 5),

            # WEATHER FEATURES
            "temperature": temperature,
            "humidity": humidity,
            "wind_speed": wind_speed,

            # POLLUTION FEATURES
            "aqi_index": int(pollution["main"]["aqi"]),
            "co": float(pollution["components"]["co"]),
            "no2": float(pollution["components"]["no2"]),
            "o3": float(pollution["components"]["o3"]),
            "so2": float(pollution["components"]["so2"]),
            "pm2_5": float(pollution["components"]["pm2_5"]),
            "pm10": float(pollution["components"]["pm10"]),
            "nh3": float(pollution["components"]["nh3"]),
        }

        all_rows.append(row)

        # =================================================
        # LOGGING
        # =================================================

        if current.hour == 0:
            print(f"✅ Fetched: {current.strftime('%Y-%m-%d')}")

        # =================================================
        # NEXT HOUR
        # =================================================

        current += timedelta(hours=1)

        # avoid API throttling
        time.sleep(1)

    return pd.DataFrame(all_rows)

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
        description="Historical AQI + weather data for Skardu",
        online_enabled=True
    )

    fg.insert(
        df,
        write_options={
            "wait_for_job": False,
            "use_kafka": False
        }
    )

    print(f"✅ {len(df)} rows inserted into Hopsworks successfully")

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    end_date = datetime.now(timezone.utc)

    start_date = end_date - timedelta(days=180)

    print(
        f"🔄 Fetching historical data "
        f"from {start_date.date()} "
        f"to {end_date.date()}..."
    )

    df = fetch_historical_data(start_date, end_date)

    print(f"✅ Fetched {len(df)} rows total")

    print(df.head())

    print("🔄 Uploading to Hopsworks...")

    upload_to_hopsworks(df)

    print("✅ Historical backfill completed!")