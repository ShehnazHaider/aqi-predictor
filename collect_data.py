import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import hopsworks
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
HOPSWORKS_API_KEY = os.getenv("HOPSWORKS_API_KEY")
HOPSWORKS_PROJECT = os.getenv("HOPSWORKS_PROJECT_NAME")
HOPSWORKS_HOST = os.getenv("HOPSWORKS_HOST")

LAT = 35.29  # Skardu
LON = 75.63

def fetch_historical_data(start_date, end_date):
    """Fetch historical pollution + weather data between two dates"""
    all_rows = []
    current = start_date

    while current <= end_date:
        timestamp = int(current.timestamp())

        # Fetch historical pollution
        pollution_url = (
            f"https://api.openweathermap.org/data/2.5/air_pollution/history"
            f"?lat={LAT}&lon={LON}&start={timestamp}&end={timestamp+3600}&appid={API_KEY}"
        )
        pollution_resp = requests.get(pollution_url).json()

        if not pollution_resp.get("list"):
            current += timedelta(hours=1)
            continue

        pollution = pollution_resp["list"][0]

        # Fetch historical weather
        weather_url = (
            f"https://api.openweathermap.org/data/3.0/onecall/timemachine"
            f"?lat={LAT}&lon={LON}&dt={timestamp}&appid={API_KEY}&units=metric"
        )
        weather_resp = requests.get(weather_url).json()

        # Extract weather safely
        try:
            weather_data = weather_resp["data"][0]
            temperature = weather_data.get("temp", 0)
            humidity = weather_data.get("humidity", 0)
            wind_speed = weather_data.get("wind_speed", 0)
        except (KeyError, IndexError):
            temperature, humidity, wind_speed = 0, 0, 0

        row = {
            "timestamp_str":  str(current),
            "hour":           current.hour,
            "day_of_week":    current.weekday(),
            "month":          current.month,
            "is_weekend":     int(current.weekday() >= 5),
            "temperature":    float(temperature),
            "humidity":       float(humidity),
            "wind_speed":     float(wind_speed),
            "aqi_index":      int(pollution["main"]["aqi"]),
            "co":             float(pollution["components"]["co"]),
            "no2":            float(pollution["components"]["no2"]),
            "o3":             float(pollution["components"]["o3"]),
            "so2":            float(pollution["components"]["so2"]),
            "pm2_5":          float(pollution["components"]["pm2_5"]),
            "pm10":           float(pollution["components"]["pm10"]),
            "nh3":            float(pollution["components"]["nh3"]),
        }
        all_rows.append(row)

        # Progress update every 24 hours
        if current.hour == 0:
            print(f"✅ Fetched: {current.strftime('%Y-%m-%d')}")

        current += timedelta(hours=1)

    return pd.DataFrame(all_rows)


def upload_to_hopsworks(df):
    """Upload historical data to Hopsworks feature store"""
    project = hopsworks.login(
        api_key_value=HOPSWORKS_API_KEY,
        project=HOPSWORKS_PROJECT,
        host=HOPSWORKS_HOST
    )
    fs = project.get_feature_store()
    fg = fs.get_or_create_feature_group(
        name="aqi_features_skardu",
        version=1,
        primary_key=["timestamp_str"],
        description="Hourly AQI + weather data for Skardu",
        online_enabled=True
    )
    fg.insert(df, write_options={"wait_for_job": False, "use_kafka": False})
    print(f"✅ {len(df)} rows inserted into Hopsworks successfully")


if __name__ == "__main__":
    # Fetch last 6 months of data
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=180)

    print(f"🔄 Fetching historical data from {start_date.date()} to {end_date.date()}...")
    print(f"   This will fetch ~{180*24} hourly records — may take a few minutes...")

    df = fetch_historical_data(start_date, end_date)
    print(f"✅ Fetched {len(df)} rows total")
    print(df.head())

    print("🔄 Uploading to Hopsworks...")
    upload_to_hopsworks(df)
    print("✅ Backfill completed!")