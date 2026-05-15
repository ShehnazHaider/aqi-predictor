import os
import requests
import pandas as pd
from datetime import datetime
import hopsworks
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
HOPSWORKS_API_KEY = os.getenv("HOPSWORKS_API_KEY")
HOPSWORKS_PROJECT = os.getenv("HOPSWORKS_PROJECT_NAME")
HOPSWORKS_HOST = os.getenv("HOPSWORKS_HOST")

LAT = 35.29  # Skardu
LON = 75.63

def fetch_data():
    pollution_url = f"https://api.openweathermap.org/data/2.5/air_pollution?lat={LAT}&lon={LON}&appid={API_KEY}"
    pollution = requests.get(pollution_url).json()["list"][0]

    weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"
    weather = requests.get(weather_url).json()

    now = datetime.utcnow()

    return pd.DataFrame([{
        "timestamp_str":  str(now),
        "temperature":    float(weather["main"]["temp"]),
        "humidity":       int(weather["main"]["humidity"]),
        "wind_speed":     float(weather["wind"]["speed"]),
        "aqi_index":      int(pollution["main"]["aqi"]),
        "co":             float(pollution["components"]["co"]),
        "no2":            float(pollution["components"]["no2"]),
        "o3":             float(pollution["components"]["o3"]),
        "so2":            float(pollution["components"]["so2"]),
        "pm2_5":          float(pollution["components"]["pm2_5"]),
        "pm10":           float(pollution["components"]["pm10"]),
        "nh3":            float(pollution["components"]["nh3"]),
    }])

def upload_to_hopsworks(df):
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
    print("✅ Data inserted into Hopsworks successfully")

if __name__ == "__main__":
    print("🔄 Fetching data...")
    df = fetch_data()
    print(df)
    print("🔄 Uploading to Hopsworks...")
    upload_to_hopsworks(df)
    print("✅ Pipeline completed!")