import os
import requests
import pandas as pd
from datetime import datetime, timezone
import hopsworks
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
HOPSWORKS_API_KEY = os.getenv("HOPSWORKS_API_KEY")
HOPSWORKS_PROJECT = os.getenv("HOPSWORKS_PROJECT_NAME")
HOPSWORKS_HOST = os.getenv("HOPSWORKS_HOST")

LAT = 35.29
LON = 75.63

def fetch_data():
    pollution_url = f"https://api.openweathermap.org/data/2.5/air_pollution?lat={LAT}&lon={LON}&appid={API_KEY}"
    pollution = requests.get(pollution_url).json()["list"][0]

    weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"
    weather = requests.get(weather_url).json()

    now = pd.Timestamp.utcnow()

    return pd.DataFrame([{
        "timestamp":   now,
        "temperature": float(weather["main"]["temp"]),
        "humidity":    int(weather["main"]["humidity"]),
        "wind_speed":  float(weather["wind"]["speed"]),
        "aqi_index":   int(pollution["main"]["aqi"]),
        "co":          float(pollution["components"]["co"]),
        "no2":         float(pollution["components"]["no2"]),
        "o3":          float(pollution["components"]["o3"]),
        "so2":         float(pollution["components"]["so2"]),
        "pm2_5":       float(pollution["components"]["pm2_5"]),
        "pm10":        float(pollution["components"]["pm10"]),
        "nh3":         float(pollution["components"]["nh3"]),
    }])

def upload_to_hopsworks(df):
    project = hopsworks.login(
        api_key_value=HOPSWORKS_API_KEY,
        project=HOPSWORKS_PROJECT,
        host=HOPSWORKS_HOST
    )
    fs = project.get_feature_store()

    fg = fs.get_feature_group(name="aqi_predictionv2", version=1)
    
    # Read existing data to get the last id
    try:
        existing = fg.read()
        last_id = existing["id"].max() if len(existing) > 0 else 0
    except:
        last_id = 0
    
    # Assign new id
    df.insert(0, "id", last_id + 1)

    fg.insert(df, write_options={"wait_for_job": False})
    print(f"✅ Data inserted: id={df['id'].iloc[0]}")

if __name__ == "__main__":
    print("🔄 Fetching data...")
    df = fetch_data()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    print(df.dtypes)
    
    print("🔄 Uploading to Hopsworks...")
    upload_to_hopsworks(df)
    print("✅ Pipeline completed!")