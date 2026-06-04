

# =========================================================
# LOCATION — SKARDU
# =========================================================

LAT = 35.29
LON = 75.63

# =========================================================
# DATE RANGE
# =========================================================

START_DATE = "2025-11-16"
END_DATE   = "2026-05-15"

# =========================================================
# OPEN-METEO HISTORICAL API
# =========================================================

url = (
    f"https://archive-api.open-meteo.com/v1/archive?"
    f"latitude={LAT}"
    f"&longitude={LON}"
    f"&start_date={START_DATE}"
    f"&end_date={END_DATE}"
    f"&hourly="
    f"temperature_2m,"
    f"relative_humidity_2m,"
    f"wind_speed_10m"
    f"&timezone=UTC"
)

# =========================================================
# FETCH DATA
# =========================================================

response = requests.get(url)

print("Status Code:", response.status_code)

data = response.json()

# =========================================================
# CREATE DATAFRAME
# =========================================================

hourly = data["hourly"]

df = pd.DataFrame({
    "timestamp": hourly["time"],
    "temperature": hourly["temperature_2m"],
    "humidity": hourly["relative_humidity_2m"],
    "wind_speed": hourly["wind_speed_10m"]
})

# =========================================================
# CONVERT TIMESTAMP
# =========================================================

df["timestamp"] = pd.to_datetime(df["timestamp"])

# =========================================================
# SHOW DATA
# =========================================================

print(df.head())

print("\n✅ Total Rows:", len(df))

print("\nMin Timestamp:", df["timestamp"].min())
print("Max Timestamp:", df["timestamp"].max())

# =========================================================
# SAVE CSV
# =========================================================

df.to_csv("historical_weather_data.csv", index=False)

print("\n✅ CSV saved successfully")

# =========================================================
#  HISTORICAL OPENWEATHER POLLUTION DATA
# =========================================================

!pip install pandas requests -q

import requests
import pandas as pd
from datetime import datetime

# =========================================================
# API KEY
# =========================================================

API_KEY = "ceec537ffad9dd2cf83f337158e2bd7d"

# =========================================================
# LOCATION — SKARDU
# =========================================================

LAT = 35.29
LON = 75.63

# =========================================================
# DATE RANGE
# =========================================================

START_DATE = datetime(2025, 11, 16, 22, 20, 34)
END_DATE   = datetime(2026, 5, 15, 21, 13, 41)

start_ts = int(START_DATE.timestamp())
end_ts   = int(END_DATE.timestamp())

# =========================================================
# SINGLE BULK API REQUEST
# =========================================================

url = (
    f"https://api.openweathermap.org/data/2.5/air_pollution/history?"
    f"lat={LAT}"
    f"&lon={LON}"
    f"&start={start_ts}"
    f"&end={end_ts}"
    f"&appid={API_KEY}"
)

response = requests.get(url)

print("Status Code:", response.status_code)

data = response.json()

# =========================================================
# CONVERT TO DATAFRAME
# =========================================================

rows = []

for item in data["list"]:

    row = {

        "timestamp": pd.to_datetime(
            item["dt"],
            unit="s"
        ),

        "aqi_index": item["main"]["aqi"],

        "co": item["components"]["co"],

        "no2": item["components"]["no2"],

        "o3": item["components"]["o3"],

        "so2": item["components"]["so2"],

        "pm2_5": item["components"]["pm2_5"],

        "pm10": item["components"]["pm10"],

        "nh3": item["components"]["nh3"],
    }

    rows.append(row)

# =========================================================
# DATAFRAME
# =========================================================

df = pd.DataFrame(rows)

# =========================================================
# SHOW DATA
# =========================================================

print(df.head())

print("\n✅ Total Rows:", len(df))

print("\nMin Timestamp:", df["timestamp"].min())
print("Max Timestamp:", df["timestamp"].max())

# =========================================================
# SAVE CSV
# =========================================================

df.to_csv(
    "historical_pollution_data.csv",
    index=False
)

print("\n✅ CSV saved successfully")

# =========================================================
# MERGE WEATHER + POLLUTION DATASETS
# GOOGLE COLAB READY
# =========================================================

!pip install pandas -q

import pandas as pd
from google.colab import files



# =========================================================
# FILE NAMES
# =========================================================

weather_file = "/content/historical_weather_data.csv"

pollution_file = "/content/historical_pollution_data.csv"

# =========================================================
# LOAD DATASETS
# =========================================================

weather_df = pd.read_csv(weather_file)

pollution_df = pd.read_csv(pollution_file)

# =========================================================
# CONVERT TIMESTAMPS
# =========================================================

weather_df["timestamp"] = pd.to_datetime(
    weather_df["timestamp"]
).dt.floor("H")

pollution_df["timestamp"] = pd.to_datetime(
    pollution_df["timestamp"]
).dt.floor("H")

# =========================================================
# MERGE DATASETS
# =========================================================

merged_df = pd.merge(
    pollution_df,
    weather_df,
    on="timestamp",
    how="inner"
)

# =========================================================
# SORT BY TIME
# =========================================================

merged_df = merged_df.sort_values(
    by="timestamp"
).reset_index(drop=True)

# =========================================================
# SHOW RESULTS
# =========================================================

print("✅ Weather Rows:", len(weather_df))

print("✅ Pollution Rows:", len(pollution_df))

print("✅ Final Merged Rows:", len(merged_df))

print("\nMin Timestamp:",
      merged_df["timestamp"].min())

print("Max Timestamp:",
      merged_df["timestamp"].max())

print("\nColumns:")
print(merged_df.columns)

print("\nPreview:")
print(merged_df.head())

# =========================================================
# SAVE FINAL DATASET
# =========================================================

output_file = "final_aqi_dataset.csv"

merged_df.to_csv(output_file, index=False)

print("\n✅ Final dataset saved successfully")

# =========================================================
# DOWNLOAD
# =========================================================

files.download(output_file)