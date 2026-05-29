import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import hopsworks
import os

# ==================== Connect to Hopsworks ==========================
project = hopsworks.login(api_key_value=os.environ["HOPSWORKS_API_KEY"])
fs = project.get_feature_store()

# === Get Raw Data from Hopsworks ===
raw_fg = fs.get_feature_group(name="aqi_predictionv2", version=1)
df = raw_fg.read()
print(f"✅ Raw data loaded from Hopsworks: {len(df)} rows")
print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")

# ==================== Feature Engineering ===========================
df.columns = df.columns.str.strip().str.lower()

# Remove exact duplicates (except timestamp and id)
dup_cols = [col for col in df.columns if col not in ["timestamp", "id"]]
df = df.drop_duplicates(subset=dup_cols)

# Convert timestamp
df["timestamp"] = pd.to_datetime(df["timestamp"])

# ==================== AQI Breakpoints (US EPA standard) ====================
breakpoints = {
    "pm2_5": [
        {"low": 0.0, "high": 12.0, "aqi_low": 0, "aqi_high": 50},
        {"low": 12.1, "high": 35.4, "aqi_low": 51, "aqi_high": 100},
        {"low": 35.5, "high": 55.4, "aqi_low": 101, "aqi_high": 150},
        {"low": 55.5, "high": 150.4, "aqi_low": 151, "aqi_high": 200},
        {"low": 150.5, "high": 250.4, "aqi_low": 201, "aqi_high": 300},
        {"low": 250.5, "high": 350.4, "aqi_low": 301, "aqi_high": 400},
        {"low": 350.5, "high": 500.4, "aqi_low": 401, "aqi_high": 500}
    ],
    "pm10": [
        {"low": 0, "high": 54, "aqi_low": 0, "aqi_high": 50},
        {"low": 55, "high": 154, "aqi_low": 51, "aqi_high": 100},
        {"low": 155, "high": 254, "aqi_low": 101, "aqi_high": 150},
        {"low": 255, "high": 354, "aqi_low": 151, "aqi_high": 200},
        {"low": 355, "high": 424, "aqi_low": 201, "aqi_high": 300},
        {"low": 425, "high": 504, "aqi_low": 301, "aqi_high": 400},
        {"low": 505, "high": 604, "aqi_low": 401, "aqi_high": 500}
    ],
    "co": [
        {"low": 0.0, "high": 4.4, "aqi_low": 0, "aqi_high": 50},
        {"low": 4.5, "high": 9.4, "aqi_low": 51, "aqi_high": 100},
        {"low": 9.5, "high": 12.4, "aqi_low": 101, "aqi_high": 150},
        {"low": 12.5, "high": 15.4, "aqi_low": 151, "aqi_high": 200},
        {"low": 15.5, "high": 30.4, "aqi_low": 201, "aqi_high": 300},
        {"low": 30.5, "high": 40.4, "aqi_low": 301, "aqi_high": 400},
        {"low": 40.5, "high": 50.4, "aqi_low": 401, "aqi_high": 500}
    ],
    "no2": [
        {"low": 0, "high": 53, "aqi_low": 0, "aqi_high": 50},
        {"low": 54, "high": 100, "aqi_low": 51, "aqi_high": 100},
        {"low": 101, "high": 360, "aqi_low": 101, "aqi_high": 150},
        {"low": 361, "high": 649, "aqi_low": 151, "aqi_high": 200},
        {"low": 650, "high": 1249, "aqi_low": 201, "aqi_high": 300},
        {"low": 1250, "high": 1649, "aqi_low": 301, "aqi_high": 400},
        {"low": 1650, "high": 2049, "aqi_low": 401, "aqi_high": 500}
    ],
    "so2": [
        {"low": 0, "high": 35, "aqi_low": 0, "aqi_high": 50},
        {"low": 36, "high": 75, "aqi_low": 51, "aqi_high": 100},
        {"low": 76, "high": 185, "aqi_low": 101, "aqi_high": 150},
        {"low": 186, "high": 304, "aqi_low": 151, "aqi_high": 200},
        {"low": 305, "high": 604, "aqi_low": 201, "aqi_high": 300},
        {"low": 605, "high": 804, "aqi_low": 301, "aqi_high": 400},
        {"low": 805, "high": 1004, "aqi_low": 401, "aqi_high": 500}
    ],
    "o3": [
        {"low": 0, "high": 54, "aqi_low": 0, "aqi_high": 50},
        {"low": 55, "high": 70, "aqi_low": 51, "aqi_high": 100},
        {"low": 71, "high": 85, "aqi_low": 101, "aqi_high": 150},
        {"low": 86, "high": 105, "aqi_low": 151, "aqi_high": 200},
        {"low": 106, "high": 200, "aqi_low": 201, "aqi_high": 300}
    ]
}

# ==================== AQI Calculation ====================
def calculate_aqi(concentration, bps):
    for bp in bps:
        if bp["low"] <= concentration <= bp["high"]:
            return ((bp["aqi_high"] - bp["aqi_low"]) / (bp["high"] - bp["low"])) * (concentration - bp["low"]) + bp["aqi_low"]
    return None

def calculate_row_aqi(row):
    max_aqi = None
    for pollutant, bps in breakpoints.items():
        if pollutant in row and pd.notnull(row[pollutant]):
            aqi = calculate_aqi(row[pollutant], bps)
            if aqi is not None and (max_aqi is None or aqi > max_aqi):
                max_aqi = aqi
    return max_aqi

df["calculated_aqi"] = df.apply(calculate_row_aqi, axis=1).round(2)

# ==================== ✅ HOURLY (keep all rows, extract time features) ====================
hourly = df.copy()
hourly = hourly.sort_values("timestamp").reset_index(drop=True)

# Extract time features from timestamp
hourly["hour"] = hourly["timestamp"].dt.hour
hourly["day"] = hourly["timestamp"].dt.day
hourly["month"] = hourly["timestamp"].dt.month

print(f"✅ Hourly data kept: {len(hourly)} rows")

# ==================== Log Transforms ====================
hourly["co_log"] = np.log1p(hourly["co"])
hourly["so2_log"] = np.log1p(hourly["so2"])
hourly["nh3_log"] = np.log1p(hourly["nh3"])

# ==================== AQI Change Rate ====================
hourly = hourly.sort_values("timestamp").reset_index(drop=True)
hourly["aqi_change_rate"] = hourly["calculated_aqi"].diff().fillna(0).round(2)

# ==================== ✅ 3-Day Forecast Targets (hourly: shift by 24/48/72) ====================
hourly["aqi_day1"] = hourly["calculated_aqi"].shift(-24)
hourly["aqi_day2"] = hourly["calculated_aqi"].shift(-48)
hourly["aqi_day3"] = hourly["calculated_aqi"].shift(-72)

# Drop rows where targets don't exist
hourly = hourly.dropna(subset=["aqi_day1", "aqi_day2", "aqi_day3"])

print(f"✅ Targets created")
print(f"   Rows after dropping NaN targets: {len(hourly)}")

# ==================== Add Unique ID ====================
hourly = hourly.drop(columns=["id"], errors="ignore")
hourly.insert(0, "id", range(1, len(hourly) + 1))

# ==================== Feature Scaling ====================
features_to_scale = [
    "pm2_5", "pm10", "co_log", "no2", "o3",
    "so2_log", "nh3_log",
    "temperature", "humidity", "wind_speed",
    "hour", "day", "month",
    "aqi_change_rate"
]

scaler = MinMaxScaler()
scaled = scaler.fit_transform(hourly[features_to_scale])
scaled_df = pd.DataFrame(
    scaled,
    columns=[f"{col}_scaled" for col in features_to_scale]
)

df_final = pd.concat([hourly.drop(columns=features_to_scale), scaled_df], axis=1)

# ==================== Winsorization ====================
def cap_outliers(df, col):
    if col in df.columns:
        lower = df[col].quantile(0.01)
        upper = df[col].quantile(0.99)
        df[col] = np.clip(df[col], lower, upper)
    return df

outlier_cols = [
    "pm2_5_scaled", "pm10_scaled", "o3_scaled",
    "so2_log_scaled", "nh3_log_scaled", "co_log_scaled"
]

for col in outlier_cols:
    df_final = cap_outliers(df_final, col)

print(f"✅ Outliers capped for {len(outlier_cols)} columns")

# ==================== Store in Hopsworks ====================
# Delete old processed feature group if exists
try:
    old_fg = fs.get_feature_group(name="processed_aqi_skardu_v2", version=1)
    old_fg.delete()
    print("✅ Old processed feature group deleted")
except:
    pass

processed_fg = fs.get_or_create_feature_group(
    name="processed_aqi_skardu_v2",
    version=1,
    primary_key=["id"],
    online_enabled=False,
    description="Hourly AQI data with 3-day forecast targets (24h/48h/72h shifts)"
)

processed_fg.insert(df_final, write_options={"wait_for_job": True})
print(f"✅ Processed data stored in Hopsworks feature group")
print(f"   Total hourly rows: {len(df_final)}")
print(f"   Feature columns: {[c for c in df_final.columns if '_scaled' in c]}")
print(f"   Target columns: aqi_day1, aqi_day2, aqi_day3")
print(f"   Date range: {df_final['timestamp'].min()} to {df_final['timestamp'].max()}")