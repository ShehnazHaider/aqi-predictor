import hopsworks
import pandas as pd
import os

# =========================================================
# ENV VARIABLES
# =========================================================

api_key = os.getenv("HOPSWORKS_API_KEY")
project_name = os.getenv("HOPSWORKS_PROJECT_NAME")
host = os.getenv("HOPSWORKS_HOST")

# =========================================================
# LOGIN
# =========================================================

project = hopsworks.login(
    api_key_value=api_key,
    project=project_name,
    host=host
)

fs = project.get_feature_store()

# =========================================================
# LOAD CSV
# =========================================================

df = pd.read_csv("skardu_aqi_dataset.csv")

# Convert timestamp properly
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Create guaranteed unique ID
df["id"] = range(len(df))

print("✅ Dataset loaded:", len(df))

# =========================================================
# CREATE A BRAND NEW FEATURE GROUP
# IMPORTANT:
# - USE FIXED VERSION
# - DO NOT USE version=None
# =========================================================

fg = fs.get_or_create_feature_group(
    name="skardu_aqi_prediction_v2",
    version=2,
    primary_key=["id"],
    event_time="timestamp",
    description="AQI Prediction Dataset",
    online_enabled=False
)

print("✅ Feature group ready")

# =========================================================
# INSERT DATA
# IMPORTANT: wait_for_job=True
# =========================================================

fg.insert(df, wait=True)

print("✅ Upload complete")

# =========================================================
# TEST READ IMMEDIATELY
# =========================================================

test_df = fg.read()

print("✅ Read successful")
print(test_df.head())
print("Rows:", len(test_df))