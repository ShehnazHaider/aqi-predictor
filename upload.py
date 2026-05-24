import hopsworks
import pandas as pd
import os

# =========================================================
# LOAD ENV VARIABLES (GitHub Actions)
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
df["timestamp"] = pd.to_datetime(df["timestamp"])

print("✅ Dataset loaded:", len(df))

# =========================================================
# GET FEATURE GROUP (AUTO VERSIONING)
# =========================================================

fg = fs.get_or_create_feature_group(
    name="skardu_aqi_prediction",
    version=None,   # 👈 IMPORTANT: auto version increment
    primary_key=["timestamp"],
    event_time="timestamp",
    online_enabled=True
)

# =========================================================
# INSERT DATA (NEW VERSION AUTOMATICALLY CREATED)
# =========================================================

fg.insert(df)

print("✅ Upload complete")