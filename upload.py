import os
import pandas as pd
import hopsworks

# =========================
# LOAD CSV
# =========================

df = pd.read_csv("skardu_aqi_dataset.csv")
df["timestamp"] = pd.to_datetime(df["timestamp"])

print("Dataset loaded:", len(df))

# =========================
# ENV VARIABLES
# =========================

api_key = os.getenv("HOPSWORKS_API_KEY")
project_name = os.getenv("HOPSWORKS_PROJECT_NAME")
host = os.getenv("HOPSWORKS_HOST")

# =========================
# CONNECT
# =========================

project = hopsworks.login(
    api_key_value=api_key,
    project=project_name,
    host=host
)

fs = project.get_feature_store()

# =========================
# FEATURE GROUP
# =========================

fg = fs.get_or_create_feature_group(
    name="skardu_aqi_prediction",
    version=1,
    primary_key=["timestamp"],
    event_time="timestamp",
    online_enabled=True
)

# =========================
# UPLOAD
# =========================

fg.insert(df)

print("Upload complete")