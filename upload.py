import hopsworks
import pandas as pd
import os

# =========================================================
# LOAD ENV VARIABLES
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

df = pd.read_csv("latest.csv")
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Add unique ID as primary key
df.insert(0, "id", range(1, len(df) + 1))

print(f"✅ Dataset loaded: {len(df)} rows")
print(f"   Columns: {df.columns.tolist()}")
print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")

# =========================================================
# CREATE FRESH FEATURE GROUP
# =========================================================

fg = fs.get_or_create_feature_group(
    name="aqi_predictionv2",
    version=1,
    primary_key=["id"],              # ✅ id as primary key
    event_time="timestamp",
    online_enabled=False,            # ✅ No online store issues
    description="Hourly AQI + weather data for Skardu (backfill)"
)

# =========================================================
# INSERT DATA
# =========================================================

fg.insert(df, write_options={"wait_for_job": True})
print(f"✅ Upload complete: {len(df)} rows inserted")

# =========================================================
# VERIFY
# =========================================================

df_check = fg.read()
print(f"✅ Verification: {len(df_check)} rows in feature group")
print(f"   Date range: {df_check['timestamp'].min()} to {df_check['timestamp'].max()}")