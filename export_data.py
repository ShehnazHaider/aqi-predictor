import hopsworks
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

project = hopsworks.login(
    api_key_value=os.getenv("HOPSWORKS_API_KEY"),
    project=os.getenv("HOPSWORKS_PROJECT_NAME"),
    host=os.getenv("HOPSWORKS_HOST")
)
print("✅ Connected to Hopsworks")

fs = project.get_feature_store()

feature_group = fs.get_feature_group(name="aqi_prediction", version=1)
print("✅ Feature group found")

# Force Hive reader — bypasses Arrow Flight gRPC which fails on fresh data
df = feature_group.read(read_options={"use_hive": True})

print("✅ Data loaded")

csv_path = "hopsworks_data.csv"
df.to_csv(csv_path, index=False)

print(f"✅ CSV saved: {csv_path}")
print(f"✅ Rows: {len(df)}")
print(df.head())