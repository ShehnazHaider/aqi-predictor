import hopsworks
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Login
project = hopsworks.login(
    api_key_value=os.getenv("HOPSWORKS_API_KEY"),
    project=os.getenv("HOPSWORKS_PROJECT_NAME"),
    host=os.getenv("HOPSWORKS_HOST")
)

print("✅ Connected to Hopsworks")

# Feature Store
fs = project.get_feature_store()

# Get Feature Group
feature_group = fs.get_feature_group(
    name="skardu_aqi_prediction",
    version=2   
)

print("✅ Feature group found")

# Read data
query = feature_group.select_all()

df = query.read()

print("✅ Data loaded")

# Save CSV
csv_path = "hopsworks_data.csv"

df.to_csv(csv_path, index=False)

print(f"✅ CSV saved: {csv_path}")
print(f"✅ Rows: {len(df)}")
print(df.head())