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

# Access Feature Store
fs = project.get_feature_store()

# Load your feature group
feature_group = fs.get_feature_group(name="skardu_aqi_prediction", version=1)

# Read data into pandas DataFrame
df = feature_group.read()

# Save as CSV
csv_path = "hopsworks_data.csv"
df.to_csv(csv_path, index=False)

print(f"✅ Data saved to {csv_path}")
print(f"✅ Total rows: {len(df)}")
print(df.head())