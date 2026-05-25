# export_hopsworks_data.py

import hopsworks
import pandas as pd
import os

# Login using environment variable
project = hopsworks.login(api_key_value=os.getenv("HOPSWORKS_API_KEY"))

# Access the Feature Store
fs = project.get_feature_store()

# Load your feature group
feature_group = fs.get_feature_group(name="aqi_prediction", version=1)

# Read data into pandas DataFrame
df = feature_group.read()

# Save as CSV
csv_path = "hopsworks_data.csv"
df.to_csv(csv_path, index=False)

print(f"✅ Data saved to {csv_path}")