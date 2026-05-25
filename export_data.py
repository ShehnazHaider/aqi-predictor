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

# Disable Arrow Flight entirely
df = feature_group.read(
    dataframe_type="pandas",
    read_options={
        "arrow_flight_config": {"disabled": True}
    }
)

print("✅ Data loaded")
df.to_csv("hopsworks_data.csv", index=False)
print(f"✅ Rows: {len(df)}")
print(df.head())