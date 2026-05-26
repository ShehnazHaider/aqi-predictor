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

fs = project.get_feature_store()
fg = fs.get_feature_group(name="aqi_prediction", version=1)

df = fg.read(online=True)  # 👈 this is the only change

print(f"✅ Rows: {len(df)}")

df.to_csv("hopsworks_data.csv", index=False)
print("✅ CSV saved")
print(df.head())