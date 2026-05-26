import hopsworks
import pandas as pd
import requests
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("HOPSWORKS_API_KEY")
HOST = os.getenv("HOPSWORKS_HOST")
PROJECT_NAME = os.getenv("HOPSWORKS_PROJECT_NAME")

# Step 1: Get project ID
headers = {
    "X-Api-Key": API_KEY,
    "Content-Type": "application/json"
}

projects_resp = requests.get(
    f"https://{HOST}/hopsworks-api/api/projects/getProjectInfo/{PROJECT_NAME}",
    headers=headers
)
project_id = projects_resp.json()["projectId"]
print(f"✅ Project ID: {project_id}")

# Step 2: Get feature store ID
fs_resp = requests.get(
    f"https://{HOST}/hopsworks-api/api/project/{project_id}/featurestores",
    headers=headers
)
fs_id = fs_resp.json()[0]["featurestoreId"]
print(f"✅ Feature store ID: {fs_id}")

# Step 3: Get feature group ID
fg_resp = requests.get(
    f"https://{HOST}/hopsworks-api/api/project/{project_id}/featurestores/{fs_id}/featuregroups?name=aqi_prediction&version=1",
    headers=headers
)
fg_id = fg_resp.json()[0]["id"]
print(f"✅ Feature group ID: {fg_id}")

# Step 4: Preview data (up to 1000 rows)
preview_resp = requests.get(
    f"https://{HOST}/hopsworks-api/api/project/{project_id}/featurestores/{fs_id}/featuregroups/{fg_id}/preview?storage=offline&n=10000",
    headers=headers
)

data = preview_resp.json()
columns = [col["name"] for col in data["columns"]]
rows = data["rows"]

df = pd.DataFrame(rows, columns=columns)
print(f"✅ Data loaded: {len(df)} rows")

df.to_csv("hopsworks_data.csv", index=False)
print("✅ CSV saved")
print(df.head())