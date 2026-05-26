import hopsworks
import requests
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("HOPSWORKS_API_KEY")
HOST = os.getenv("HOPSWORKS_HOST")
PROJECT_NAME = os.getenv("HOPSWORKS_PROJECT_NAME")

print("="*60)
print("ENV CHECK")
print("="*60)
print(f"HOST: {HOST}")
print(f"PROJECT: {PROJECT_NAME}")
print(f"API_KEY set: {'Yes' if API_KEY else 'NO - MISSING!'}")

# ── Step 1: Login ──────────────────────────────────────────
print("\n" + "="*60)
print("STEP 1: LOGIN")
print("="*60)
project = hopsworks.login(
    api_key_value=API_KEY,
    project=PROJECT_NAME,
    host=HOST
)
print(f"✅ Logged in | Project: {project.name} | ID: {project.id}")

# ── Step 2: Feature Store ──────────────────────────────────
print("\n" + "="*60)
print("STEP 2: FEATURE STORE")
print("="*60)
fs = project.get_feature_store()
print(f"✅ Feature store: {fs.name} | ID: {fs.id}")

# ── Step 3: Feature Group metadata ────────────────────────
print("\n" + "="*60)
print("STEP 3: FEATURE GROUP METADATA")
print("="*60)
fg = fs.get_feature_group(name="aqi_prediction", version=1)
print(f"✅ Found: {fg.name} v{fg.version}")
print(f"   ID            : {fg.id}")
print(f"   Location      : {fg.location}")
print(f"   Online enabled: {fg.online_enabled}")
print(f"   Features      : {[f.name for f in fg.features]}")

# ── Step 4: REST API raw check ─────────────────────────────
print("\n" + "="*60)
print("STEP 4: REST API RAW CHECK")
print("="*60)
headers = {"X-Api-Key": API_KEY, "Content-Type": "application/json"}

# Get project ID
r = requests.get(
    f"https://{HOST}/hopsworks-api/api/projects/getProjectInfo/{PROJECT_NAME}",
    headers=headers
)
print(f"Project info status: {r.status_code}")
project_id = r.json()["projectId"]
print(f"Project ID: {project_id}")

# Get feature store ID
r = requests.get(
    f"https://{HOST}/hopsworks-api/api/project/{project_id}/featurestores",
    headers=headers
)
print(f"Feature store status: {r.status_code}")
fs_id = r.json()[0]["featurestoreId"]
print(f"Feature store ID: {fs_id}")

# Get feature group details
r = requests.get(
    f"https://{HOST}/hopsworks-api/api/project/{project_id}/featurestores/{fs_id}/featuregroups?name=aqi_prediction&version=1",
    headers=headers
)
print(f"Feature group status : {r.status_code}")
fg_data = r.json()[0]
print(f"Feature group raw response:")
for k, v in fg_data.items():
    print(f"   {k}: {v}")

fg_id = fg_data["id"]

# ── Step 5: Preview via REST ───────────────────────────────
print("\n" + "="*60)
print("STEP 5: PREVIEW VIA REST API")
print("="*60)
r = requests.get(
    f"https://{HOST}/hopsworks-api/api/project/{project_id}/featurestores/{fs_id}/featuregroups/{fg_id}/preview?storage=offline&n=5",
    headers=headers
)
print(f"Preview status: {r.status_code}")
print(f"Preview raw response: {r.text[:500]}")  # first 500 chars

# ── Step 6: Try read with hsfs ─────────────────────────────
print("\n" + "="*60)
print("STEP 6: HSFS READ ATTEMPTS")
print("="*60)

# Attempt A
print("\n── Attempt A: feature_group.read() ──")
try:
    df = fg.read()
    print(f"✅ Success! Rows: {len(df)}")
except Exception as e:
    print(f"❌ Failed: {e}")

# Attempt B
print("\n── Attempt B: feature_group.read(online=True) ──")
try:
    df = fg.read(online=True)
    print(f"✅ Success! Rows: {len(df)}")
except Exception as e:
    print(f"❌ Failed: {e}")

# Attempt C
print("\n── Attempt C: select_all().read() ──")
try:
    df = fg.select_all().read()
    print(f"✅ Success! Rows: {len(df)}")
except Exception as e:
    print(f"❌ Failed: {e}")

# Attempt D: Arrow Flight explicitly disabled
print("\n── Attempt D: arrow_flight disabled ──")
try:
    df = fg.read(
        dataframe_type="pandas",
        read_options={"arrow_flight_config": {"disabled": True}}
    )
    print(f"✅ Success! Rows: {len(df)}")
except Exception as e:
    print(f"❌ Failed: {e}")

print("\n" + "="*60)
print("DEBUG COMPLETE — paste full output here")
print("="*60)