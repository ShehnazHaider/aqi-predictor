import hopsworks
import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("HOPSWORKS_API_KEY")
HOST = os.getenv("HOPSWORKS_HOST")
PROJECT_NAME = os.getenv("HOPSWORKS_PROJECT_NAME")

# ── Login ──────────────────────────────────────────────────
project = hopsworks.login(
    api_key_value=API_KEY,
    project=PROJECT_NAME,
    host=HOST
)
fs = project.get_feature_store()
fg = fs.get_feature_group(name="aqi_prediction", version=1)

# We already know these from debug output — use them directly
PROJECT_ID = project.id        # 32041
FS_ID      = fs.id             # 20725
FG_ID      = fg.id             # 41747

print(f"PROJECT_ID : {PROJECT_ID}")
print(f"FS_ID      : {FS_ID}")
print(f"FG_ID      : {FG_ID}")

headers = {"X-Api-Key": API_KEY, "Content-Type": "application/json"}

# ── Test 1: Preview offline ────────────────────────────────
print("\n── Test 1: Preview offline ──")
r = requests.get(
    f"https://{HOST}/hopsworks-api/api/project/{PROJECT_ID}/featurestores/{FS_ID}/featuregroups/{FG_ID}/preview?storage=offline&n=5",
    headers=headers
)
print(f"Status : {r.status_code}")
print(f"Response: {r.text[:1000]}")

# ── Test 2: Preview online ─────────────────────────────────
print("\n── Test 2: Preview online ──")
r = requests.get(
    f"https://{HOST}/hopsworks-api/api/project/{PROJECT_ID}/featurestores/{FS_ID}/featuregroups/{FG_ID}/preview?storage=online&n=5",
    headers=headers
)
print(f"Status : {r.status_code}")
print(f"Response: {r.text[:1000]}")

# ── Test 3: fg.read() ──────────────────────────────────────
print("\n── Test 3: fg.read() ──")
try:
    df = fg.read()
    print(f"✅ Rows: {len(df)}")
    print(df.head())
except Exception as e:
    print(f"❌ {e}")

# ── Test 4: online read ────────────────────────────────────
print("\n── Test 4: fg.read(online=True) ──")
try:
    df = fg.read(online=True)
    print(f"✅ Rows: {len(df)}")
    print(df.head())
except Exception as e:
    print(f"❌ {e}")