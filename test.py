import hopsworks
import os
import pandas as pd

# ==================== Connect ====================
project = hopsworks.login(api_key_value=os.environ["HOPSWORKS_API_KEY"])
fs = project.get_feature_store()

fg = fs.get_feature_group(name="aqi_prediction", version=1)

# ==================== Debug ====================
print("=" * 60)
print("🔍 DEBUG: aqi_prediction Feature Group")
print("=" * 60)

# 1. Feature group metadata
print(f"\n📋 FG Name: {fg.name}")
print(f"📋 FG Version: {fg.version}")
print(f"📋 Online Enabled: {fg.online_enabled}")
print(f"📋 Primary Key(s): {fg.primary_key}")

# 2. Try online read
print("\n--- Online Store Read ---")
try:
    df_online = fg.read()
    print(f"Rows: {len(df_online)}")
    print(f"Columns: {df_online.columns.tolist()}")
    if len(df_online) > 0:
        print(f"Timestamp range: {df_online['timestamp'].min()} → {df_online['timestamp'].max()}")
        print(f"First row:\n{df_online.iloc[0].to_dict()}")
except Exception as e:
    print(f"❌ Online read failed: {e}")

# 3. Try offline read
print("\n--- Offline Store Read ---")
try:
    df_offline = fg.read(read_options={"use_hive": True})
    print(f"Rows: {len(df_offline)}")
    print(f"Columns: {df_offline.columns.tolist()}")
    if len(df_offline) > 0:
        print(f"Timestamp range: {df_offline['timestamp'].min()} → {df_offline['timestamp'].max()}")
        print(f"Unique dates: {pd.to_datetime(df_offline['timestamp']).dt.date.nunique()}")
        print(f"First row:\n{df_offline.iloc[0].to_dict()}")
        print(f"Last row:\n{df_offline.iloc[-1].to_dict()}")
except Exception as e:
    print(f"❌ Offline read failed: {e}")

# 4. Try SQL-style query
print("\n--- SQL Query Read ---")
try:
    query = fg.select_all()
    df_query = query.read()
    print(f"Rows via query: {len(df_query)}")
except Exception as e:
    print(f"❌ Query read failed: {e}")

# 5. List all feature groups
print("\n--- All Feature Groups ---")
for f in fs.get_feature_groups():
    print(f"  - {f.name} (v{f.version})")

print("\n" + "=" * 60)
print("✅ Debug complete")
print("=" * 60)