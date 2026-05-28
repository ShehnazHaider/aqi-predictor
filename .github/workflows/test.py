import hopsworks
import os

project = hopsworks.login(api_key_value=os.environ["HOPSWORKS_API_KEY"])
fs = project.get_feature_store()

fg = fs.get_feature_group(name="aqi_prediction", version=1)
df = fg.read()

print(f"Total rows: {len(df)}")
print(f"First timestamp: {df['timestamp'].min()}")
print(f"Last timestamp: {df['timestamp'].max()}")
print(f"Unique dates: {df['timestamp'].dt.date.nunique()}")
print(f"First 5 rows:\n{df.head()}")