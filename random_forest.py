import os
import pandas as pd
import numpy as np
import hopsworks
import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import json
from datetime import datetime

# ==================== Load Data ====================
project = hopsworks.login(api_key_value=os.environ["HOPSWORKS_API_KEY"])
fs = project.get_feature_store()
mr = project.get_model_registry()

fg = fs.get_feature_group(name="processed_aqi_skardu", version=1)
df = fg.read()
df = df.sort_values("date").reset_index(drop=True)

# ==================== Features & Targets ====================
feature_cols = [
    "pm2_5_scaled", "pm10_scaled", "co_log_scaled", "no2_scaled",
    "o3_scaled", "so2_log_scaled", "nh3_log_scaled",
    "temperature_scaled", "humidity_scaled", "wind_speed_scaled",
    "day_scaled", "month_scaled", "aqi_change_rate_scaled"
]

target_cols = ["aqi_day1", "aqi_day2", "aqi_day3"]

df_model = df.dropna(subset=target_cols).copy()

X = df_model[feature_cols].values
y = df_model[target_cols].values

split_idx = int(len(X) * 0.8)
X_train, X_test = X[:split_idx], X[split_idx:]
y_train, y_test = y[:split_idx], y[split_idx:]

# ==================== Train ====================
model = RandomForestRegressor(
    n_estimators=200,
    max_depth=10,
    min_samples_split=4,
    min_samples_leaf=2,
    random_state=42,
    n_jobs=-1
)
model.fit(X_train, y_train)

# ==================== Evaluate ====================
y_pred = model.predict(X_test)

metrics = {
    "model": "RandomForest",
    "timestamp": datetime.utcnow().isoformat(),
    "train_samples": len(X_train),
    "test_samples": len(X_test),
    "rmse_day1": float(np.sqrt(mean_squared_error(y_test[:, 0], y_pred[:, 0]))),
    "rmse_day2": float(np.sqrt(mean_squared_error(y_test[:, 1], y_pred[:, 1]))),
    "rmse_day3": float(np.sqrt(mean_squared_error(y_test[:, 2], y_pred[:, 2]))),
    "rmse_overall": float(np.sqrt(mean_squared_error(y_test, y_pred))),
    "mae_day1": float(mean_absolute_error(y_test[:, 0], y_pred[:, 0])),
    "mae_day2": float(mean_absolute_error(y_test[:, 1], y_pred[:, 1])),
    "mae_day3": float(mean_absolute_error(y_test[:, 2], y_pred[:, 2])),
    "mae_overall": float(mean_absolute_error(y_test, y_pred)),
    "r2_day1": float(r2_score(y_test[:, 0], y_pred[:, 0])),
    "r2_day2": float(r2_score(y_test[:, 1], y_pred[:, 1])),
    "r2_day3": float(r2_score(y_test[:, 2], y_pred[:, 2])),
    "r2_overall": float(r2_score(y_test, y_pred)),
}

print("📊 Random Forest Metrics:")
print(json.dumps(metrics, indent=2))

# ==================== Save Model ====================
model_path = "rf_model.pkl"
joblib.dump(model, model_path)

model_obj = mr.sklearn.create_model(
    name="aqi_rf",
    version=int(datetime.utcnow().strftime("%Y%m%d%H%M")),
    description="Random Forest for 3-day AQI prediction",
    metrics=metrics
)
model_obj.save(model_path)
print("✅ Random Forest model saved to Hopsworks")