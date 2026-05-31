import os
import pandas as pd
import numpy as np
import hopsworks
import joblib
import xgboost as xgb
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import json
from datetime import datetime

# ==================== Load Data ====================
project = hopsworks.login(api_key_value=os.environ["HOPSWORKS_API_KEY"])
fs = project.get_feature_store()
mr = project.get_model_registry()

fg = fs.get_feature_group(name="processed_aqi_skardu_v2", version=1)
df = fg.read()
df = df.sort_values("timestamp").reset_index(drop=True)

feature_cols = [col for col in df.columns if col.endswith("_scaled")]
feature_cols = [col for col in df.columns if col.endswith("_scaled")]
print(f"✅ Using {len(feature_cols)} features: {feature_cols[:5]}...")  # Show first 5

target_cols = ["aqi_day1", "aqi_day2", "aqi_day3"]

df_model = df.dropna(subset=target_cols).copy()

X = df_model[feature_cols].values
y = df_model[target_cols].values

split_idx = int(len(X) * 0.8)
X_train, X_test = X[:split_idx], X[split_idx:]
y_train, y_test = y[:split_idx], y[split_idx:]

# ==================== Train (multi-output wrapper) ====================
from sklearn.multioutput import MultiOutputRegressor

model = MultiOutputRegressor(
    xgb.XGBRegressor(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1
    )
)
model.fit(X_train, y_train)

# ==================== Evaluate ====================
y_pred = model.predict(X_test)

metrics = {
    
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

print("📊 XGBoost Metrics:")
print(json.dumps(metrics, indent=2))

# ==================== Save Model ====================
model_path = "xgb_model.pkl"
joblib.dump(model, model_path)

model_obj = mr.sklearn.create_model(
    name="aqi_xgb",
    version=int(datetime.utcnow().strftime("%y%m%d")),
    description="XGBoost for 3-day AQI prediction",
    metrics=metrics
)
model_obj.save(model_path)
print("✅ XGBoost model saved to Hopsworks")