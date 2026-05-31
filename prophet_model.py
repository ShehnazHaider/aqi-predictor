import os
import pandas as pd
import numpy as np
import hopsworks
import joblib
from prophet import Prophet
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

# ==================== Prepare Data for Prophet ====================
# Prophet needs 'ds' (timestamp) and 'y' (target)
df_prophet = df[["timestamp", "calculated_aqi"]].copy()
df_prophet.columns = ["ds", "y"]

# Time-based split (last 20% for test)
split_idx = int(len(df_prophet) * 0.8)
train = df_prophet.iloc[:split_idx]
test = df_prophet.iloc[split_idx:]

print(f"Train: {len(train)} rows, Test: {len(test)} rows")

# ==================== Train Prophet ====================
model = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=True,
    daily_seasonality=True,
    changepoint_prior_scale=0.05,
    seasonality_prior_scale=10.0
)
model.fit(train)

# ==================== Predict ====================
# Predict for test period
future = model.make_future_dataframe(periods=len(test), freq='h')
forecast = model.predict(future)

# Get predictions for test period only
y_pred = forecast.iloc[-len(test):]["yhat"].values
y_test = test["y"].values

# For multi-output comparison (3 horizons), we'll predict 24h, 48h, 72h ahead
# Prophet's strength is single-step; we'll use it for day1 and cascade
forecast_full = model.predict(future)
day1_pred = forecast_full.iloc[-len(test):]["yhat"].values[:len(test)]

# Create 3-day predictions by shifting Prophet's forecast
day1_pred = day1_pred[:len(test)-72] if len(day1_pred) > 72 else day1_pred
day2_pred = forecast_full.iloc[-len(test)+24:]["yhat"].values[:len(day1_pred)] if len(test) > 24 else day1_pred
day3_pred = forecast_full.iloc[-len(test)+48:]["yhat"].values[:len(day1_pred)] if len(test) > 48 else day1_pred

# Align test values
y_test_aligned = y_test[:len(day1_pred)]
y_test_day2 = y_test[24:24+len(day1_pred)] if len(y_test) > 24 else y_test_aligned
y_test_day3 = y_test[48:48+len(day1_pred)] if len(y_test) > 48 else y_test_aligned

# ==================== Evaluate ====================
metrics = {
    "train_samples": len(train),
    "test_samples": len(test),
    "rmse_day1": float(np.sqrt(mean_squared_error(y_test_aligned, day1_pred))),
    "rmse_day2": float(np.sqrt(mean_squared_error(y_test_day2, day2_pred))),
    "rmse_day3": float(np.sqrt(mean_squared_error(y_test_day3, day3_pred))),
    "rmse_overall": float(np.sqrt(mean_squared_error(
        np.concatenate([y_test_aligned, y_test_day2, y_test_day3]),
        np.concatenate([day1_pred, day2_pred, day3_pred])
    ))),
    "mae_day1": float(mean_absolute_error(y_test_aligned, day1_pred)),
    "mae_day2": float(mean_absolute_error(y_test_day2, day2_pred)),
    "mae_day3": float(mean_absolute_error(y_test_day3, day3_pred)),
    "mae_overall": float(mean_absolute_error(
        np.concatenate([y_test_aligned, y_test_day2, y_test_day3]),
        np.concatenate([day1_pred, day2_pred, day3_pred])
    )),
    "r2_day1": float(r2_score(y_test_aligned, day1_pred)),
    "r2_day2": float(r2_score(y_test_day2, day2_pred)),
    "r2_day3": float(r2_score(y_test_day3, day3_pred)),
    "r2_overall": float(r2_score(
        np.concatenate([y_test_aligned, y_test_day2, y_test_day3]),
        np.concatenate([day1_pred, day2_pred, day3_pred])
    )),
}

print("📊 Prophet Metrics:")
print(json.dumps(metrics, indent=2))

# ==================== Save Model ====================
model_path = "prophet_model.pkl"
joblib.dump(model, model_path)

model_obj = mr.sklearn.create_model(
    name="aqi_prophet",
    version=int(datetime.utcnow().strftime("%y%m%d")),
    description="Prophet for 3-day AQI prediction",
    metrics=metrics
)
model_obj.save(model_path)
print("✅ Prophet model saved to Hopsworks")