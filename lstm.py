import os
import pandas as pd
import numpy as np
import hopsworks
import json
from datetime import datetime
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# ==================== Load Data ====================
project = hopsworks.login(api_key_value=os.environ["HOPSWORKS_API_KEY"])
fs = project.get_feature_store()
mr = project.get_model_registry()

fg = fs.get_feature_group(name="processed_aqi_skardu", version=1)
df = fg.read()
df = df.sort_values("date").reset_index(drop=True)

feature_cols = [
    "pm2_5_scaled", "pm10_scaled", "co_log_scaled", "no2_scaled",
    "o3_scaled", "so2_log_scaled", "nh3_log_scaled",
    "temperature_scaled", "humidity_scaled", "wind_speed_scaled",
    "day_scaled", "month_scaled", "aqi_change_rate_scaled"
]

target_cols = ["aqi_day1", "aqi_day2", "aqi_day3"]

df_model = df.dropna(subset=target_cols).copy()

X_all = df_model[feature_cols].values
y_all = df_model[target_cols].values

split_idx = int(len(X_all) * 0.8)
X_train, X_test = X_all[:split_idx], X_all[split_idx:]
y_train, y_test = y_all[:split_idx], y_all[split_idx:]

# ==================== Reshape for LSTM ====================
# LSTM expects (samples, timesteps, features)
# We'll use a lookback of 7 days
lookback = 7

def create_sequences(X, y, lookback):
    X_seq, y_seq = [], []
    for i in range(lookback, len(X)):
        X_seq.append(X[i-lookback:i])
        y_seq.append(y[i])
    return np.array(X_seq), np.array(y_seq)

X_train_seq, y_train_seq = create_sequences(X_train, y_train, lookback)
X_test_seq, y_test_seq = create_sequences(X_test, y_test, lookback)

print(f"X_train_seq shape: {X_train_seq.shape}")
print(f"X_test_seq shape: {X_test_seq.shape}")

# ==================== Build LSTM ====================
n_features = X_train_seq.shape[2]

model = keras.Sequential([
    layers.LSTM(64, activation='relu', return_sequences=True, input_shape=(lookback, n_features)),
    layers.Dropout(0.2),
    layers.LSTM(32, activation='relu'),
    layers.Dropout(0.2),
    layers.Dense(16, activation='relu'),
    layers.Dense(3)  # 3 outputs: day1, day2, day3
])

model.compile(optimizer='adam', loss='mse', metrics=['mae'])
model.summary()

# ==================== Train ====================
early_stop = keras.callbacks.EarlyStopping(patience=20, restore_best_weights=True)

history = model.fit(
    X_train_seq, y_train_seq,
    validation_split=0.2,
    epochs=100,
    batch_size=8,
    callbacks=[early_stop],
    verbose=1
)

# ==================== Evaluate ====================
y_pred = model.predict(X_test_seq)

metrics = {
    "model": "LSTM",
    "timestamp": datetime.utcnow().isoformat(),
    "train_samples": len(X_train_seq),
    "test_samples": len(X_test_seq),
    "lookback_days": lookback,
    "epochs_trained": len(history.history['loss']),
    "rmse_day1": float(np.sqrt(mean_squared_error(y_test_seq[:, 0], y_pred[:, 0]))),
    "rmse_day2": float(np.sqrt(mean_squared_error(y_test_seq[:, 1], y_pred[:, 1]))),
    "rmse_day3": float(np.sqrt(mean_squared_error(y_test_seq[:, 2], y_pred[:, 2]))),
    "rmse_overall": float(np.sqrt(mean_squared_error(y_test_seq, y_pred))),
    "mae_day1": float(mean_absolute_error(y_test_seq[:, 0], y_pred[:, 0])),
    "mae_day2": float(mean_absolute_error(y_test_seq[:, 1], y_pred[:, 1])),
    "mae_day3": float(mean_absolute_error(y_test_seq[:, 2], y_pred[:, 2])),
    "mae_overall": float(mean_absolute_error(y_test_seq, y_pred)),
    "r2_day1": float(r2_score(y_test_seq[:, 0], y_pred[:, 0])),
    "r2_day2": float(r2_score(y_test_seq[:, 1], y_pred[:, 1])),
    "r2_day3": float(r2_score(y_test_seq[:, 2], y_pred[:, 2])),
    "r2_overall": float(r2_score(y_test_seq, y_pred)),
}

print("📊 LSTM Metrics:")
print(json.dumps(metrics, indent=2))

# ==================== Save Model ====================
model_path = "lstm_model.keras"
model.save(model_path)

model_obj = mr.tensorflow.create_model(
    name="aqi_lstm",
    version=int(datetime.utcnow().strftime("%Y%m%d%H%M")),
    description="LSTM for 3-day AQI prediction (7-day lookback)",
    metrics=metrics
)
model_obj.save(model_path)
print("✅ LSTM model saved to Hopsworks")