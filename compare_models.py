import os
import hopsworks
import json
import pandas as pd

project = hopsworks.login(api_key_value=os.environ["HOPSWORKS_API_KEY"])
mr = project.get_model_registry()

# Get all models
model_names = ["aqi_lr", "aqi_rf", "aqi_xgb", "aqi_gbm", "aqi_lstm"]

best_model = None
best_rmse = float('inf')
results = []

for name in model_names:
    try:
        models = mr.get_models(name=name)
        if models:
            latest = models[-1]  # newest version
            metrics = latest.training_metrics
            results.append({
                "name": name,
                "version": latest.version,
                "rmse_overall": metrics.get("rmse_overall", float('inf')),
                "mae_overall": metrics.get("mae_overall", float('inf')),
                "r2_overall": metrics.get("r2_overall", -float('inf')),
            })
            print(f"✅ {name} v{latest.version}: RMSE={metrics.get('rmse_overall', 'N/A')}")
        else:
            print(f"⚠️ No model found for {name}")
    except Exception as e:
        print(f"❌ Error fetching {name}: {e}")

# Find best by RMSE
results_df = pd.DataFrame(results)
if not results_df.empty:
    best = results_df.loc[results_df["rmse_overall"].idxmin()]
    print(f"\n🏆 Best Model: {best['name']} v{best['version']}")
    print(f"   RMSE: {best['rmse_overall']:.2f}")
    print(f"   MAE: {best['mae_overall']:.2f}")
    print(f"   R²: {best['r2_overall']:.4f}")
    
    # Save results for Streamlit
    results_df.to_csv("model_comparison.csv", index=False)
    print("✅ Comparison saved to model_comparison.csv")
else:
    print("❌ No models found to compare")