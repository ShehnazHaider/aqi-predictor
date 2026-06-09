# 🌫️ AQI Prediction System — Skardu, Pakistan

 
A fully serverless machine learning pipeline that predicts the Air Quality Index (AQI) for the next 3 days using real-time environmental data.

🔗 **Live Dashboard:** [aqi-predictor.streamlit.app](https://aqi-predictor-8dsetbtaryczxebhhwrkfp.streamlit.app/)

---

## 📖 Overview

Air pollution is a growing concern, especially in mountain valleys like Skardu, Gilgit-Baltistan, where winter temperature inversions trap pollutants for months. This project automates the entire process — from hourly data collection to real-time dashboard visualization — using a serverless CI/CD pipeline and machine learning models.

---

## 🚀 Features

✅ **Automated Data Collection**  
Fetches air quality + weather data hourly via OpenWeatherMap & Open-Meteo APIs using GitHub Actions.

✅ **Data Management with Hopsworks Feature Store**  
Stores, versions, and manages all raw and processed features for reproducibility.

✅ **Data Preprocessing & Feature Engineering**  
Log transforms, winsorization, temporal features, rolling statistics, lag windows, and feature interactions — 46 engineered features total.

✅ **Model Training & Comparison**  
Trains Linear Regression, Random Forest, XGBoost, and Gradient Boosting daily. Automatically selects and promotes the best-performing model.

✅ **Real-Time Dashboard**  
Interactive Streamlit dashboard with 3-day AQI forecasts, EPA color coding, health advisories, historical trends, and pollutant breakdowns.

✅ **Continuous Integration & Delivery (CI/CD)**  
Fully automated workflow — hourly data updates and daily retraining with zero manual intervention.

---

## 📊 Model Performance

| Model | RMSE | MAE | R² |
|-------|------|-----|-----|
| **XGBoost** 🏆 | 39.26 | 30.53 | 0.58 |
| Random Forest | 40.19 | 33.04 | 0.52 |
| Gradient Boosting | 40.23 | 30.99 | 0.41 |
| Linear Regression | 42.45 | 32.28 | 0.35 |

✅ **Final Model:** XGBoost (Multi-Output)  
✅ **Forecast Range:** Next 3 Days (24h, 48h, 72h)  
✅ **Features:** 46 engineered features across 4,100+ hourly records



## 🛠️ Tech Stack

| Category | Tools |
|----------|-------|
| **Data Sources** | OpenWeatherMap, Open-Meteo |
| **Feature Store** | Hopsworks |
| **ML Libraries** | Scikit-learn, XGBoost |
| **Dashboard** | Streamlit, Plotly |
| **CI/CD** | GitHub Actions |
| **Language** | Python 3.10 |

---

## 🤝 Acknowledgments

This project was developed as part of the **10Pearl Institute** program.

Special thanks to our mentors for their guidance and support throughout this journey.

---

## 🧩 Future Improvements

- Add SHAP explainability for feature importance
- Include additional cities for regional comparison
- Experiment with ensemble voting for improved forecasts
- Add email/SMS alerts for hazardous AQI levels

---

## 🏅 Team

**10Pearl AQI Team**  
June 2026
