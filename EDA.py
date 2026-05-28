import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# ── Load Data ──────────────────────────────────────────────
df = pd.read_csv("skardu_aqi_dataset.csv")

print("="*60)
print("STEP 1: BASIC INFO")
print("="*60)
print(f"Shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")
print(df.head())

# ── Step 2: Summary Statistics ─────────────────────────────
print("\n" + "="*60)
print("STEP 2: SUMMARY STATISTICS")
print("="*60)
print(df.describe())
print("\nData Types:")
print(df.dtypes)

# ── Step 3: Missing Values ─────────────────────────────────
print("\n" + "="*60)
print("STEP 3: MISSING VALUES")
print("="*60)
print(df.isnull().sum())

plt.figure(figsize=(10, 6))
sns.heatmap(df.isnull(), cbar=False, cmap="YlOrRd")
plt.title("Missing Values Heatmap")
plt.tight_layout()
plt.show()

# ── Step 4: Distribution of Each Feature ──────────────────
print("\n" + "="*60)
print("STEP 4: FEATURE DISTRIBUTIONS")
print("="*60)
numeric_cols = df.select_dtypes(include='number').columns
print(df[numeric_cols].describe().T)

df[numeric_cols].hist(figsize=(16, 12), bins=30, edgecolor='black')
plt.suptitle("Feature Distributions", fontsize=16)
plt.tight_layout()
plt.show()

# ── Step 5: Correlation Heatmap ────────────────────────────
print("\n" + "="*60)
print("STEP 5: CORRELATION HEATMAP")
print("="*60)
plt.figure(figsize=(12, 8))
sns.heatmap(df[numeric_cols].corr(), annot=True, cmap='coolwarm', fmt=".2f")
plt.title("Correlation Heatmap")
plt.tight_layout()
plt.show()

# ── Step 6: Time Series Trend of AQI ──────────────────────
print("\n" + "="*60)
print("STEP 6: AQI TIME SERIES")
print("="*60)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values('timestamp')

plt.figure(figsize=(14, 6))
plt.plot(df['timestamp'], df['aqi_index'], marker='o', markersize=2, linewidth=1)
plt.title("AQI Over Time")
plt.xlabel("Timestamp")
plt.ylabel("AQI Index")
plt.grid(True)
plt.tight_layout()
plt.show()

# ── Step 7: AQI Distribution ───────────────────────────────
plt.figure(figsize=(8, 5))
sns.countplot(x='aqi_index', data=df, palette='Set2')
plt.title("AQI Index Distribution")
plt.xlabel("AQI Index")
plt.ylabel("Count")
plt.tight_layout()
plt.show()

# ── Step 8: Outlier Detection (Boxplots) ───────────────────
print("\n" + "="*60)
print("STEP 8: OUTLIER DETECTION")
print("="*60)
pollutants = ['pm2_5', 'pm10', 'co', 'no2', 'so2', 'o3', 'nh3']

fig, axes = plt.subplots(3, 3, figsize=(16, 12))
axes = axes.flatten()

for i, col in enumerate(pollutants):
    sns.boxplot(x=df[col], ax=axes[i])
    axes[i].set_title(f"Outliers: {col}")

# hide unused subplots
for j in range(i+1, len(axes)):
    axes[j].set_visible(False)

plt.suptitle("Outlier Detection", fontsize=16)
plt.tight_layout()
plt.show()

# ── Step 9: Pollutant Trends Over Time ─────────────────────
print("\n" + "="*60)
print("STEP 9: POLLUTANT TRENDS OVER TIME")
print("="*60)
fig, axes = plt.subplots(len(pollutants), 1, figsize=(14, 20))

for i, col in enumerate(pollutants):
    axes[i].plot(df['timestamp'], df[col], linewidth=0.8)
    axes[i].set_title(f"{col} Over Time")
    axes[i].set_ylabel(col)
    axes[i].grid(True)

plt.suptitle("Pollutant Trends Over Time", fontsize=16)
plt.tight_layout()
plt.show()

# ── Step 10: Weather vs AQI ────────────────────────────────
print("\n" + "="*60)
print("STEP 10: WEATHER VS AQI")
print("="*60)
weather_cols = ['temperature', 'humidity', 'wind_speed']

fig, axes = plt.subplots(1, 3, figsize=(16, 5))

for i, col in enumerate(weather_cols):
    sns.scatterplot(x=df[col], y=df['aqi_index'], ax=axes[i], alpha=0.5)
    axes[i].set_title(f"{col} vs AQI Index")
    axes[i].set_xlabel(col)
    axes[i].set_ylabel("AQI Index")

plt.suptitle("Weather Features vs AQI", fontsize=16)
plt.tight_layout()
plt.show()

# ── Step 11: Monthly AQI Trend ─────────────────────────────
print("\n" + "="*60)
print("STEP 11: MONTHLY AQI TREND")
print("="*60)
df['month'] = df['timestamp'].dt.month
df['hour'] = df['timestamp'].dt.hour

plt.figure(figsize=(12, 5))
sns.boxplot(x='month', y='aqi_index', data=df, palette='Set3')
plt.title("AQI Distribution by Month")
plt.xlabel("Month")
plt.ylabel("AQI Index")
plt.tight_layout()
plt.show()

# ── Step 12: Hourly AQI Trend ──────────────────────────────
plt.figure(figsize=(12, 5))
sns.boxplot(x='hour', y='aqi_index', data=df, palette='Set1')
plt.title("AQI Distribution by Hour of Day")
plt.xlabel("Hour")
plt.ylabel("AQI Index")
plt.tight_layout()
plt.show()

print("\n✅ EDA Complete")