import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine


# PostgreSQL connection parameters
username = "postgres"
password = "Admin"
host = "localhost"
port = "5432"
database = "DISS"

# Create connection
engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')


#For finding the answer of the question 1------------------------
# Load the table
df = pd.read_sql("SELECT * FROM trip_with_weather", engine)

# Convert date to datetime (just in case)
df['trip_date'] = pd.to_datetime(df['trip_date'])


# 1.boxplot: Trip Count Distribution: Rainy vs Non-Rainy Days
df['rainy_day'] = df['total_precipitation'] > 0
plt.figure(figsize=(8,6))
sns.boxplot(x='rainy_day', y='trip_count', data=df)
plt.xticks([0, 1], ['No Rain', 'Rain'])
plt.title("Trip Count Distribution: Rainy vs Non-Rainy Days")
plt.xlabel("Day Type")
plt.ylabel("Trip Count")
plt.show()


# 2. Scatterplot: Humidity vs. Trip Count
plt.figure(figsize=(8, 6))
sns.scatterplot(x='avg_humidity', y='trip_count', data=df)
plt.title("Humidity vs. Trip Count")
plt.xlabel("Average Humidity")
plt.ylabel("Trip Count")
plt.tight_layout()
plt.show()


# 3. Correlation Heatmap (Optional)
plt.figure(figsize=(10, 6))
corr_cols = ['trip_count', 'avg_temp', 'total_precipitation', 'avg_humidity', 'avg_wind_speed', 'avg_visibility', 'avg_pressure_change']
sns.heatmap(df[corr_cols].corr(), annot=True, cmap='coolwarm')
plt.title("Correlation Heatmap")
plt.tight_layout()
plt.show()




#for answer question 2-------------
# Load trip data
df = pd.read_sql("SELECT * FROM sorted_trip_data_with_station_names", engine)


#TOP 10 BUSIEST STATIONS BY HOUR

# Group by start station and hour
hourly_usage = df.groupby(['start_station_name', 'hour'])['trip_id'].count().reset_index(name='trip_count')

# Total trip count per station (to get top 10)
total_usage = hourly_usage.groupby('start_station_name')['trip_count'].sum().reset_index()
top_10_stations = total_usage.sort_values(by='trip_count', ascending=False).head(10)['start_station_name']

# Filter only top 10
top_hourly_usage = hourly_usage[hourly_usage['start_station_name'].isin(top_10_stations)]

# Plot
plt.figure(figsize=(12, 6))
sns.barplot(data=top_hourly_usage, x='hour', y='trip_count', hue='start_station_name')
plt.title("Top 10 Busiest Stations by Hourly Usage")
plt.xlabel("Hour of the Day")
plt.ylabel("Trip Count")
plt.legend(title="Station", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()


#Rushhour finding
# Group by hour to get total trip count
hourly_trip_count = df.groupby('hour')['trip_id'].count().reset_index(name='trip_count')

# Plot
plt.figure(figsize=(10, 5))
sns.lineplot(data=hourly_trip_count, x='hour', y='trip_count', marker='o', linewidth=2)
plt.title("Trip Count by Hour of the Day")
plt.xlabel("Hour (0–23)")
plt.ylabel("Total Trip Count")
plt.xticks(range(0, 24))
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()


#for finding answer of the question 3--------------------------
# Load trip data again (if not already loaded)
# 1. Trip Count by Passholder Type
plt.figure(figsize=(8,5))
sns.countplot(data=df, x='passholder_type', order=df['passholder_type'].value_counts().index)
plt.title("Trip Count by Passholder Type")
plt.xlabel("Passholder Type")
plt.ylabel("Number of Trips")
plt.xticks(rotation=20)
plt.tight_layout()
plt.show()

# 2. Hourly Usage by Passholder Type
plt.figure(figsize=(12,6))
sns.histplot(data=df, x='hour', hue='passholder_type', multiple='stack', bins=24)
plt.title("Hourly Usage by Passholder Type")
plt.xlabel("Hour of the Day")
plt.ylabel("Trip Count")
plt.tight_layout()
plt.show()




#for giving answer to question 4:
# Load 2023 & 2024 monthly actuals
query_actual = """
SELECT 
  EXTRACT(YEAR FROM trip_date) AS year,
  EXTRACT(MONTH FROM trip_date) AS month,
  COUNT(*) AS trip_count
FROM sorted_trip_data_with_station_names
WHERE EXTRACT(YEAR FROM trip_date) IN (2023, 2024)
GROUP BY year, month
ORDER BY year, month
"""
df_actual = pd.read_sql(query_actual, engine)

# Load 2025 predictions
df_pred = pd.read_sql("SELECT * FROM trip_predictions_2025", engine)
df_pred.rename(columns={'prediction': 'trip_count'}, inplace=True)
df_pred['year'] = 2025

# Combine both
df_combined = pd.concat([df_actual, df_pred], ignore_index=True)
df_combined['month'] = df_combined['month'].astype(int)

# Pivot for plotting
df_pivot = df_combined.pivot(index='month', columns='year', values='trip_count')

# Plot
plt.figure(figsize=(10, 6))
for year in df_pivot.columns:
    plt.plot(df_pivot.index, df_pivot[year], marker='o', label=int(year))

plt.title("Monthly Trip Count: 2023 vs 2024 vs Predicted 2025")
plt.xlabel("Month")
plt.ylabel("Trip Count")
plt.xticks(range(1, 13),
           ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
            'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
plt.legend(title="Year")
plt.grid(True)
plt.tight_layout()
plt.show()