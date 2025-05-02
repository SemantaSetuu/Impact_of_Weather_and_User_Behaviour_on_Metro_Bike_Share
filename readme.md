
---

##  Tools & Technologies

- **Scala (2.12.18)**
- **Apache Spark (3.5.1)**
- **Spark SQL & MLlib**
- **Azure Blob Storage (WASBS)**
- **PostgreSQL (JDBC)**
- **IntelliJ IDEA**

---

##  Features

1. **Reads 8 raw CSV files** (2023–2024) from Azure Blob Storage.
2. Cleans trip data, weather data, and merges station metadata.
3. Parses timestamps and enriches data with station names.
4. Joins daily trip data with weather metrics (temp, humidity, etc.).
5. Stores cleaned data in PostgreSQL tables.
6. **Implements Random Forest Regression** to predict 2025 monthly trip count.
7. Uploads prediction results to PostgreSQL for further analysis.

---

##  Research Questions Answered

1. **How does weather affect trip volume?**
2. **What are the busiest stations and peak hours?**
3. **How do passholder types vary in behavior?**
4. **Can we predict future (2025) trip trends monthly?**

---

##  Prediction Metrics (Random Forest)

- **RMSE**: ~3776.57
- **MAE**: ~3113.46
- **R² Score**: ~0.86

---

##  How to Run

1. Clone the repo.
2. Update the Azure Storage key in `AzureConnector.scala`.
3. Configure PostgreSQL URL, username, and password in `PostgreConnector.scala`.
4. Run `Main.scala` from IntelliJ IDEA or using `sbt run`.

---

## ️ Output Tables in PostgreSQL

- `trip_with_weather`
- `sorted_trip_data_with_station_names`
- `trip_predictions_2025`

##  Run Python file for visualization for finding answers
1. Configure PostgreSQL URL, username, and password in `PostgreConnector.scala`.
2. Run `visualization.py` in python to get the visualizations.