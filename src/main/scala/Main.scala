//Main.scala
import org.apache.spark.sql.SparkSession
import AzureConnector._
import Data_CleaningProcessing._
import PostgreConnector._

object Main {
  def main(args: Array[String]): Unit = {

    // FORCE Spark to bind to localhost
    System.setProperty("spark.driver.bindAddress", "127.0.0.1")

    val spark = SparkSession.builder().appName("AzureBlobRead").master("local[*]").getOrCreate()

    configure(spark)

    val df_trip_data = spark.read.option("header", "true").csv(
      "wasbs://alldata@tripdata126.blob.core.windows.net/metro-trips-2023-q1.csv",
      "wasbs://alldata@tripdata126.blob.core.windows.net/metro-trips-2023-q1.csv",
      "wasbs://alldata@tripdata126.blob.core.windows.net/metro-trips-2023-q2.csv",
      "wasbs://alldata@tripdata126.blob.core.windows.net/metro-trips-2023-q3.csv",
      "wasbs://alldata@tripdata126.blob.core.windows.net/metro-trips-2023-q4.csv",
      "wasbs://alldata@tripdata126.blob.core.windows.net/metro-trips-2024-q1.csv",
      "wasbs://alldata@tripdata126.blob.core.windows.net/metro-trips-2024-q2.csv",
      "wasbs://alldata@tripdata126.blob.core.windows.net/metro-trips-2024-q3.csv",
      "wasbs://alldata@tripdata126.blob.core.windows.net/metro-trips-2024-q4.csv")

    df_trip_data.show()


    val df_station_data= spark.read.option("header","true").csv(
      "wasbs://alldata@tripdata126.blob.core.windows.net/metro-bike-stations-data.csv")
    df_station_data.show()

    val df_weather_data= spark.read.option("header","true").csv(
      "wasbs://alldata@tripdata126.blob.core.windows.net/weather-data-for-2023-and-2024.csv")

    val df_weather_data_cleaned = Data_CleaningProcessing.cleanWeatherData(df_weather_data)
    df_weather_data_cleaned.show()
    println(s"Original weather data: Rows = ${df_weather_data.count()}, Columns = ${df_weather_data.columns.length}")
    println(s"Processed weather data: Rows = ${df_weather_data_cleaned.count()}, Columns = ${df_weather_data_cleaned.columns.length}")



    //step 4: cleaning trip data
    val df_cleaned_trip_data = Data_CleaningProcessing.clean(df_trip_data)
    print(s"Original number of rows are ${df_trip_data.count()}.\nAfter cleaning number of rows are ${df_cleaned_trip_data.count()}\n")

    val df_trip_data_with_time = Data_CleaningProcessing.parseTimestamps(df_cleaned_trip_data)


    val df_trip_data_daily = Data_CleaningProcessing.aggregateTripsByDay(df_trip_data_with_time)
    df_trip_data_daily.show()

    //merging both df  together for finding answer of our question no 1
    val df_trip_with_weather = Data_CleaningProcessing.joinTripWithWeather(df_trip_data_daily, df_weather_data_cleaned)
    df_trip_with_weather.orderBy("trip_date").show()




    //
    val df_with_station_names = Data_CleaningProcessing.addStartStationName(df_trip_data_with_time, df_station_data)
    val df_tripdata_with_station_names = Data_CleaningProcessing.addEndStationName(df_with_station_names,df_station_data)
    val sorted_df_for_tripdata_and_station_names = df_tripdata_with_station_names.orderBy("start_time")
    sorted_df_for_tripdata_and_station_names.show()

    //sorted_df_for_tripdata_and_station_names.coalesce(1).write.option("header", "true").mode("overwrite").csv("wasbs://alldata@tripdata126.blob.core.windows.net/output/trip_data_with_time")


    // Upload sorted trip data to PostgreSQL for further analysis for question 2 and 3
    val dbTableName = "sorted_trip_data_with_station_names" //table name
    sorted_df_for_tripdata_and_station_names.write
      .format("jdbc")
      .option("url",url)
      .option("dbtable",dbTableName)
      .option("user", dbUser)
      .option("password",dbPassword)
      .option("driver",driver)
      .mode("overwrite")
      .save()

    //saving to the database for analysis question 1
    val dbTableName2 = "trip_with_weather" //table name
    df_trip_with_weather.write
      .format("jdbc")
      .option("url",url)
      .option("dbtable",dbTableName2)
      .option("user", dbUser)
      .option("password",dbPassword)
      .option("driver",driver)
      .mode("overwrite")
      .save()

    println("Data uploaded successfully to PostgreSQL table")









    //using ml for predicting 2025 monthly basis trip count for giving answer to question 4
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.regression.RandomForestRegressor
    import org.apache.spark.ml.evaluation.RegressionEvaluator
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // Step 1: Prepare ML features by extracting year and month
    val df_with_time = Data_CleaningProcessing.parseTimestamps(df_cleaned_trip_data)
    val df_monthly = df_with_time
      .withColumn("year", year(col("trip_date")))
      .withColumn("month", month(col("trip_date")))
      .groupBy("year", "month")
      .agg(count("trip_id").alias("trip_count"))
      .orderBy("year", "month")

    // Step 2: Assemble features
    val assembler = new VectorAssembler()
      .setInputCols(Array("year", "month"))
      .setOutputCol("features")

    val final_df = assembler.transform(df_monthly).select("features", "trip_count")

    // Step 3: Train/Test Split
    val Array(trainDF, testDF) = final_df.randomSplit(Array(0.8, 0.2), seed = 42)

    val rf = new RandomForestRegressor()
      .setLabelCol("trip_count")
      .setFeaturesCol("features")
      .setNumTrees(100)

    val model = rf.fit(trainDF)
    val predictions = model.transform(testDF)

    // Step 4: Evaluate the model
    val rmseEvaluator = new RegressionEvaluator()
      .setLabelCol("trip_count")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val maeEvaluator = new RegressionEvaluator()
      .setLabelCol("trip_count")
      .setPredictionCol("prediction")
      .setMetricName("mae")

    val r2Evaluator = new RegressionEvaluator()
      .setLabelCol("trip_count")
      .setPredictionCol("prediction")
      .setMetricName("r2")

    val rmse = rmseEvaluator.evaluate(predictions)
    val mae = maeEvaluator.evaluate(predictions)
    val r2 = r2Evaluator.evaluate(predictions)

    println(s"RMSE: $rmse")
    println(s"MAE: $mae")
    println(s"R² Score: $r2")

    // Step 5: Predict 2025 values
    val future_2025 = Seq(
      (2025, 1), (2025, 2), (2025, 3), (2025, 4),
      (2025, 5), (2025, 6), (2025, 7), (2025, 8),
      (2025, 9), (2025,10), (2025,11), (2025,12)
    ).toDF("year", "month")

    val future_2025_features = assembler.transform(future_2025)
    val future_predictions = model.transform(future_2025_features)

    future_predictions.select("year", "month", "prediction").show()

    // Step 6: Save predictions to PostgreSQL
    val dbTableName3 = "trip_predictions_2025"
    future_predictions.select("year", "month", "prediction")
      .write
      .format("jdbc")
      .option("url", url)
      .option("dbtable", dbTableName3)
      .option("user", dbUser)
      .option("password", dbPassword)
      .option("driver", driver)
      .mode("overwrite")
      .save()
    spark.stop()
  }
}