//Data_Preprocessing.scala
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
object Data_CleaningProcessing {

  //step 1:
  def clean(df: DataFrame): DataFrame = {
    val dfNoNulls = df.na.drop()
    val dfTripCleanedData = dfNoNulls.dropDuplicates()
    val dfTripCleanedDataWithoutUnwantedCharacters = dfTripCleanedData.withColumn("bike_id",regexp_replace(col("bike_id"),"[^\\d]",""))
    dfTripCleanedDataWithoutUnwantedCharacters
    //dfTripCleanedData
  }
  //
  def aggregateTripsByDay(df: DataFrame): DataFrame = {
    df.groupBy("trip_date")
      .agg(
        count("*").alias("trip_count"),
        sum("duration").alias("total_trip_duration")
      )
      .orderBy("trip_date")
  }

  //merging dataframes
  def addStartStationName(tripDF: DataFrame, stationDF: DataFrame): DataFrame = {
    tripDF
      .join(
        stationDF.select(col("Station ID"), col("Station Name").alias("start_station_name")),
        tripDF("start_station") === col("Station ID"),
        "left"
      )
      .drop("Station ID", "Station Name") // drop to avoid duplication
  }
  def addEndStationName(tripDF: DataFrame, stationDF: DataFrame): DataFrame = {
    tripDF
      .join(
        stationDF.select(col("Station ID"), col("Station Name").alias("end_station_name")),
        tripDF("end_station") === col("Station ID"),
        "left"
      )
      .drop("Station ID", "Station Name") // remove unwanted join cols
  }

  // step 2: parse timestamps and extract date/hour
  def parseTimestamps(df: DataFrame): DataFrame = {
    df.withColumn("start_time", to_timestamp(col("start_time"), "M/d/yyyy H:mm"))
      .withColumn("end_time", to_timestamp(col("end_time"), "M/d/yyyy H:mm"))
      .withColumn("trip_date", to_date(col("start_time")))
      .withColumn("hour", hour(col("start_time")))
  }


  //for weather data
  def cleanWeatherData(df: DataFrame): DataFrame = {
    df.select(
        to_date(col("DATE")).alias("weather_date"),
        col("HourlyDryBulbTemperature").cast("double").alias("temperature"),
        col("HourlyPrecipitation").cast("double").alias("precipitation"),
        col("HourlyWindSpeed").cast("double").alias("wind_speed"),
        col("HourlyVisibility").cast("double").alias("visibility"),
        col("HourlyRelativeHumidity").cast("double").alias("humidity"),
        col("HourlyPressureChange").cast("double").alias("pressure_change")
      )
      .na.fill(Map(
        "temperature" -> 0.0,
        "precipitation" -> 0.0,
        "wind_speed" -> 0.0,
        "visibility" -> 0.0,
        "humidity" -> 0.0,
        "pressure_change" -> 0.0
      ))
      .groupBy("weather_date")
      .agg(
        avg("temperature").alias("avg_temp"),
        sum("precipitation").alias("total_precipitation"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed"),
        avg("visibility").alias("avg_visibility"),
        avg("pressure_change").alias("avg_pressure_change")
      )
      .orderBy("weather_date")
  }

  // Join daily trip data with weather data
  def joinTripWithWeather(tripDF: DataFrame, weatherDF: DataFrame): DataFrame = {
    tripDF.join(
      weatherDF,
      tripDF("trip_date") === weatherDF("weather_date"),
      "inner"
    ).drop("weather_date") // Drop duplicate column
  }

}
