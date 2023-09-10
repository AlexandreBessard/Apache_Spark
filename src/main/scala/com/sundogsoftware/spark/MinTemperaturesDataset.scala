package com.sundogsoftware.spark

import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Find the minimum temperature by weather station */
object MinTemperaturesDataset {

  case class Temperature(stationID: String, date: Int, measure_type: String, temperature: Float)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MinTemperatures")
      .master("local[*]")
      .getOrCreate()

    //We do not have a header, specify the header
    val temperatureSchema = new StructType()
      .add("stationID", StringType, nullable = true)
      .add("date", IntegerType, nullable = true)
      .add("measure_type", StringType, nullable = true)
      .add("temperature", FloatType, nullable = true)

    // Read the file as dataset
    import spark.implicits._
    val ds = spark.read
      .schema(temperatureSchema) // Construct DatFrame
      // ITE00100554,18000101,TMAX,-75,,,E,
      .csv("data/1800.csv")
      .as[Temperature] // Construct DataSet
    
    // Filter out all but TMIN entries
    // measure_type is the column name
    val minTemps = ds.filter($"measure_type" === "TMIN")
    
    // Select only stationID and temperature)
    val stationTemps = minTemps.select("stationID", "temperature")
    
    // Aggregate to find minimum temperature for every station
    // Create column name min(temperature)
    val minTempsByStation = stationTemps.groupBy("stationID").min("temperature")

    println("Debug ===> ")
    minTempsByStation.printSchema()

    // Convert temperature to fahrenheit and sort the dataset
    val minTempsByStationF = minTempsByStation
      //Create new colum named temperature, min temperature comes from minTempsByStation variable
      .withColumn("temperature", round($"min(temperature)" * 0.1f * (9.0f / 5.0f) + 32.0f, 2))
      //Select data we need
      .select("stationID", "temperature").sort("temperature")

    // Collect, format, and print the results
    val results = minTempsByStationF.collect()
    
    for (result <- results) {
       val station = result(0)
       val temp = result(1).asInstanceOf[Float]
       val formattedTemp = f"$temp%.2f F"
       println(s"$station minimum temperature: $formattedTemp")
    }
  }
}