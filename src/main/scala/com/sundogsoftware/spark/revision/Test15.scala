package com.sundogsoftware.spark.revision

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Test15 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Cast as String and Double
     */

    // Sample data
    val data = Seq(("Spring", 10.5), ("Summer", 15.2), ("Fall", 12.8), ("Winter", 8.3))

    // Create a DataFrame using the sample data and schema
    val seasonsDf: DataFrame = spark.createDataFrame(data)
      .toDF("season", "wind_speed_ms")
      .withColumn(
        "season",
        col("season").cast(StringType))
      .withColumn(
        "wind_speed_ms",
        col("wind_speed_ms").cast(DoubleType)
      )

    // Show the DataFrame
    seasonsDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
