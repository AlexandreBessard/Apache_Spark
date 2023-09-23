package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, dayofyear, from_unixtime}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test27 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Sample data with Unix timestamps
    val data: DataFrame = Seq(
      ("Store A", 1632303600),  // Unix timestamp for September 22, 2021, 10:00 AM UTC
      ("Store B", 1629871200),  // Unix timestamp for August 25, 2021, 12:00 PM UTC
      ("Store C", 1641058800)   // Unix timestamp for January 2, 2022, 2:00 PM UTC
    ).toDF("StoreName", "openDate")

    // Convert Unix timestamp to Timestamp and extract day of year
    //timestamp can also be TIMESTAMP
    val storesDFWithTimestamp = data
      .withColumn("openTimestamp", col("openDate").cast("timestamp"))
      .withColumn("dayOfYear", dayofyear(col("openTimestamp")))

    // Show the resulting DataFrame
    storesDFWithTimestamp.show(false)

    // Stop the SparkSession
    spark.stop()
  }

}
