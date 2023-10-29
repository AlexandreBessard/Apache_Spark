package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test19 {

  // Create case class with the schema of u.data
  case class UserRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test18") // Updated to match the object name
      .master("local[*]") // You can change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data (replace this with your actual DataFrame)
    val data = Seq(
      (1, 101),
      (1, 102),
      (2, 103),
      (2, 104),
      (3, 105)
    )

    // Define the schema for the DataFrame
    val schema = List("storeId", "value")

    // Create a DataFrame from the sample data
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Group by "storeId" and calculate the average of "value"
    val resultDf = transactionsDf.groupBy("storeId").
      agg(avg("value"))

    // Show the resulting DataFrame
    resultDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
