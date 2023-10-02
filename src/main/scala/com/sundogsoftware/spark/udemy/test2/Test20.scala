package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test20 {

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

    // Sample data for the first DataFrame
    val data1 = Seq(
      (1, "ProductA", 4.2),
      (2, "ProductB", 2.5),
      (3, "ProductC", 1.8)
    )

    // Sample data for the second DataFrame
    val data2 = Seq(
      (1, "ProductA", 4.2),
      (5, "ProductE", 3.7),
      (6, "ProductF", 2.0)
    )

    // Define the schema for the DataFrames
    val schema = List("transactionId", "itemName", "predError")

    // Create DataFrames from the sample data
    val transactionsDf1 = spark.createDataFrame(data1).toDF(schema: _*)
    val transactionsDf2 = spark.createDataFrame(data2).toDF(schema: _*)

    // Union the two DataFrames and remove duplicates
    val unionedDf = transactionsDf1.union(transactionsDf2).distinct()

    // Show the resulting DataFrame
    unionedDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
