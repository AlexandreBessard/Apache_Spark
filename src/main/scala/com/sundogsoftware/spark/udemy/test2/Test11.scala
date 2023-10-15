package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test11 {

  // Create case class with the schema of u.data
  case class UserRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test10") // Updated to match the object name
      .master("local[*]") // You can change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data (replace this with your actual DataFrame)
    val data = Seq(
      (1, "ProductA", Array("blue", "winter", "cozy")),
      (2, "ProductB", Array("red", "summer", "fresh")),
      (3, "ProductC", Array("green", "spring", "vibrant"))
    )

    // Define the schema for the DataFrame
    val schema = List("itemId", "itemName", "attributes")

    // Create a DataFrame from the sample data
    val itemsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Sort the "attributes" column in descending order
    val sortedDf = itemsDf.withColumn("attributes",
      sort_array(col("attributes"), asc = false)) // desc order

    val sortedDf1 = itemsDf.withColumn("attributes",
      sort_array(col("attributes"))) // asc order (by default)

    // Show the resulting DataFrame
    sortedDf.show(truncate = false)

    sortedDf1.show()

    // Stop the SparkSession
    spark.stop()
  }
}
