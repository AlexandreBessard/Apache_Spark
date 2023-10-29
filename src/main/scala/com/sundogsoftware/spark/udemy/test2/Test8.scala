package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test8 {

  // Create case class with the schema of u.data
  case class UserRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test8")
      .master("local[*]") // You can change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data for itemsDf (replace with your actual data)
    val itemsData = Seq(
      (1, "ProductA"),
      (2, "ProductB"),
      (3, "ProductC")
    )

    // Sample data for transactionsDf (replace with your actual data)
    val transactionsData = Seq(
      (1, "CustomerA"),
      (2, "CustomerB"),
      (3, "CustomerA")
    )

    // Define the schemas for the DataFrames
    val itemsSchema = List("itemId", "itemName")
    val transactionsSchema = List("transactionId", "customerName")

    // Create DataFrames from the sample data
    val itemsDf =
      spark.createDataFrame(itemsData).toDF(itemsSchema: _*)

    val transactionsDf =
      spark.createDataFrame(transactionsData).toDF(transactionsSchema: _*)

    // Perform an inner join using itemId and transactionId as join keys
    val joinedDf: DataFrame =
      itemsDf.join(transactionsDf, itemsDf("itemId") === transactionsDf("transactionId"), "inner")

    // Show the resulting DataFrame after the inner join
    joinedDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
