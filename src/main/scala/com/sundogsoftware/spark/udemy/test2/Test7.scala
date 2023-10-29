package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test7 {

  // Create case class with the schema of u.data
  case class UserRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test7")
      .master("local[*]")
      .getOrCreate()

    // Sample data (replace this with your actual DataFrame)
    val data = Seq(
      (1, "ProductA", 3.0),
      (2, "ProductB", 2.5),
      (3, "ProductA", 1.8),
      (4, "ProductC", 4.2),
      (5, "ProductB", 3.7)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "itemName", "predError")

    // Create a DataFrame from the sample data
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Select specific columns and collect the results
    val selectedRows: Array[Row] = transactionsDf
      .select("transactionId", "predError")
      .collect() // Returns an array, like take(int) which takes a parameter, collect does not

    selectedRows.foreach(e => println(e))

    // Print the collected data
    selectedRows.foreach(row => {
      // index-based 0 because it is an array
      val transactionId = row.getInt(0) // Assuming "transactionId" is the first selected column
      val predError = row.getDouble(1) // Assuming "predError" is the second selected column
      println(s"Transaction ID: $transactionId, Prediction Error: $predError")
    })

    // Stop the SparkSession
    spark.stop()
  }
}
