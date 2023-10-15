package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Test13 {

  // Defining case class outside of main() to avoid issues
  case class UserRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test13")
      .master("local[*]") // Using local mode for this example
      .getOrCreate()

    // Sample data
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

    // Define a UDF that checks if "predError" is greater than a threshold (e.g., 3.0)
    // Takes Double as param and returns Boolean
    val isHighError: Double => Boolean = (predError: Double) => predError > 3.0

    // Use the UDF without specifying the return type
    val isHighErrorUdf = udf(isHighError)

    // Add a new column "isHighError" using the UDF
    val updatedDf = transactionsDf.withColumn("isHighError", isHighErrorUdf(col("predError")))

    // Show the resulting DataFrame
    updatedDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
