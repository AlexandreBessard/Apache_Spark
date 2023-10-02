package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions.udf

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test13 {

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
    val isHighError: Double => Boolean = (predError: Double) => predError > 3.0
    val isHighErrorUdf = udf(isHighError, DataTypes.BooleanType)

    // Add a new column "isHighError" using the UDF
    // Beware to use the UDF itself and not the lambda associated to this UDF method.
    val updatedDf = transactionsDf.withColumn("isHighError", isHighErrorUdf(col("predError")))

    // Show the resulting DataFrame
    updatedDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
