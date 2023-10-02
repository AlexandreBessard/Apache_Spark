package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test17 {

  // Create case class with the schema of u.data
  case class UserRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test17")
      .master("local[*]") // You can change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data (replace this with your actual DataFrame)
    val data = Seq(
      (1, 25, 3.0),
      (2, 30, 2.5),
      (3, 25, 1.8),
      (4, 35, 4.2),
      (5, 25, 3.7)
    )

    // Define the schema for the DataFrame
    val schema = StructType(
      Seq(
        StructField("transactionId", IntegerType, nullable = false),
        StructField("storeId", IntegerType, nullable = false),
        StructField("predError", DoubleType, nullable = false)
      )
    )

    // Create a DataFrame from the sample data
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF("transactionId", "storeId", "predError")


    // Filter the DataFrame to select rows where "storeId" is equal to 25
    val filteredDf = transactionsDf.filter(col("storeId") === 25)

    // Select specific columns "predError" and "storeId" and remove duplicates
    val resultDf = filteredDf.select("predError", "storeId").distinct()

    // Show the resulting DataFrame
    resultDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
