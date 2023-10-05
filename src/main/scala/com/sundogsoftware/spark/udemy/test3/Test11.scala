package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{asc, col, desc, unix_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test11 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test6")
      .master("local[*]")
      .getOrCreate()

    // Sample data (Replace with your actual data or DataFrame)
    val data = Seq(
      (1, "2023-09-26 10:30:00"),
      (2, "2023-09-27 15:45:30"),
      (3, "2023-09-28 20:15:15")
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "transactionDate")

    // Create a DataFrame from the sample data
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Convert the 'transactionDate' column to a Unix timestamp
    val dfWithTimestamp = transactionsDf.withColumn(
      "transactionTimestamp",
      unix_timestamp(col("transactionDate"), "yyyy-MM-dd HH:mm:ss")
    )

    // Drop the original 'transactionDate' column
    val finalDf = dfWithTimestamp.drop("transactionDate")

    // Show the resulting DataFrame
    finalDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
