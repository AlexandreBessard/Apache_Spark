package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{max, min} // Add this import
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test14 {
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
      (1, 101, 10.0),
      (2, 102, 15.0),
      (3, 101, 12.0),
      (4, 103, 8.0),
      (5, 102, 18.0)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "productId", "value")

    // Create a DataFrame from the sample data
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Group by 'productId' and calculate max and min values for 'value'
    val groupedDf = transactionsDf
      .groupBy("productId")
      .agg(max("value").alias("highest"), min("value").alias("lowest"))

    // Show the resulting DataFrame
    groupedDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
