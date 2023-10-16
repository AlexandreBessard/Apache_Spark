package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, unix_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test12 {
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
      (1, "ProductA",
        Array("blue", "winter", "cozy"), "Sports Company Inc."),

      (2, "ProductB",
        Array("red", "summer", "fresh", "cooling"), "YetiX"),

      (3, "ProductC",
        Array("green", "summer", "travel"), "Sports Company Inc.")
    )

    // Define the schema for the DataFrame
    val schema = List("itemId", "itemName", "attributes", "supplier")

    // Create a DataFrame from the sample data
    val itemsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Rename columns "attributes" to "feature0" and "supplier" to "feature1"
    val renamedDf = itemsDf
      .withColumnRenamed("attributes", "feature0")
      .withColumnRenamed("supplier", "feature1")

    // Show the resulting DataFrame
    renamedDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
