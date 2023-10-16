package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{desc_nulls_first, desc_nulls_last}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test9 {
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
      (1, "ProductA", "100.0", null),
      (2, "ProductB", "200.0", "3.5"),
      (3, "ProductC", "300.0", "4.0"),
      (4, "ProductD", "400.0", null)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "itemName", "value", "predError")

    // Create a DataFrame from the sample data
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Sort the DataFrame by 'value' column in descending order with nulls appearing last
    val sortedDf = transactionsDf.orderBy(desc_nulls_last("predError"))

    val sortedDf1 = transactionsDf.orderBy(desc_nulls_first("predError"))

    // Show the resulting DataFrame
    sortedDf.show()
    sortedDf1.show()

    // Stop the SparkSession
    spark.stop()
  }
}
