package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test7 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Return a list

    // Sample data
    val data = Seq(
      (1, "A", 25, 100.0),
      (2, "B", 30, 200.0),
      (3, "C", 25, 150.0),
      (4, "D", 40, 300.0),
      (5, "E", 25, 120.0),
      (6, "F", 35, 250.0)
    )

    // Define schema
    val schema = Seq(
      "transactionId", "item", "storeId", "amount"
    )

    // Create a DataFrame
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Display the original DataFrame
    println("Original DataFrame:")
    transactionsDf.show()

    // Filter the DataFrame to select rows with storeId = 25 and take the first 3 rows
    // returns an array as result
    val filteredRows = transactionsDf.filter(col("storeId") === 25).take(2)

    // Display the filtered rows
    println("Filtered Rows:")
    filteredRows.foreach(println)

    // Stop the SparkSession
    spark.stop()
  }
}
