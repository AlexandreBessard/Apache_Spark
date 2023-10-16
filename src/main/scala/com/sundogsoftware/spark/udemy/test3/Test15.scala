package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test15 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test6")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data (Replace with your actual data or DataFrame)
    val data = Seq(
      (1, "ProductA", 100.0, "Store1"),
      (2, "ProductB", 200.0, "Store2"),
      (3, "ProductC", 300.0, "Store1"),
      (4, "ProductD", 400.0, "Store3"),
      (5, "ProductE", 500.0, "Store2")
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "itemName", "value", "storeId")

    // Create a DataFrame from the sample data
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Define the output Parquet file location
    val filePath = "output/parquet_data"
    /*
    The default write mode is "errorifexists", meaning if the file path already exists, Spark will raise an error.
     */

    // Write the DataFrame as a Parquet file with partitioning on 'storeId'
    transactionsDf
      .write
      //.mode("overwrite") // You can change this to "append" or "ignore" as needed
      .partitionBy("storeId")
      // Specify the column for partitioning
      .parquet(filePath)

    // Stop the SparkSession
    spark.stop()
  }
}
