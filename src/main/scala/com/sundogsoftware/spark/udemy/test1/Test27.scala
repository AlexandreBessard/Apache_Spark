package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object Test27 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data (replace this with your actual DataFrame)
    val data = Seq(
      (1, "ProductA", 10.5),
      (2, "ProductB", 15.2),
      (3, "ProductA", 12.8),
      (4, "ProductC", 8.3),
      (5, "ProductB", 9.7)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "itemName", "value")

    // Create a DataFrame from the sample data
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Specify the output directory
    val storeDir = "/path/to/output/directory"

    // Write the DataFrame to Parquet format with options
    transactionsDf.write
      .format("parquet")
      .mode("overwrite")          // Overwrite if the directory already exists
      .option("compression", "snappy")  // Use snappy compression
      .save(storeDir)

    // Stop the SparkSession
    spark.stop()
  }
}
