package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object Test12 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Read data from a CSV file into a DataFrame
    val csvFilePath = "src/main/scala/com/sundogsoftware/spark/transaction.csv" // Replace with your actual CSV file path
    val transactionsDf = spark.read
      .option("header", "true") // Assumes the CSV file has a header row
      .option("inferSchema", "true") // Infer data types
      .csv(csvFilePath)

    // Perform some transformations on the DataFrame (for demonstration purposes)
    val transformedDf = transactionsDf.select("transactionId", "storeId")

    transformedDf.show

    // Write the transformed DataFrame to Parquet format and partition by 'storeId'
    transformedDf.write
      .format("parquet")
      .partitionBy("storeId")
      .save("src/main/scala/com/sundogsoftware/spark/result")

    // Stop the SparkSession
    spark.stop()
  }
}
