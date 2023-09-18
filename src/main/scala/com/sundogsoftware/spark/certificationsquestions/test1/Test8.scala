package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object Test8 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Path to the JSON file
    val jsonFilePath = "/FileStore/imports.json"

    // Read the JSON file into a DataFrame
    val jsonDataFrame: DataFrame = spark.read.json(jsonFilePath)

    // Display the schema and show the DataFrame
    println("Schema of the JSON DataFrame:")
    jsonDataFrame.printSchema()

    println("Contents of the JSON DataFrame:")
    jsonDataFrame.show()

    // Stop the SparkSession
    spark.stop()
  }
}
