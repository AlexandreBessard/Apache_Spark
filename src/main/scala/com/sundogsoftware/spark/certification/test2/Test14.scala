package com.sundogsoftware.spark.certification.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Test14 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Specify the path to the CSV file
    val file = "./file.csv"

    // Define the CSV read options
    val options = Map(
      "header" -> "true", // First row contains column names
      "inferSchema" -> "true", // Infer data types of columns
      "sep" -> ";" // Use semicolon as the column separator
    )

    // Read the CSV file with specified options into a DataFrame
    val df: DataFrame = spark.read.format("csv")
      .options(options)
      .load(file)

    //Exact same result without using a map
    val df1: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("InferSchema", "true")
      .option("sep", ";")
      .load(file)

    // Show the DataFrame
    df.show()
    df1.show()

    // Stop the SparkSession
    spark.stop()

  }
}
