package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test23 {

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test21")
      .master("local[*]")
      .getOrCreate()
    /*
    Example of CSV file to read

    transactionId;storeId;productId;name
    1;23;12;green grass
    2;35;31;yellow sun
    3;23;12;green grass
     */

    // Define the path to the CSV file
    val filePath = "data/transactions.csv"

    // Define options for reading the CSV file
    val options: Map[String, String] = Map(
      "sep" -> ";", // Specify the delimiter used in the CSV file
      "format" -> "csv", // Specify the file format as CSV
      "header" -> "true", // Treat the first row as header containing column names
      "inferSchema" -> "true" // Infer the schema of the DataFrame from the data
    )

    /*
    "inferSchema" -> "true": By setting this option to "true" when reading a CSV file with Spark,
    we are telling Spark to examine the data in the file and make educated guesses about the data
    types and structure of the columns. In other words, Spark will try to figure out which columns contain numbers,
    which ones have text, and so on.
     */

    // Load the CSV file into a DataFrame using the specified options
    val transactionsDf: DataFrame = spark.read.options(options).csv(filePath)

    // Show the loaded DataFrame
    transactionsDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
