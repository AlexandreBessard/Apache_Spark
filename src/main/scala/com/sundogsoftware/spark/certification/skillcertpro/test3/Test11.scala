  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


  object Test11 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Replace 'file' with the path to your CSV file
    val file = "sample_data.csv"

    // Use Spark to read a CSV file with options
    val df: DataFrame = spark.read
      .format("csv") // Specifies the file format as CSV
      .option("header", "true") // Treats the first row as a header
      .option("inferSchema", "true") // Infers the schema from the data
      .option("sep", "\t") // Sets the tab ('\t') as the separator
      .load(file) // Loads the CSV file

    // Show the DataFrame
    df.show()

    // Stop the SparkSession
    spark.stop()

  }
}