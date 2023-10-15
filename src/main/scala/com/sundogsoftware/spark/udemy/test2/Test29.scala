package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test29 {

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test29")
      .master("local[*]")
      .getOrCreate()

    // Define the path to the Parquet file
    val filePath = "path_to_parquet_file.parquet" // Replace with the actual file path

    // Define the schema for the Parquet file using StructType and StructField
    val schema = StructType(
      Array(
        StructField("column1", IntegerType, nullable = true), // Replace with your column names and types
        StructField("column2", IntegerType, nullable = true)
        // Add more StructFields for additional columns as needed
      )
    )

    // Read the Parquet file into a DataFrame with the defined schema
    // Type of file "parquet" or "json" and so on has to be placed at the end
    val parquetDf: DataFrame = spark.read.schema(schema).parquet(filePath)

    // Show the contents of the DataFrame
    parquetDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
