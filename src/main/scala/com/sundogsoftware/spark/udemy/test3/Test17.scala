package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, from_unixtime, unix_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test17 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test6")
      .master("local[*]")
      .getOrCreate()


    // Define the file path to your Parquet file
    val filePath = "path_to_parquet_file.parquet" // Replace with your actual file path

    // Read the Parquet file with schema merging option
    val parquetDf: DataFrame = spark.read.option("mergeSchema", "true").parquet(filePath)

    // Show the contents of the DataFrame
    parquetDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
