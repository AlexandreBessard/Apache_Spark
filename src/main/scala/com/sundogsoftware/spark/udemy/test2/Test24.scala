package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test24 {

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
    Read a binary file like .png extension file.
     */

    // Specify the path to the directory containing binary files
    val path = "path/to/binary/files"

    // Read all binary files from the specified directory
    val binaryFilesDF = spark.read.format("binaryFile")
      .option("pathGlobFilter", "*") // Read all files
      .load(path)

    /*
    The pathGlobFilter is an option used when reading files from a directory in Apache Spark,
    typically used with file-based data sources like text, parquet, or binaryFile. It allows you to filter the
    files you want to include in your DataFrame based on a glob pattern.
     */

    // Filter the DataFrame to keep only files with the .png extension
    val pngFilesDF = binaryFilesDF.filter("input_file_name LIKE '%.png'")

    // Show the resulting DataFrame
    pngFilesDF.show()

    // Stop the SparkSession
    spark.stop()
  }
}
