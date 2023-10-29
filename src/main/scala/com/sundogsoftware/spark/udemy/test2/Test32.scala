package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test32 {

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test29")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Define the path to the JSON file
    val jsonPath = "path_to_json_file.json" // Replace with the actual file path

    // Read the JSON file into a DataFrame
    val importedDf: DataFrame = spark.read.json(jsonPath)

    // Create or replace a temporary view for the DataFrame
    importedDf.createOrReplaceTempView("importedDf")

    // Run a SQL query on the temporary view
    val filteredDf = spark.sql("SELECT * FROM importedDf WHERE productId != 3")

    // Show the resulting DataFrame
    filteredDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
