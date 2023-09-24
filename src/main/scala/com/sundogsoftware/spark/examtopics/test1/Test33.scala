package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test33 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Import implicits for DataFrame operations
    import spark.implicits._

    /*
    Read a parquet file from a specific location.
     */

    // Define the file path where the Parquet data is located
    val filePath = "/path/to/parquet/files"

    // Read the Parquet data into a DataFrame
    val parquetDF: DataFrame = spark.read.parquet(filePath)

    // Show the content of the DataFrame
    parquetDF.show()
    // Stop the SparkSession
    spark.stop()
  }

}
