package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test2 {

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

    // Define the schema for the Parquet file
    val schema = StructType(
      Array(
        StructField("itemId", IntegerType, nullable = false),
        StructField("attributes", ArrayType(StringType), nullable = true),
        StructField("supplier", StringType, nullable = true)
      )
    )

    // Read the Parquet file into DataFrame 'itemsDf' with the defined schema
    val itemsDf: DataFrame = spark.read.schema(schema).parquet(filePath)

    // Show the contents of the DataFrame
    itemsDf.show()


    // Stop the SparkSession
    spark.stop()
  }
}
