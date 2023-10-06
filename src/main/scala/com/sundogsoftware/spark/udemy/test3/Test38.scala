package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test38 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("FilterAndLimitExample")
      .master("local[*]")
      .getOrCreate()

    import org.apache.spark.sql.types._

    // Define the schema
    val schema = new StructType()
      .add(StructField("itemId", IntegerType, true))
      .add(StructField("attributes", ArrayType(StringType, true), true))
      .add(StructField("supplier", StringType, true))

    // Read Parquet files with a path filter
    val path = "/home/alex/Dev/Apache_Spark/SparkScalaCourse/src/main/scala/com/sundogsoftware/spark/udemy/test3/DataStore/" // Specify the directory where your Parquet files are located
    val filteredDf: DataFrame = spark.read
      .option("pathGlobFilter", "*_723.parquet") // Filter by file name pattern
      .schema(schema) // Apply the specified schema
      .parquet(path) // Read Parquet files from the specified path

    // Show the resulting DataFrame
    filteredDf.show()
    // Stop the SparkSession
    spark.stop()
  }
}
