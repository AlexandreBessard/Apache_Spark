package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}

object Test6 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Read parquet file as DataFrame

    // Path to the Parquet file
    val parquetFilePath = "/FileStore/imports.parquet"

    // Read the Parquet file into a DataFrame
    val parquetDataFrame: DataFrame = spark.read.parquet(parquetFilePath)

    // Display the schema and show the DataFrame
    println("Schema of the Parquet DataFrame:")
    parquetDataFrame.printSchema()

    println("Contents of the Parquet DataFrame:")
    parquetDataFrame.show()

    // Stop the SparkSession
    spark.stop()
  }
}
